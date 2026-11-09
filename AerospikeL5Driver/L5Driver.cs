/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Aerospike.Client;

namespace Aerospike.L5Driver
{
	/// <summary>
	/// Driver for the l5-aws harness in aerospike-client-conformance (l5-aws/DRIVER.md there, contract
	/// version 1): builds a client from the flags, runs a retrying load, and writes what the client sees
	/// to stdout as JSON lines while it answers the commands it reads from stdin.
	/// </summary>
	public sealed class L5Driver
	{
		public const int Contract = 1;
		public const long PollMs = 10;
		public const long HeartbeatMs = 100;
		public const long ReachIntervalMs = 500;
		public const int ReachTimeoutMs = 1000;
		public const long ProbeIntervalMs = 10;
		public const int ProbeConnectMs = 50;
		public const long WatchBoundMs = 60000;
		public const long QuitMs = 30000;
		public const int KeySearchLimit = 1000000;

		private static readonly long StartTicks = Stopwatch.GetTimestamp();
		private static readonly string Usage =
			"usage: L5Driver --seed HOST:PORT --shape pass-through|port-map " +
			"[--port-map NODE=HOST:PORT,...] [--on-miss strict|pass-through] [--user USER] --tend-interval MS " +
			"--timeout MS --namespace NS --set SET [--workers N] [--keys N]";

		public static int Main(string[] args)
		{
			Options options;

			try
			{
				options = new Options(args, Environment.GetEnvironmentVariable("AEROSPIKE_ADMIN_PASSWORD"));
			}
			catch (ArgumentException e)
			{
				Console.Error.WriteLine("ERROR: " + e.Message);
				Console.Error.WriteLine(Usage);
				return 2;
			}

			Output output = new Output();
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
				output.Write(new Json("log", Now()).Put("level", level.ToString()).Put("message", message).ToString()));

			AerospikeClient client;

			try
			{
				client = new AerospikeClient(options.Policy(), options.Seed);
			}
			catch (Exception e)
			{
				output.WriteNow(new Json("error", Now()).Put("message", "client not created: " + Message(e)).ToString());
				Console.Error.WriteLine("ERROR: client not created: " + e);
				return 3;
			}

			new L5Driver(options, output, client).Run();
			return 0;
		}

		internal static long Now()
		{
			return (Stopwatch.GetTimestamp() - StartTicks) * 1000L / Stopwatch.Frequency;
		}

		internal static string Message(Exception e)
		{
			return e.Message ?? e.ToString();
		}

		private readonly Options options;
		private readonly Output output;
		private readonly AerospikeClient client;
		private readonly Load load;
		private readonly Routing routing;
		private readonly ConcurrentDictionary<Node, bool> reachable = new ConcurrentDictionary<Node, bool>();
		private readonly ConcurrentDictionary<Node, byte> checking = new ConcurrentDictionary<Node, byte>();
		private Timer routingPoll;
		private Timer reachPoll;
		private Watch watch;

		private L5Driver(Options options, Output output, AerospikeClient client)
		{
			this.options = options;
			this.output = output;
			this.client = client;
			routing = new Routing(this);
			load = new Load(this, Ready);
		}

		private void Run()
		{
			reachPoll = new Timer(_ => CheckReachable(), null, 0, ReachIntervalMs);
			load.Start();

			try
			{
				string line;

				while ((line = Console.In.ReadLine()) != null)
				{
					if (!Command(line))
					{
						break;
					}
				}
			}
			catch (IOException e)
			{
				Console.Error.WriteLine("stdin: " + e);
			}

			Quit();
		}

		private void Ready()
		{
			string version = typeof(AerospikeClient).Assembly.GetName().Version?.ToString() ?? "development";
			output.Open(new Json("ready", Now()).Put("contract", Contract)
				.Put("epochMs", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
				.Put("client", "csharp").Put("clientVersion", version).Put("shape", options.Shape).ToString());
			routingPoll = new Timer(_ => routing.Poll(), null, 0, PollMs);
		}

		private bool Command(string line)
		{
			string[] words = line.Trim().Split((char[])null, StringSplitOptions.RemoveEmptyEntries);

			if (words.Length == 0)
			{
				Error("malformed command: " + line);
				return true;
			}

			switch (words[0])
			{
				case "serve":
					if (words.Length != 2)
					{
						Error("malformed command: " + line);
					}
					else
					{
						Serve(words[1]);
					}
					return true;

				case "reset":
					if (words.Length != 1)
					{
						Error("malformed command: " + line);
					}
					else
					{
						long t = Now();
						load.Reset();
						routing.Reset(t);
						output.Write(new Json("reset", t).ToString());
					}
					return true;

				case "watch":
					Host host = words.Length == 3 ? HostPort(words[1]) : null;

					if (host == null)
					{
						Error("malformed command: " + line);
					}
					else
					{
						Watch next = new Watch(this, words[1], host, words[2]);

						lock (this)
						{
							if (watch == null)
							{
								watch = next;
								next.Start();
							}
							else
							{
								Error("a watch is already running: " + line);
							}
						}
					}
					return true;

				case "quit":
					if (words.Length != 1)
					{
						Error("malformed command: " + line);
						return true;
					}
					return false;

				default:
					Error("unknown command: " + line);
					return true;
			}
		}

		private void Error(string message)
		{
			output.Write(new Json("error", Now()).Put("message", message).ToString());
		}

		private void Quit()
		{
			long deadline = Stopwatch.GetTimestamp() + QuitMs * Stopwatch.Frequency / 1000L;
			load.Stop(deadline);
			Watch running;

			lock (this)
			{
				running = watch;
			}

			running?.Stop(deadline);
			routingPoll?.Dispose();
			reachPoll?.Dispose();
			client.Close();
			Environment.Exit(0);
		}

		private void Serve(string step)
		{
			Key[] keys = new Key[Node.PARTITIONS];
			int found = 0;

			for (int i = 0; i < KeySearchLimit && found < Node.PARTITIONS; i++)
			{
				Key candidate = new Key(options.Namespace, options.Set, step + '-' + i);
				int partition = (int)Partition.GetPartitionId(candidate.digest);

				if (keys[partition] == null)
				{
					keys[partition] = candidate;
					found++;
				}
			}

			Stopwatch sw = Stopwatch.StartNew();
			WritePolicy wp = options.WritePolicy();
			Policy rp = options.ReadPolicy();
			int written = 0;
			int read = 0;
			int mismatched = 0;
			int failures = 0;
			string firstFailure = null;

			for (int partition = 0; partition < Node.PARTITIONS; partition++)
			{
				if (keys[partition] == null)
				{
					failures++;
					firstFailure ??= "partition " + partition + ": no key in " + KeySearchLimit + " candidates";
					continue;
				}

				string value = step + '-' + partition;

				try
				{
					client.Put(wp, keys[partition], new Bin("v", value));
					written++;
				}
				catch (Exception e)
				{
					failures++;
					firstFailure ??= "partition " + partition + " put: " + Message(e);
					continue;
				}

				try
				{
					Record record = client.Get(rp, keys[partition]);

					if (record != null && value.Equals(record.GetValue("v")))
					{
						read++;
					}
					else
					{
						mismatched++;
					}
				}
				catch (Exception e)
				{
					failures++;
					firstFailure ??= "partition " + partition + " get: " + Message(e);
				}
			}

			output.Write(new Json("served", Now()).Put("step", step).Put("partitions", Node.PARTITIONS)
				.Put("written", written).Put("read", read).Put("mismatched", mismatched)
				.Put("failures", failures).Put("firstFailure", firstFailure)
				.Put("elapsedMs", sw.ElapsedMilliseconds).ToString());
		}

		private void CheckReachable()
		{
			try
			{
				List<Node> nodes = client.Nodes.ToList();
				HashSet<Node> live = new HashSet<Node>(nodes);

				foreach (Node key in reachable.Keys)
				{
					if (!live.Contains(key))
					{
						reachable.TryRemove(key, out _);
					}
				}

				foreach (Node node in nodes)
				{
					if (checking.TryAdd(node, 0))
					{
						ThreadPool.QueueUserWorkItem(_ =>
						{
							try
							{
								reachable[node] = Reachable(node);
							}
							finally
							{
								checking.TryRemove(node, out byte _);
							}
						});
					}
				}
			}
			catch (Exception e)
			{
				Console.Error.WriteLine("reach poll: " + e);
			}
		}

		internal static bool Reachable(Node node)
		{
			InfoPolicy policy = new InfoPolicy { timeout = ReachTimeoutMs };

			try
			{
				return node.Name.Equals(Info.Request(policy, node, "node"));
			}
			catch (Exception)
			{
				// SocketException and other I/O can escape Auth/Info without becoming AerospikeException;
				// never let a reach check abort the driver process.
				return false;
			}
		}

		internal Dictionary<string, Node> NodesByName(Node[] nodes)
		{
			Dictionary<string, Node> map = new Dictionary<string, Node>(StringComparer.Ordinal);

			foreach (Node node in nodes)
			{
				map[node.Name] = node;
			}

			return map;
		}

		internal int Unowned(Dictionary<string, Node> nodes, Node[] masters)
		{
			client.Cluster.partitionMap.TryGetValue(options.Namespace, out Partitions partitions);
			int unowned = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = partitions == null ? null : partitions.replicas[0][i];

				if (node == null || !node.Active ||
					!nodes.TryGetValue(node.Name, out Node held) || !ReferenceEquals(held, node))
				{
					masters[i] = null;
					unowned++;
				}
				else
				{
					masters[i] = node;
				}
			}

			return unowned;
		}

		internal static string Address(Node node)
		{
			IPEndPoint endpoint = node.NodeAddress;
			return endpoint?.Address?.ToString() ?? endpoint?.ToString();
		}

		internal static Host HostPort(string value)
		{
			int colon = value.LastIndexOf(':');

			if (colon <= 0)
			{
				return null;
			}

			string host = value.Substring(0, colon);

			if (host.StartsWith('[') && host.EndsWith(']'))
			{
				host = host.Substring(1, host.Length - 2);
			}

			if (!int.TryParse(value.AsSpan(colon + 1), out int port) || host.Length == 0 ||
				port <= 0 || port > 65535)
			{
				return null;
			}

			return new Host(host, port);
		}

		internal bool TryGetReachable(Node node, out bool? value)
		{
			if (reachable.TryGetValue(node, out bool known))
			{
				value = known;
				return true;
			}

			value = null;
			return false;
		}

		internal void ClearWatch(Watch expected)
		{
			lock (this)
			{
				if (ReferenceEquals(watch, expected))
				{
					watch = null;
				}
			}
		}

		internal void Write(string line) => output.Write(line);

		/// <summary>
		/// Polls the client's routing every <see cref="PollMs"/>, writes a change snapshot when it differs
		/// and a heartbeat snapshot every <see cref="HeartbeatMs"/>.
		/// </summary>
		private sealed class Routing
		{
			private readonly L5Driver driver;
			private string last;
			private long nextHeartbeat;
			private long unownedSince = -1;
			private long longestClosed;
			private readonly object gate = new object();

			public Routing(L5Driver driver)
			{
				this.driver = driver;
			}

			public void Reset(long t)
			{
				lock (gate)
				{
					longestClosed = 0;

					if (unownedSince >= 0)
					{
						unownedSince = t;
					}
				}
			}

			public void Poll()
			{
				try
				{
					long t = Now();
					Node[] nodes = driver.client.Nodes.OrderBy(n => n.Name, StringComparer.Ordinal).ToArray();
					Dictionary<string, Node> byName = driver.NodesByName(nodes);
					Node[] masters = new Node[Node.PARTITIONS];
					int unowned = driver.Unowned(byName, masters);
					long longest = LongestUnowned(t, unowned);
					StringBuilder signature = new StringBuilder().Append(unowned);

					foreach (Node node in nodes)
					{
						Host h = node.Host;
						signature.Append('|').Append(node.Name).Append(',').Append(h.name).Append(',')
							.Append(h.port).Append(',').Append(node.Active);
					}

					string current = signature.ToString();

					if (last == null)
					{
						nextHeartbeat = t;
					}
					else if (!current.Equals(last, StringComparison.Ordinal))
					{
						Snapshot("change", t, nodes, masters, unowned, longest);
					}

					last = current;

					if (t >= nextHeartbeat)
					{
						Snapshot("heartbeat", t, nodes, masters, unowned, longest);
						nextHeartbeat = Math.Max(nextHeartbeat + HeartbeatMs, t + 1);
					}
				}
				catch (Exception e)
				{
					Console.Error.WriteLine("routing poll: " + e);
				}
			}

			private long LongestUnowned(long t, int unowned)
			{
				lock (gate)
				{
					if (unowned > 0 && unownedSince < 0)
					{
						unownedSince = t;
					}
					else if (unowned == 0 && unownedSince >= 0)
					{
						longestClosed = Math.Max(longestClosed, t - unownedSince);
						unownedSince = -1;
					}

					return unownedSince >= 0 ? Math.Max(longestClosed, t - unownedSince) : longestClosed;
				}
			}

			private void Snapshot(string reason, long t, Node[] nodes, Node[] masters, int unowned, long longest)
			{
				StringBuilder list = new StringBuilder("[");
				SortedDictionary<string, int> owners =
					new SortedDictionary<string, int>(StringComparer.Ordinal);
				long errors = 0;
				long timeouts = 0;

				foreach (Node node in nodes)
				{
					Host h = node.Host;

					if (list.Length > 1)
					{
						list.Append(',');
					}

					driver.TryGetReachable(node, out bool? reach);
					list.Append(new Json().Put("name", node.Name).Put("host", h.name).Put("port", h.port)
						.Put("address", Address(node)).Put("active", node.Active).Put("reachable", reach));
					errors += node.GetErrorCount();
					timeouts += node.GetTimeoutCount();
				}

				list.Append(']');

				foreach (Node node in masters)
				{
					if (node != null)
					{
						owners[node.Name] = owners.GetValueOrDefault(node.Name, 0) + 1;
					}
				}

				Json ownersJson = new Json();

				foreach (KeyValuePair<string, int> entry in owners)
				{
					ownersJson.Put(entry.Key, entry.Value);
				}

				driver.Write(new Json("snapshot", t).Put("reason", reason).Raw("nodes", list.ToString())
					.Put("unowned", unowned).Raw("owners", ownersJson.ToString())
					.Put("longestUnownedMs", longest).Put("clientErrors", errors)
					.Put("clientTimeouts", timeouts).Put("succeeded", Interlocked.Read(ref driver.load.succeeded))
					.Put("failed", Interlocked.Read(ref driver.load.failed))
					.Put("maxLatencyMs", driver.load.MaxLatencyMs())
					.Put("lastError", Volatile.Read(ref driver.load.lastError)).ToString());
			}
		}

		/// <summary>
		/// Probes HOST:PORT every <see cref="ProbeIntervalMs"/> until it accepts a connection, then
		/// checks every <see cref="ProbeIntervalMs"/> whether the node is usable, and replies.
		/// </summary>
		internal sealed class Watch
		{
			private readonly L5Driver driver;
			private readonly string address;
			private readonly Host host;
			private readonly string node;
			private Timer timer;
			private long? servingAt;
			private long? lastClosedAt;

			public Watch(L5Driver driver, string address, Host host, string node)
			{
				this.driver = driver;
				this.address = address;
				this.host = host;
				this.node = node;
			}

			public void Start()
			{
				timer = new Timer(_ => Tick(), null, 0, ProbeIntervalMs);
			}

			public void Stop(long deadlineTicks)
			{
				long remainingMs = Math.Max(0,
					(deadlineTicks - Stopwatch.GetTimestamp()) * 1000L / Stopwatch.Frequency);
				ManualResetEvent done = new ManualResetEvent(false);
				timer?.Dispose(done);
				done.WaitOne((int)Math.Min(remainingMs, int.MaxValue));
			}

			private void Tick()
			{
				try
				{
					if (servingAt == null)
					{
						long t = Now();

						if (!Connects())
						{
							lastClosedAt = t;
							return;
						}

						servingAt = t;
					}

					long now = Now();

					if (Usable())
					{
						Reply(now);
					}
					else if (now - servingAt >= WatchBoundMs)
					{
						Reply(null);
					}
				}
				catch (Exception e)
				{
					driver.Error("watch " + address + ' ' + node + ": " + Message(e));
					Finish();
				}
			}

			private bool Connects()
			{
				try
				{
					using Socket socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
					IAsyncResult result = socket.BeginConnect(host.name, host.port, null, null);
					bool ok = result.AsyncWaitHandle.WaitOne(ProbeConnectMs);

					if (!ok)
					{
						try { socket.Close(); } catch { /* ignore */ }
						return false;
					}

					socket.EndConnect(result);
					return true;
				}
				catch (Exception)
				{
					return false;
				}
			}

			private bool Usable()
			{
				Dictionary<string, Node> nodes = driver.NodesByName(driver.client.Nodes);
				return nodes.TryGetValue(node, out Node n) && n.Active &&
					driver.Unowned(nodes, new Node[Node.PARTITIONS]) == 0;
			}

			private void Reply(long? usableAt)
			{
				driver.Write(new Json("watch", Now()).Put("address", address).Put("node", node)
					.Put("servingAt", servingAt).Put("lastClosedAt", lastClosedAt).Put("usableAt", usableAt)
					.Put("usableMs", usableAt == null || servingAt == null ? null : usableAt - servingAt)
					.Put("ownershipChecked", true).ToString());
				Finish();
			}

			private void Finish()
			{
				timer?.Dispose();
				driver.ClearWatch(this);
			}
		}

		/// <summary>
		/// The load: each worker runs one command at a time, and issues a failed command again after
		/// one tend interval until it succeeds.
		/// </summary>
		internal sealed class Load
		{
			internal long succeeded;
			internal long failed;
			internal string lastError;
			private readonly L5Driver driver;
			private readonly Action onFirstSuccess;
			private readonly List<Thread> workers = new List<Thread>();
			private volatile bool stop;
			private long resetAtTicks = StartTicks;
			private long maxLatencyTicks;
			private int started;

			public Load(L5Driver driver, Action onFirstSuccess)
			{
				this.driver = driver;
				this.onFirstSuccess = onFirstSuccess;
			}

			public void Start()
			{
				WritePolicy wp = driver.options.WritePolicy();
				Policy rp = driver.options.ReadPolicy();

				for (int w = 0; w < driver.options.Workers; w++)
				{
					int worker = w;
					Thread thread = new Thread(() => Run(worker, wp, rp))
					{
						IsBackground = true,
						Name = "l5-load-" + w
					};
					workers.Add(thread);
					thread.Start();
				}
			}

			private void Run(int worker, WritePolicy wp, Policy rp)
			{
				for (long i = 0; !stop; i++)
				{
					Key key = new Key(driver.options.Namespace, driver.options.Set,
						"load-" + ((worker + i * driver.options.Workers) % driver.options.Keys));
					long first = Stopwatch.GetTimestamp();

					while (true)
					{
						try
						{
							if (i % 2 == 0)
							{
								driver.client.Put(wp, key, new Bin("v", i));
							}
							else
							{
								driver.client.Get(rp, key);
							}

							break;
						}
						catch (Exception e)
						{
							Interlocked.Increment(ref failed);
							Volatile.Write(ref lastError, Message(e));
						}

						if (stop)
						{
							return;
						}

						Thread.Sleep(driver.options.TendInterval);

						if (stop)
						{
							return;
						}
					}

					Interlocked.Increment(ref succeeded);
					Record(first, Stopwatch.GetTimestamp());

					if (Interlocked.Exchange(ref started, 1) == 0)
					{
						onFirstSuccess();
					}
				}
			}

			private void Record(long first, long end)
			{
				lock (this)
				{
					if (first - resetAtTicks >= 0)
					{
						maxLatencyTicks = Math.Max(maxLatencyTicks, end - first);
					}
				}
			}

			public void Reset()
			{
				lock (this)
				{
					resetAtTicks = Stopwatch.GetTimestamp();
					maxLatencyTicks = 0;
				}
			}

			public long MaxLatencyMs()
			{
				lock (this)
				{
					return maxLatencyTicks * 1000L / Stopwatch.Frequency;
				}
			}

			public void Stop(long deadlineTicks)
			{
				stop = true;

				foreach (Thread thread in workers)
				{
					long remainingMs = (deadlineTicks - Stopwatch.GetTimestamp()) * 1000L / Stopwatch.Frequency;

					if (remainingMs > 0)
					{
						thread.Join((int)Math.Min(remainingMs, int.MaxValue));
					}
				}
			}
		}

		/// <summary>
		/// The flags, validated.
		/// </summary>
		internal sealed class Options
		{
			public Host Seed { get; private set; }
			public string Shape { get; private set; }
			public Dictionary<string, string> PortMap { get; private set; }
			public PortMapAddressTranslator PortMapTranslator { get; private set; }
			public TranslationMissMode OnMiss { get; private set; } = TranslationMissMode.STRICT;
			public string User { get; private set; }
			public string Password { get; private set; }
			public int TendInterval { get; private set; } = -1;
			public int Timeout { get; private set; } = -1;
			public string Namespace { get; private set; }
			public string Set { get; private set; }
			public int Workers { get; private set; } = 4;
			public int Keys { get; private set; } = 1000;

			public Options(string[] args, string password)
			{
				Dictionary<string, string> flags = new Dictionary<string, string>(StringComparer.Ordinal);

				for (int i = 0; i < args.Length; i += 2)
				{
					if (!args[i].StartsWith("--", StringComparison.Ordinal))
					{
						throw new ArgumentException("unexpected argument: " + args[i]);
					}

					if (i + 1 >= args.Length)
					{
						throw new ArgumentException(args[i] + " needs a value");
					}

					if (!flags.TryAdd(args[i], args[i + 1]))
					{
						throw new ArgumentException(args[i] + " given twice");
					}
				}

				foreach (KeyValuePair<string, string> flag in flags)
				{
					string value = flag.Value;

					switch (flag.Key)
					{
						case "--seed":
							Seed = HostPort(value) ??
								throw new ArgumentException("--seed must be HOST:PORT: " + value);
							break;

						case "--shape":
							if (value != "pass-through" && value != "port-map")
							{
								throw new ArgumentException("--shape must be pass-through or port-map: " + value);
							}
							Shape = value;
							break;

						case "--port-map":
							PortMap = ParsePortMap(value);
							break;

						case "--on-miss":
							if (value == "strict")
							{
								OnMiss = TranslationMissMode.STRICT;
							}
							else if (value == "pass-through")
							{
								OnMiss = TranslationMissMode.PASS_THROUGH;
							}
							else
							{
								throw new ArgumentException("--on-miss must be strict or pass-through: " + value);
							}
							break;

						case "--user":
							if (password == null)
							{
								throw new ArgumentException(
									"--user needs AEROSPIKE_ADMIN_PASSWORD in the environment");
							}
							User = value;
							Password = password;
							break;

						case "--tend-interval":
							TendInterval = Positive(flag.Key, value);
							break;

						case "--timeout":
							Timeout = Positive(flag.Key, value);
							break;

						case "--namespace":
							Namespace = value;
							break;

						case "--set":
							Set = value;
							break;

						case "--workers":
							Workers = Positive(flag.Key, value);
							break;

						case "--keys":
							Keys = Positive(flag.Key, value);
							break;

						default:
							throw new ArgumentException("unknown flag: " + flag.Key);
					}
				}

				Require(Seed, "--seed");
				Require(Shape, "--shape");
				Require(TendInterval > 0 ? TendInterval : null, "--tend-interval");
				Require(Timeout > 0 ? Timeout : null, "--timeout");
				Require(Namespace, "--namespace");
				Require(Set, "--set");

				if (Shape == "port-map")
				{
					Require(PortMap, "--port-map with --shape port-map");

					try
					{
						PortMapTranslator = new PortMapAddressTranslator(PortMap);
					}
					catch (AerospikeException e)
					{
						throw new ArgumentException("--port-map: " + Message(e));
					}
				}
				else if (PortMap != null)
				{
					throw new ArgumentException("--port-map only with --shape port-map");
				}
			}

			private static void Require(object value, string flag)
			{
				if (value == null)
				{
					throw new ArgumentException("missing " + flag);
				}
			}

			private static int Positive(string flag, string value)
			{
				if (int.TryParse(value, out int n) && n > 0)
				{
					return n;
				}

				throw new ArgumentException(flag + " must be a positive integer: " + value);
			}

			private static Dictionary<string, string> ParsePortMap(string value)
			{
				Dictionary<string, string> map = new Dictionary<string, string>(StringComparer.Ordinal);

				foreach (string entry in value.Split(','))
				{
					int eq = entry.IndexOf('=');

					if (eq <= 0 || HostPort(entry.Substring(eq + 1)) == null)
					{
						throw new ArgumentException("--port-map entry must be NODE=HOST:PORT: " + entry);
					}

					if (!map.TryAdd(entry.Substring(0, eq), entry.Substring(eq + 1)))
					{
						throw new ArgumentException("--port-map names " + entry.Substring(0, eq) + " twice");
					}
				}

				return map;
			}

			public ClientPolicy Policy()
			{
				ClientPolicy policy = new ClientPolicy
				{
					tendInterval = TendInterval,
					timeout = Timeout,
					user = User,
					password = Password,
					translationOnMiss = OnMiss,
					readPolicyDefault = ReadPolicy(),
					writePolicyDefault = WritePolicy()
				};

				if (Shape == "pass-through")
				{
					policy.useServicesAlternate = true;
					policy.addressTranslator = PassThroughAddressTranslator.Instance;
				}
				else
				{
					policy.useServicesAlternate = false;
					policy.addressTranslator = PortMapTranslator;
				}

				return policy;
			}

			public Policy ReadPolicy()
			{
				return new Policy
				{
					socketTimeout = Timeout,
					totalTimeout = Timeout,
					maxRetries = 0
				};
			}

			public WritePolicy WritePolicy()
			{
				return new WritePolicy
				{
					socketTimeout = Timeout,
					totalTimeout = Timeout,
					maxRetries = 0
				};
			}
		}

		/// <summary>
		/// Stdout, one JSON object per line, each flushed. Lines written before ready are held and
		/// follow it. Anything else printing to System.out goes to stderr.
		/// </summary>
		internal sealed class Output
		{
			private readonly TextWriter stdout = Console.Out;
			private List<string> held = new List<string>();
			private readonly object gate = new object();

			public Output()
			{
				Console.SetOut(Console.Error);
			}

			public void Write(string line)
			{
				lock (gate)
				{
					if (held != null)
					{
						held.Add(line);
					}
					else
					{
						Print(line);
					}
				}
			}

			public void WriteNow(string line)
			{
				lock (gate)
				{
					Print(line);
				}
			}

			public void Open(string ready)
			{
				lock (gate)
				{
					Print(ready);

					foreach (string line in held)
					{
						Print(line);
					}

					held = null;
				}
			}

			private void Print(string line)
			{
				stdout.Write(line);
				stdout.Write('\n');
				stdout.Flush();
			}
		}

		/// <summary>
		/// A JSON object written by hand.
		/// </summary>
		internal sealed class Json
		{
			private readonly StringBuilder sb = new StringBuilder("{");

			public Json()
			{
			}

			public Json(string type, long t)
			{
				Put("type", type);
				Put("t", t);
			}

			public Json Put(string name, string value)
			{
				return Raw(name, value == null ? "null" : Quote(value));
			}

			public Json Put(string name, long value)
			{
				return Raw(name, value.ToString());
			}

			public Json Put(string name, long? value)
			{
				return Raw(name, value == null ? "null" : value.Value.ToString());
			}

			public Json Put(string name, bool value)
			{
				return Raw(name, value ? "true" : "false");
			}

			public Json Put(string name, bool? value)
			{
				return Raw(name, value == null ? "null" : value.Value ? "true" : "false");
			}

			public Json Raw(string name, string json)
			{
				if (sb.Length > 1)
				{
					sb.Append(',');
				}

				sb.Append(Quote(name)).Append(':').Append(json);
				return this;
			}

			public override string ToString() => sb + "}";

			internal static string Quote(string s)
			{
				StringBuilder q = new StringBuilder(s.Length + 2).Append('"');

				foreach (char c in s)
				{
					switch (c)
					{
						case '"':
							q.Append("\\\"");
							break;
						case '\\':
							q.Append("\\\\");
							break;
						case '\n':
							q.Append("\\n");
							break;
						case '\r':
							q.Append("\\r");
							break;
						case '\t':
							q.Append("\\t");
							break;
						default:
							if (c < 0x20 || c == 0x2028 || c == 0x2029)
							{
								q.AppendFormat("\\u{0:x4}", (int)c);
							}
							else
							{
								q.Append(c);
							}
							break;
					}
				}

				return q.Append('"').ToString();
			}
		}
	}
}
