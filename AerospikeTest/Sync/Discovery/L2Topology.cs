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
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// L2 topology built by .github/workflows/discovery-l2.yaml: a three-node cluster on a private
	/// network with no published ports, reachable from the client only through one Toxiproxy listener
	/// per node behind one endpoint hostname. Topology arrives as environment variables matching the
	/// aerospike-client-conformance discovery-l2-topology env-file contract.
	/// </summary>
	internal static class L2Topology
	{
		public const int TendInterval = 250;
		public const int WaitMs = 30000;
		public const string TlsProxySuffix = "-tls";

		public const int InUse = 0;
		public const int InPool = 1;
		public const int Opened = 2;
		public const int Closed = 3;
		public const int Errors = 4;
		public const int Timeouts = 5;

		private static readonly Regex UpstreamPortPattern = new Regex("\"upstream\":\"[^\"]*:(\\d+)\"", RegexOptions.Compiled);
		private static readonly Regex ListenPortPattern = new Regex("\"listen\":\"[^\"]*:(\\d+)\"", RegexOptions.Compiled);
		private static readonly Regex PeerPattern = new Regex("\\[([0-9A-Fa-f]+),([^,\\[\\]]*),\\[((?:[^\\[\\]]|\\[[^\\]]*\\])*)\\]\\]", RegexOptions.Compiled);
		private static readonly HttpClient Http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

		private static readonly object InitLock = new object();
		private static bool initialized;
		private static List<L2Node> nodes;

		private static string host;
		private static List<int> ports;
		private static List<int> tlsPorts;
		private static Dictionary<string, int> proxyPorts;
		private static string subset;
		private static string toxiproxyUrl;
		private static List<string> containers;
		private static string controlDir;
		private static string reachableNode;
		private static string passThroughBranch;
		private static string portMapRange;
		private static string portMapPortBase;
		private static string tlsCertShape;
		private static string clientNetworkMtu;
		private static string proxyPortRange;
		private static string idleReset;

		public static string Host { get { Ensure(); return host; } }
		public static IReadOnlyList<int> Ports { get { Ensure(); return ports; } }
		public static IReadOnlyList<int> TlsPorts { get { Ensure(); return tlsPorts; } }
		public static IReadOnlyDictionary<string, int> ProxyPorts { get { Ensure(); return proxyPorts; } }
		public static string Subset { get { Ensure(); return subset; } }
		public static string ToxiproxyUrl { get { Ensure(); return toxiproxyUrl; } }
		public static IReadOnlyList<string> Containers { get { Ensure(); return containers; } }
		public static string ControlDir { get { Ensure(); return controlDir; } }
		public static string ReachableNode { get { Ensure(); return reachableNode; } }
		public static string PassThroughBranch { get { Ensure(); return passThroughBranch; } }
		public static string PortMapRange { get { Ensure(); return portMapRange; } }
		public static string PortMapPortBase { get { Ensure(); return portMapPortBase; } }
		public static string TlsCertShape { get { Ensure(); return tlsCertShape; } }
		public static string ClientNetworkMtu { get { Ensure(); return clientNetworkMtu; } }
		public static string ProxyPortRange { get { Ensure(); return proxyPortRange; } }
		public static string IdleReset { get { Ensure(); return idleReset; } }

		public static string Namespace
		{
			get
			{
				return !string.IsNullOrEmpty(SuiteHelpers.ns) ? SuiteHelpers.ns : "test";
			}
		}

		/// <summary>
		/// Node behind one listener, as it advertises itself.
		/// </summary>
		public sealed class L2Node
		{
			public string Proxy { get; }
			public int Port { get; }
			public string Name { get; }
			public List<string> Advertised { get; }
			public int TlsPort { get; }
			public List<string> TlsAdvertised { get; }
			public string TlsName { get; set; }

			public L2Node(string proxy, int port, string name, List<string> advertised, int tlsPort, List<string> tlsAdvertised)
			{
				Proxy = proxy;
				Port = port;
				Name = name;
				Advertised = advertised;
				TlsPort = tlsPort;
				TlsAdvertised = tlsAdvertised;
			}

			public override string ToString()
			{
				return Name + " proxy=" + Proxy + " listener=" + Port + " advertised=" + string.Join(",", Advertised) +
					(TlsPort > 0 ? " tlsListener=" + TlsPort + " tlsAdvertised=" + string.Join(",", TlsAdvertised) + " tlsName=" + TlsName : "");
			}
		}

		/// <summary>
		/// Peer as listed by a node's peers reply.
		/// </summary>
		public sealed class Peer
		{
			public string Name { get; }
			public string TlsName { get; }
			public List<string> Endpoints { get; }

			public Peer(string name, string tlsName, List<string> endpoints)
			{
				Name = name;
				TlsName = tlsName;
				Endpoints = endpoints;
			}
		}

		public sealed class Joined
		{
			public Node Node { get; }
			public long BumpToActiveMs { get; }
			public long Generation { get; }

			public Joined(Node node, long bumpToActiveMs, long generation)
			{
				Node = node;
				BumpToActiveMs = bumpToActiveMs;
				Generation = generation;
			}
		}

		public sealed class Outcome
		{
			public long StartNanos { get; }
			public long ElapsedMs { get; }
			public Exception Error { get; }

			public Outcome(long startNanos, long elapsedMs, Exception error)
			{
				StartNanos = startNanos;
				ElapsedMs = elapsedMs;
				Error = error;
			}

			public string Kind()
			{
				if (Error is AerospikeException ae)
				{
					return Error.GetType().Name + '(' + ae.Result + ')';
				}
				return Error == null ? "ok" : Error.GetType().Name;
			}
		}

		public sealed class Load
		{
			public ConcurrentQueue<Outcome> Outcomes { get; } = new ConcurrentQueue<Outcome>();
			private readonly CancellationTokenSource stop = new CancellationTokenSource();
			private readonly List<Task> tasks = new List<Task>();
			private readonly string name;

			public Load(string name, int threads, Action<int, int> command)
			{
				this.name = name;

				for (int i = 0; i < threads; i++)
				{
					int thread = i;
					tasks.Add(Task.Run(() =>
					{
						for (int iteration = 0; !stop.IsCancellationRequested; iteration++)
						{
							long begin = NanoTime();
							Exception error = null;

							try
							{
								command(thread, iteration);
							}
							catch (Exception t)
							{
								error = t;
							}

							Outcomes.Enqueue(new Outcome(begin, (NanoTime() - begin) / 1000000L, error));
						}
					}));
				}
			}

			public bool Stop(long boundMs)
			{
				stop.Cancel();
				bool done = Task.WaitAll(tasks.ToArray(), (int)Math.Min(boundMs, int.MaxValue));

				if (!done)
				{
					Console.WriteLine("L2-BLOCKED " + name + " still running after stop bound " + boundMs + "ms");
				}
				return done;
			}

			public List<Outcome> Errors()
			{
				List<Outcome> list = new List<Outcome>();

				foreach (Outcome outcome in Outcomes)
				{
					if (outcome.Error != null)
					{
						list.Add(outcome);
					}
				}
				return list;
			}

			public long Successes()
			{
				long count = 0;

				foreach (Outcome outcome in Outcomes)
				{
					if (outcome.Error == null)
					{
						count++;
					}
				}
				return count;
			}
		}

		/// <summary>
		/// Skip the L2 class when the discovery env-file contract is not present (normal unit-test runs).
		/// </summary>
		public static void RequireEnv()
		{
			if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DISCOVERY_ENDPOINT_HOSTNAME")) ||
				string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DISCOVERY_ENDPOINT_PORTS")) ||
				string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DISCOVERY_PROXY_MAP")) ||
				string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DISCOVERY_SUBSET")) ||
				string.IsNullOrEmpty(Environment.GetEnvironmentVariable("TOXIPROXY_URL")) ||
				string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AEROSPIKE_CONTAINER_NAMES")))
			{
				Assert.Inconclusive("L2 discovery env not set. Run via discovery-l2 workflow / run-l2-suite.sh.");
			}
			Ensure();
		}

		public static List<L2Node> Nodes()
		{
			Ensure();

			if (nodes != null)
			{
				return nodes;
			}

			Assert.AreEqual(containers.Count, ports.Count,
				"DISCOVERY_ENDPOINT_PORTS " + string.Join(",", ports) + " vs AEROSPIKE_CONTAINER_NAMES " + string.Join(",", containers));

			List<L2Node> list = new List<L2Node>();
			Dictionary<string, string> tlsNames = new Dictionary<string, string>();

			string[] commands = tlsPorts.Count == 0
				? new string[] { "node", "service-clear-std" }
				: new string[] { "node", "service-clear-std", "service-tls-std", "peers-tls-std" };

			foreach (int port in ports)
			{
				string proxy = ProxyName(port);
				Dictionary<string, string> info = Info.Request(host, port, commands);
				string name = info.TryGetValue("node", out string n) ? n : null;
				Assert.IsNotNull(name, "No node name from listener " + port);

				proxyPorts.TryGetValue(proxy + TlsProxySuffix, out int tlsPort);
				info.TryGetValue("service-clear-std", out string clear);
				info.TryGetValue("service-tls-std", out string tls);
				L2Node node = new L2Node(proxy, port, name, Endpoints(clear), tlsPort, Endpoints(tls));
				Assert.IsFalse(node.Advertised.Count == 0, "Node " + name + " advertises no clear address");
				list.Add(node);

				if (info.TryGetValue("peers-tls-std", out string peersTls))
				{
					foreach (Peer peer in ParsePeers(peersTls))
					{
						if (!string.IsNullOrEmpty(peer.TlsName))
						{
							tlsNames[peer.Name] = peer.TlsName;
						}
					}
				}
			}

			foreach (L2Node node in list)
			{
				if (tlsNames.TryGetValue(node.Name, out string tlsName))
				{
					node.TlsName = tlsName;
				}
				Console.WriteLine("L2-NODE " + node);
			}

			nodes = list;
			return nodes;
		}

		public static List<string> NodeNames()
		{
			List<string> names = new List<string>();

			foreach (L2Node node in Nodes())
			{
				names.Add(node.Name);
			}
			names.Sort(StringComparer.Ordinal);
			return names;
		}

		public static global::Aerospike.Client.Host Seed()
		{
			return new global::Aerospike.Client.Host(Host, Nodes()[0].Port);
		}

		public static string Listener(int port)
		{
			return Host + ':' + port;
		}

		public static ClientPolicy Policy()
		{
			return new ClientPolicy
			{
				tendInterval = TendInterval,
				timeout = 1000
			};
		}

		public static Dictionary<string, string> PortMapByEndpoint(List<L2Node> list)
		{
			Dictionary<string, string> map = new Dictionary<string, string>();

			foreach (L2Node node in list)
			{
				foreach (string advertised in node.Advertised)
				{
					map[advertised] = Listener(node.Port);
				}
			}
			return map;
		}

		public static Dictionary<string, string> PortMapByNodeName(List<L2Node> list)
		{
			Dictionary<string, string> map = new Dictionary<string, string>();

			foreach (L2Node node in list)
			{
				map[node.Name] = Listener(node.Port);
			}
			return map;
		}

		public static Dictionary<string, string> TlsPortMap(List<L2Node> list)
		{
			Dictionary<string, string> map = new Dictionary<string, string>();

			foreach (L2Node node in list)
			{
				foreach (string advertised in node.TlsAdvertised)
				{
					map[advertised] = Listener(node.TlsPort);
				}
			}
			return map;
		}

		public static List<string> NodeNames(IAerospikeClient client)
		{
			List<string> names = new List<string>();

			foreach (Node node in client.Nodes)
			{
				names.Add(node.Name);
			}
			names.Sort(StringComparer.Ordinal);
			return names;
		}

		public static bool WaitFor(Func<bool> condition, int pollMs = 100)
		{
			long deadline = Environment.TickCount64 + WaitMs;

			while (!condition())
			{
				if (Environment.TickCount64 >= deadline)
				{
					return false;
				}
				Thread.Sleep(pollMs);
			}
			return true;
		}

		public static void WaitNodes(IAerospikeClient client, List<string> names)
		{
			WaitFor(() => ListsEqual(NodeNames(client), names));
			CollectionAssert.AreEqual(names, NodeNames(client));
		}

		public static Node[] AssertAllPartitionsOwned(string row, AerospikeClient client)
		{
			Node[] masters = new Node[Node.PARTITIONS];
			WaitFor(() => Unowned(client, masters) == 0);
			int unowned = Unowned(client, masters);

			Console.WriteLine("L2-PARTITIONS " + row + " partitions=" + Node.PARTITIONS + " unowned=" + unowned +
				" owners=" + FormatOwners(Owners(masters)));
			Assert.AreEqual(0, unowned);
			return masters;
		}

		public static Dictionary<string, int> Owners(AerospikeClient client)
		{
			Node[] masters = new Node[Node.PARTITIONS];
			Unowned(client, masters);
			return Owners(masters);
		}

		public static Dictionary<string, int> Owners(Node[] masters)
		{
			SortedDictionary<string, int> owners = new SortedDictionary<string, int>(StringComparer.Ordinal);

			foreach (Node node in masters)
			{
				if (node != null)
				{
					owners[node.Name] = owners.GetValueOrDefault(node.Name, 0) + 1;
				}
			}
			return new Dictionary<string, int>(owners);
		}

		public static void AssertUnreachable(string endpoint)
		{
			int index = endpoint.LastIndexOf(':');
			string h = endpoint.Substring(0, index);
			int port = int.Parse(endpoint.Substring(index + 1));

			try
			{
				using (Socket socket = new Socket(SocketType.Stream, ProtocolType.Tcp))
				{
					IAsyncResult result = socket.BeginConnect(h, port, null, null);
					bool connected = result.AsyncWaitHandle.WaitOne(2000, true);

					if (connected && socket.Connected)
					{
						socket.EndConnect(result);
						Assert.Fail("Advertised address " + endpoint + " is reachable from the client");
					}
				}
			}
			catch (Exception expected)
			{
				Console.WriteLine("L2-UNREACHABLE " + endpoint + ' ' + expected.Message);
			}
		}

		public static Dictionary<string, long> NamespaceStats(L2Node node, params string[] names)
		{
			string reply = Info.Request(Host, node.Port, "namespace/" + Namespace);
			Dictionary<string, long> stats = new Dictionary<string, long>();

			foreach (string token in reply.Split(';'))
			{
				int index = token.IndexOf('=');

				if (index > 0)
				{
					string name = token.Substring(0, index);

					foreach (string wanted in names)
					{
						if (wanted.Equals(name))
						{
							stats[name] = long.Parse(token.Substring(index + 1));
						}
					}
				}
			}

			foreach (string wanted in names)
			{
				Assert.IsTrue(stats.ContainsKey(wanted), "Node " + node.Name + " has no namespace statistic " + wanted);
			}
			return stats;
		}

		public static void SetListener(string proxy, bool enabled)
		{
			HttpResponseMessage response = Toxiproxy("POST", "/proxies/" + proxy, "{\"enabled\":" + (enabled ? "true" : "false") + "}");
			string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Console.WriteLine("L2-TOXIPROXY " + proxy + " enabled=" + enabled + " status=" + (int)response.StatusCode + ' ' + body);
			Assert.AreEqual(200, (int)response.StatusCode, proxy + ": " + body);
		}

		public static int AddListener(string proxy, string upstream)
		{
			return AddListener(proxy, NextListenerPort(), upstream);
		}

		public static int AddListener(string proxy, int port, string upstream)
		{
			HttpResponseMessage response = Toxiproxy("POST", "/proxies",
				"{\"name\":\"" + proxy + "\",\"listen\":\"0.0.0.0:" + port + "\",\"upstream\":\"" + upstream + "\",\"enabled\":true}");
			string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Console.WriteLine("L2-TOXIPROXY add " + proxy + " status=" + (int)response.StatusCode + ' ' + body);
			Assert.AreEqual(201, (int)response.StatusCode, proxy + ": " + body);
			return port;
		}

		public static int NextListenerPort()
		{
			int port = 0;

			foreach (int used in ListenerPorts())
			{
				port = Math.Max(port, used);
			}
			return port + 1;
		}

		public static void RemoveListener(string proxy)
		{
			HttpResponseMessage response = Toxiproxy("DELETE", "/proxies/" + proxy, null);
			string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Console.WriteLine("L2-TOXIPROXY remove " + proxy + " status=" + (int)response.StatusCode);
			Assert.AreEqual(204, (int)response.StatusCode, proxy + ": " + body);
		}

		public static void AddToxic(string proxy, string name, string type, string stream, double toxicity, string attributes)
		{
			HttpResponseMessage response = Toxiproxy("POST", "/proxies/" + proxy + "/toxics",
				"{\"name\":\"" + name + "\",\"type\":\"" + type + "\",\"stream\":\"" + stream +
				"\",\"toxicity\":" + toxicity.ToString(System.Globalization.CultureInfo.InvariantCulture) +
				",\"attributes\":" + attributes + '}');
			string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Console.WriteLine("L2-TOXIPROXY toxic add " + proxy + ' ' + name + " status=" + (int)response.StatusCode + ' ' + body);
			Assert.AreEqual(200, (int)response.StatusCode, proxy + ": " + body);
		}

		public static void RemoveToxic(string proxy, string name)
		{
			HttpResponseMessage response = Toxiproxy("DELETE", "/proxies/" + proxy + "/toxics/" + name, null);
			string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Console.WriteLine("L2-TOXIPROXY toxic remove " + proxy + ' ' + name + " status=" + (int)response.StatusCode);
			Assert.AreEqual(204, (int)response.StatusCode, proxy + ": " + body);
		}

		public static string Toxics(string proxy)
		{
			HttpResponseMessage response = Toxiproxy("GET", "/proxies/" + proxy + "/toxics", null);
			string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Assert.AreEqual(200, (int)response.StatusCode, proxy + ": " + body);
			return body;
		}

		public static int UpstreamPort(string proxy)
		{
			HttpResponseMessage response = Toxiproxy("GET", "/proxies/" + proxy, null);
			string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Match m = UpstreamPortPattern.Match(body);
			Assert.AreEqual(200, (int)response.StatusCode, proxy + ": " + body);
			Assert.IsTrue(m.Success, "No upstream port for " + proxy + ": " + body);
			return int.Parse(m.Groups[1].Value);
		}

		public static HashSet<int> ListenerPorts()
		{
			HttpResponseMessage response = Toxiproxy("GET", "/proxies", null);
			string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
			Assert.AreEqual(200, (int)response.StatusCode, body);
			HashSet<int> set = new HashSet<int>(ProxyPorts.Values);

			foreach (Match m in ListenPortPattern.Matches(body))
			{
				set.Add(int.Parse(m.Groups[1].Value));
			}
			return set;
		}

		public static Dictionary<string, string> Control(string command)
		{
			string dir = ControlDir;
			string id = Guid.NewGuid().ToString();
			string tmp = Path.Combine(dir, id + ".tmp");
			string ack = Path.Combine(dir, id + ".ack");
			string request = Path.Combine(dir, id + ".request");

			File.WriteAllText(tmp, command + '\n', Encoding.UTF8);
			File.Move(tmp, request);

			Assert.IsTrue(WaitFor(() => File.Exists(ack)), "No acknowledgement for '" + command + "' in " + dir);

			Dictionary<string, string> map = new Dictionary<string, string>();

			foreach (string line in File.ReadAllLines(ack, Encoding.UTF8))
			{
				int index = line.IndexOf('=');

				if (index > 0)
				{
					map[line.Substring(0, index)] = line.Substring(index + 1);
				}
			}
			File.Delete(ack);
			Console.WriteLine("L2-CONTROL " + command + ' ' + string.Join(",", map.Select(e => e.Key + '=' + e.Value)));
			Assert.IsFalse(map.ContainsKey("error"), "'" + command + "' failed: " + (map.TryGetValue("error", out string err) ? err : ""));
			return map;
		}

		public static long PeersGeneration(L2Node node)
		{
			return long.Parse(Info.Request(Host, node.Port, "peers-generation"));
		}

		public static Joined WaitJoined(AerospikeClient client, L2Node observer, long generation, List<string> names)
		{
			long[] bump = { 0 };
			long[] active = { 0 };
			Node[] added = { null };

			Assert.IsTrue(WaitFor(() =>
			{
				long now = NanoTime();

				if (bump[0] == 0 && PeersGeneration(observer) != generation)
				{
					bump[0] = now;
				}

				if (active[0] == 0)
				{
					foreach (Node node in client.Nodes)
					{
						if (node.Active && !names.Contains(node.Name))
						{
							active[0] = now;
							added[0] = node;
						}
					}
				}
				return bump[0] != 0 && active[0] != 0;
			}, 5), "No peers-generation bump or no active node after generation " + generation);

			return new Joined(added[0], (active[0] - bump[0]) / 1000000L, PeersGeneration(observer));
		}

		public static List<string> Alternates(L2Node node)
		{
			return Endpoints(Info.Request(Host, node.Port, "service-clear-alt"));
		}

		public static string PeerName(L2Node observer, string command, string endpoint)
		{
			foreach (Peer peer in ParsePeers(Info.Request(Host, observer.Port, command)))
			{
				if (peer.Endpoints.Contains(endpoint))
				{
					return peer.Name;
				}
			}
			return null;
		}

		public static void WaitClusterStable(int size)
		{
			string command = "cluster-stable:size=" + size + ";ignore-migrations=false";
			List<string> keys = new List<string>();

			bool stable = WaitFor(() =>
			{
				keys.Clear();

				foreach (L2Node node in Nodes())
				{
					try
					{
						keys.Add(Info.Request(Host, node.Port, command));
					}
					catch (Exception e)
					{
						keys.Add(e.Message);
					}
				}
				return keys.Distinct().Count() == 1 && !keys[0].StartsWith("ERROR", StringComparison.Ordinal);
			});
			Console.WriteLine("L2-CLUSTER-STABLE size=" + size + ' ' + string.Join(",", keys));
			Assert.IsTrue(stable, "Cluster not stable at size " + size + ": " + string.Join(",", keys));
		}

		public static Dictionary<string, long[]> ConnectionStats(AerospikeClient client)
		{
			ClusterStats stats = client.GetClusterStats();
			SortedDictionary<string, long[]> map = new SortedDictionary<string, long[]>(StringComparer.Ordinal);

			foreach (NodeStats node in stats.nodes)
			{
				map[node.node.Name] = new long[]
				{
					node.syncStats.inUse,
					node.syncStats.inPool,
					node.syncStats.opened,
					node.syncStats.closed,
					node.ErrorCount,
					node.TimeoutCount
				};
			}
			return new Dictionary<string, long[]>(map);
		}

		public static string Format(Dictionary<string, long[]> stats)
		{
			StringBuilder sb = new StringBuilder();

			foreach (KeyValuePair<string, long[]> entry in stats)
			{
				long[] v = entry.Value;
				sb.Append(entry.Key).Append("{inUse=").Append(v[0]).Append(",inPool=").Append(v[1]).Append(",opened=")
					.Append(v[2]).Append(",closed=").Append(v[3]).Append(",errors=").Append(v[4]).Append(",timeouts=")
					.Append(v[5]).Append("} ");
			}
			return sb.ToString().Trim();
		}

		public static Dictionary<string, int> Kinds(List<Outcome> outcomes)
		{
			SortedDictionary<string, int> map = new SortedDictionary<string, int>(StringComparer.Ordinal);

			foreach (Outcome outcome in outcomes)
			{
				string kind = outcome.Kind();
				map[kind] = map.GetValueOrDefault(kind, 0) + 1;
			}
			return new Dictionary<string, int>(map);
		}

		public static List<Peer> ParsePeers(string reply)
		{
			List<Peer> peers = new List<Peer>();

			if (reply == null)
			{
				return peers;
			}

			int first = reply.IndexOf(',');
			int second = first < 0 ? -1 : reply.IndexOf(',', first + 1);

			if (first < 0 || second < 0)
			{
				return peers;
			}

			string portString = reply.Substring(first + 1, second - first - 1);
			int defaultPort = portString.Length == 0 ? 0 : int.Parse(portString);

			foreach (Match m in PeerPattern.Matches(reply.Substring(second + 1)))
			{
				List<string> endpoints = new List<string>();

				foreach (string address in m.Groups[3].Value.Split(','))
				{
					if (address.Length == 0)
					{
						continue;
					}

					bool hasPort = address.StartsWith('[') ? address.Contains("]:") : address.Contains(':');
					endpoints.Add(hasPort ? address : address + ':' + defaultPort);
				}
				peers.Add(new Peer(m.Groups[1].Value, m.Groups[2].Value, endpoints));
			}
			return peers;
		}

		private static void Ensure()
		{
			if (initialized)
			{
				return;
			}

			lock (InitLock)
			{
				if (initialized)
				{
					return;
				}

				host = Required("DISCOVERY_ENDPOINT_HOSTNAME");
				ports = ParsePorts(Required("DISCOVERY_ENDPOINT_PORTS"));
				tlsPorts = ParsePorts(Optional("DISCOVERY_TLS_ENDPOINT_PORTS"));
				proxyPorts = ParseProxyPorts(Required("DISCOVERY_PROXY_MAP"));
				subset = Required("DISCOVERY_SUBSET");
				toxiproxyUrl = Required("TOXIPROXY_URL");
				containers = new List<string>(Required("AEROSPIKE_CONTAINER_NAMES").Split(','));
				controlDir = Optional("DISCOVERY_CONTROL_DIR");
				reachableNode = Optional("DISCOVERY_REACHABLE_NODE");
				passThroughBranch = Optional("DISCOVERY_PASS_THROUGH_BRANCH");
				portMapRange = Optional("DISCOVERY_PORT_MAP_RANGE");
				portMapPortBase = Optional("DISCOVERY_PORT_MAP_PORT_BASE");
				tlsCertShape = Optional("DISCOVERY_TLS_CERT_SHAPE");
				clientNetworkMtu = Optional("DISCOVERY_CLIENT_NETWORK_MTU");
				proxyPortRange = Optional("DISCOVERY_PROXY_PORT_RANGE");
				idleReset = Optional("DISCOVERY_IDLE_RESET");
				initialized = true;
			}
		}

		private static HttpResponseMessage Toxiproxy(string method, string path, string body)
		{
			using (HttpRequestMessage request = new HttpRequestMessage(new HttpMethod(method), ToxiproxyUrl + path))
			{
				if (body != null)
				{
					request.Content = new StringContent(body, Encoding.UTF8, "application/json");
				}
				return Http.Send(request);
			}
		}

		private static List<string> Endpoints(string reply)
		{
			List<string> list = new List<string>();

			if (string.IsNullOrEmpty(reply))
			{
				return list;
			}

			foreach (global::Aerospike.Client.Host h in global::Aerospike.Client.Host.ParseServiceHosts(reply))
			{
				list.Add(h.name.IndexOf(':') >= 0 ? "[" + h.name + "]:" + h.port : h.name + ':' + h.port);
			}
			return list;
		}

		private static string ProxyName(int port)
		{
			foreach (KeyValuePair<string, int> entry in proxyPorts)
			{
				if (entry.Value == port)
				{
					return entry.Key;
				}
			}
			throw new InvalidOperationException("DISCOVERY_PROXY_MAP has no proxy for listener " + port + ": " +
				string.Join(",", proxyPorts.Select(e => e.Key + ':' + e.Value)));
		}

		private static Dictionary<string, int> ParseProxyPorts(string csv)
		{
			Dictionary<string, int> map = new Dictionary<string, int>();

			foreach (string entry in csv.Split(','))
			{
				if (entry.Length == 0)
				{
					continue;
				}
				int index = entry.LastIndexOf(':');
				map[entry.Substring(0, index)] = int.Parse(entry.Substring(index + 1));
			}
			return map;
		}

		private static List<int> ParsePorts(string csv)
		{
			List<int> list = new List<int>();

			foreach (string port in csv.Split(','))
			{
				if (port.Length > 0)
				{
					list.Add(int.Parse(port));
				}
			}
			return list;
		}

		private static string Optional(string name)
		{
			string value = Environment.GetEnvironmentVariable(name);
			return value ?? "";
		}

		private static string Required(string name)
		{
			string value = Environment.GetEnvironmentVariable(name);

			if (string.IsNullOrEmpty(value))
			{
				throw new InvalidOperationException("Environment variable " + name +
					" is not set. Run .github/discovery/run-l2-suite.sh or the discovery-l2 workflow");
			}
			return value;
		}

		private static int Unowned(AerospikeClient client, Node[] masters)
		{
			client.Cluster.partitionMap.TryGetValue(Namespace, out Partitions partitions);
			int unowned = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = null;

				if (partitions != null && partitions.replicas.Length > 0)
				{
					node = partitions.replicas[0][i];
				}

				if (node == null || !node.Active)
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

		private static bool ListsEqual(List<string> a, List<string> b)
		{
			if (a.Count != b.Count)
			{
				return false;
			}

			for (int i = 0; i < a.Count; i++)
			{
				if (!a[i].Equals(b[i], StringComparison.Ordinal))
				{
					return false;
				}
			}
			return true;
		}

		private static string FormatOwners(Dictionary<string, int> owners)
		{
			StringBuilder sb = new StringBuilder("{");

			foreach (KeyValuePair<string, int> entry in owners)
			{
				if (sb.Length > 1)
				{
					sb.Append(", ");
				}
				sb.Append(entry.Key).Append('=').Append(entry.Value);
			}
			return sb.Append('}').ToString();
		}

		private static long NanoTime()
		{
			return (long)(Stopwatch.GetTimestamp() * (1000000000.0 / Stopwatch.Frequency));
		}
	}
}
