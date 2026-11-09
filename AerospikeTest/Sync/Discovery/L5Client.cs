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
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using Aerospike.Client;
using Pod = Aerospike.Test.K8sCluster.Pod;

namespace Aerospike.Test
{
	/// <summary>
	/// Client-side checks the L5 rows share: which pod each node is, where the client reaches it,
	/// what each pod advertises, partition ownership, and a steady read and write load.
	/// </summary>
	internal static class L5Client
	{
		public const long RoutingWaitMs = 60000;
		public const int QuietTendIntervals = 5;
		public const int ReachTimeoutMs = 1000;
		public const int KeySearchLimit = 1000000;
		public const int LoadThreads = 4;
		public const int LoadKeys = 1000;
		public const int LoadMaxRetries = 2;
		public const long LoadStopMs = 30000;

		private static readonly Regex Peer = new Regex(
			@"\[([0-9A-Fa-f]+),([^,\[\]]*),\[((?:[^\[\]]|\[[^\]]*\])*)\]\]",
			RegexOptions.Compiled);

		/// <summary>
		/// Aerospike pod with its node name and its own Service NodePort.
		/// </summary>
		public sealed class PodEndpoint
		{
			public Pod Pod { get; }
			public string NodeName { get; }
			public int NodePort { get; }

			public PodEndpoint(Pod pod, string nodeName, int nodePort)
			{
				Pod = pod;
				NodeName = nodeName;
				NodePort = nodePort;
			}
		}

		/// <summary>
		/// Return every pod of the cluster by node name, each with its Service NodePort. The node
		/// name is the node ID the operator reports, as the server formats it: upper-case
		/// hexadecimal without leading zeros.
		/// </summary>
		public static SortedDictionary<string, PodEndpoint> PodEndpoints(K8sCluster cluster)
		{
			int port = cluster.ServicePort();
			SortedDictionary<string, PodEndpoint> map =
				new SortedDictionary<string, PodEndpoint>(StringComparer.Ordinal);
			HashSet<int> nodePorts = new HashSet<int>();

			foreach (Pod pod in cluster.Pods())
			{
				string reported = cluster.NodeId(pod.Name);
				Assert.IsTrue(Regex.IsMatch(reported, "^[0-9A-Fa-f]{1,16}$"),
					pod.Name + " has no hexadecimal node ID in the AerospikeCluster status: " + reported);
				string name = Convert.ToUInt64(reported, 16).ToString("X", CultureInfo.InvariantCulture);
				int nodePort = cluster.NodePort(pod.Name, port);
				Console.WriteLine("L5-POD pod=" + pod.Name + " worker=" + pod.Node + " phase=" + pod.Phase +
					" nodeId=" + reported + " node=" + name + " nodePort=" + nodePort);
				Assert.IsTrue(nodePort > 0, pod.Name + " has no Service NodePort for " + port);
				Assert.IsTrue(nodePorts.Add(nodePort),
					"NodePort " + nodePort + " of " + pod.Name + " is not unique");
				map[name] = new PodEndpoint(pod, name, nodePort);
			}

			return map;
		}

		public static void CaptureLog()
		{
			CaptureLog(_ => { });
		}

		/// <summary>
		/// Print the client's warnings and pass each to <paramref name="observer"/>, which runs on
		/// the tend thread.
		/// </summary>
		public static void CaptureLog(Action<string> observer)
		{
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				Console.WriteLine("CLIENT-LOG " + level + ' ' + message);
				observer(message);
			});
		}

		/// <summary>
		/// Return whether the node answers an info request within <see cref="ReachTimeoutMs"/>.
		/// </summary>
		public static bool Reachable(Node node)
		{
			InfoPolicy policy = new InfoPolicy { timeout = ReachTimeoutMs };

			try
			{
				return node.Name.Equals(Info.Request(policy, node, "node"));
			}
			catch (Exception)
			{
				// SocketException and other I/O can escape Auth/Info without becoming AerospikeException.
				return false;
			}
		}

		/// <summary>
		/// Fill <paramref name="masters"/> with the master of each partition and return how many
		/// partitions are unowned (missing, inactive, wrong node object, or unreachable when asked).
		/// </summary>
		public static int Unowned(AerospikeClient client, Node[] masters, bool reachable)
		{
			client.Cluster.partitionMap.TryGetValue(L5Environment.Namespace, out Partitions partitions);
			Dictionary<Node, bool> answers = new Dictionary<Node, bool>();
			int unowned = 0;

			for (int i = 0; i < Node.PARTITIONS; i++)
			{
				Node node = partitions == null ? null : partitions.replicas[0][i];
				bool answersOk = true;

				if (reachable && node != null)
				{
					if (!answers.TryGetValue(node, out answersOk))
					{
						answersOk = Reachable(node);
						answers[node] = answersOk;
					}
				}

				if (node == null || !node.Active || !ReferenceEquals(NodeNamed(client, node.Name), node) ||
					(reachable && !answersOk))
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

		public static Node NodeNamed(AerospikeClient client, string name)
		{
			foreach (Node node in client.Nodes)
			{
				if (node.Name.Equals(name, StringComparison.Ordinal))
				{
					return node;
				}
			}

			return null;
		}

		public static List<string> Unreachable(AerospikeClient client)
		{
			List<string> list = new List<string>();

			foreach (Node node in client.Nodes)
			{
				if (!node.Active || !Reachable(node))
				{
					list.Add(node.Name);
				}
			}

			return list;
		}

		/// <summary>
		/// Wait until the client holds <paramref name="size"/> answering nodes owning every
		/// partition.
		/// </summary>
		public static long AwaitRouting(string step, AerospikeClient client, int size, long boundMs)
		{
			Node[] masters = new Node[Node.PARTITIONS];
			Stopwatch sw = Stopwatch.StartNew();
			bool routed = K8sCluster.WaitFor(() =>
				client.Nodes.Length == size && Unreachable(client).Count == 0 &&
				Unowned(client, masters, true) == 0 && Owners(masters).Count == size, boundMs, 100);
			long elapsedMs = sw.ElapsedMilliseconds;
			Console.WriteLine("L5-ROUTING step=" + step + " nodes=" + Format(Names(client)) +
				" unreachable=" + Format(Unreachable(client)) + " unowned=" + Unowned(client, masters, true) +
				" owners=" + Format(Owners(masters)) + " waitedMs=" + elapsedMs + " boundMs=" + boundMs);
			Assert.IsTrue(routed, step + ": client did not hold " + size +
				" answering nodes owning every partition within " + boundMs + "ms: nodes=" +
				Format(Names(client)) + " unreachable=" + Format(Unreachable(client)));
			return elapsedMs;
		}

		/// <summary>
		/// Assert the client holds one node per pod, each at an address the endpoint name resolves
		/// to and at that pod's own NodePort.
		/// </summary>
		public static void AssertNodesAtEndpoint(string step, L5Environment env, AerospikeClient client,
			IReadOnlyDictionary<string, PodEndpoint> endpoints)
		{
			List<string> resolved = Addresses(env.EndpointName);
			HashSet<string> matched = new HashSet<string>(StringComparer.Ordinal);

			foreach (Node node in client.Nodes)
			{
				Assert.IsTrue(endpoints.TryGetValue(node.Name, out PodEndpoint endpoint),
					step + ": node " + node.Name + " is no pod in " + Format(endpoints.Keys));
				Host h = node.Host;
				Console.WriteLine("L5-NODE step=" + step + " node=" + node.Name + " pod=" + endpoint.Pod.Name +
					" clientHost=" + h.name + ':' + h.port + " expected=" + env.EndpointName + ':' +
					endpoint.NodePort + " endpointResolvesTo=" + Format(resolved));
				Assert.IsTrue(matched.Add(endpoint.Pod.Name), endpoint.Pod.Name + " matched twice");
				Assert.AreEqual(endpoint.NodePort, h.port,
					step + ": " + node.Name + " (" + endpoint.Pod.Name + ") port");
				Assert.IsTrue(h.name.Equals(env.EndpointName, StringComparison.Ordinal) ||
					resolved.Contains(h.name),
					step + ": " + node.Name + " host " + h.name + " is not " + env.EndpointName +
					Format(resolved));
			}

			Assert.AreEqual(endpoints.Count, matched.Count, step + ": pods without a node");
		}

		/// <summary>
		/// Assert each pod advertises the endpoint name, verbatim, with its own NodePort as its
		/// alternate service address, and that peers-clear-alt lists every other pod likewise.
		/// </summary>
		public static void AssertAdvertisement(string step, L5Environment env, AerospikeClient client,
			IReadOnlyDictionary<string, PodEndpoint> endpoints)
		{
			foreach (Node node in client.Nodes)
			{
				Assert.IsTrue(endpoints.TryGetValue(node.Name, out PodEndpoint self),
					step + ": node " + node.Name + " is no pod in " + Format(endpoints.Keys));
				Dictionary<string, string> info = Info.Request(null, node, "service-clear-alt", "peers-clear-alt");
				string service = info["service-clear-alt"];
				string reply = info["peers-clear-alt"];
				SortedDictionary<string, List<string>> peers = Peers(reply);
				Console.WriteLine("L5-ADVERTISED step=" + step + " node=" + node.Name + " pod=" + self.Pod.Name +
					" service-clear-alt=" + service + " peers-clear-alt=" + reply);
				Assert.AreEqual(env.Advertised(self.NodePort), service,
					step + ": " + self.Pod.Name + " service-clear-alt");

				foreach (PodEndpoint peer in endpoints.Values)
				{
					if (ReferenceEquals(peer, self))
					{
						continue;
					}

					Assert.IsTrue(peers.TryGetValue(peer.NodeName, out List<string> addresses),
						step + ": " + self.Pod.Name + " peers-clear-alt missing " + peer.Pod.Name);
					CollectionAssert.AreEqual(new List<string> { env.Advertised(peer.NodePort) }, addresses,
						step + ": " + self.Pod.Name + " peers-clear-alt entry for " + peer.Pod.Name +
						" (" + peer.NodeName + ") in " + reply);
				}

				Assert.AreEqual(endpoints.Count - 1, peers.Count,
					step + ": " + self.Pod.Name + " peers-clear-alt peers " + Format(peers.Keys));
			}
		}

		public static SortedDictionary<string, List<string>> Peers(string reply)
		{
			Assert.IsNotNull(reply, "No peers reply");
			string[] head = reply.Split(',', 3);
			Assert.AreEqual(3, head.Length, "Peers reply " + reply);
			string defaultPort = head[1];
			SortedDictionary<string, List<string>> peers =
				new SortedDictionary<string, List<string>>(StringComparer.Ordinal);
			MatchCollection matches = Peer.Matches(head[2]);

			foreach (Match m in matches)
			{
				List<string> addresses = new List<string>();

				foreach (string address in m.Groups[3].Value.Split(','))
				{
					if (address.Length == 0)
					{
						continue;
					}

					bool hasPort = address.LastIndexOf(':') > address.LastIndexOf(']');
					addresses.Add(hasPort ? address : address + ':' + defaultPort);
				}

				peers[m.Groups[1].Value.ToUpperInvariant()] = addresses;
			}

			return peers;
		}

		/// <summary>
		/// Write and read one key in every partition of the namespace.
		/// </summary>
		public static void AssertEveryPartitionServed(string step, AerospikeClient client, string set)
		{
			Node[] masters = new Node[Node.PARTITIONS];
			Assert.AreEqual(0, Unowned(client, masters, false), step + ": unowned partitions");
			Key[] keys = new Key[Node.PARTITIONS];
			int found = 0;

			for (int i = 0; i < KeySearchLimit && found < Node.PARTITIONS; i++)
			{
				Key candidate = new Key(L5Environment.Namespace, set, step + '-' + i);
				int partition = (int)Partition.GetPartitionId(candidate.digest);

				if (keys[partition] == null)
				{
					keys[partition] = candidate;
					found++;
				}
			}

			Assert.AreEqual(Node.PARTITIONS, found, step + ": keys found for partitions");

			Stopwatch sw = Stopwatch.StartNew();
			WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
			Policy rp = new Policy(client.ReadPolicyDefault);

			for (int partition = 0; partition < Node.PARTITIONS; partition++)
			{
				string value = step + '-' + partition;
				client.Put(wp, keys[partition], new Bin("v", value));
				Record record = client.Get(rp, keys[partition]);
				Assert.IsNotNull(record, step + ": partition " + partition + " read nothing");
				Assert.AreEqual(value, record.GetString("v"), step + ": partition " + partition);
			}

			Console.WriteLine("L5-SERVED step=" + step + " partitions=" + Node.PARTITIONS +
				" writes=" + Node.PARTITIONS + " reads=" + Node.PARTITIONS + " owners=" +
				Format(Owners(masters)) + " elapsedMs=" + sw.ElapsedMilliseconds);
		}

		public static List<string> Names(AerospikeClient client)
		{
			List<string> names = new List<string>();

			foreach (Node node in client.Nodes)
			{
				names.Add(node.Name);
			}

			names.Sort(StringComparer.Ordinal);
			return names;
		}

		public static SortedDictionary<string, int> Owners(Node[] masters)
		{
			SortedDictionary<string, int> owners =
				new SortedDictionary<string, int>(StringComparer.Ordinal);

			foreach (Node node in masters)
			{
				if (node != null)
				{
					owners[node.Name] = owners.GetValueOrDefault(node.Name, 0) + 1;
				}
			}

			return owners;
		}

		public static List<string> Addresses(string name)
		{
			List<string> addresses = new List<string>();

			foreach (IPAddress address in Dns.GetHostAddresses(name))
			{
				addresses.Add(address.ToString());
			}

			return addresses;
		}

		/// <summary>
		/// Wait, at most <paramref name="boundMs"/>, for <see cref="QuietTendIntervals"/> tend
		/// intervals in which the client's errors and retries and the load's failed count do not
		/// grow and at least one command succeeds.
		/// </summary>
		public static void AwaitQuiet(string step, AerospikeClient client, Load load, long boundMs)
		{
			long windowMs = (long)QuietTendIntervals * L5Environment.TendInterval;
			long[] from = Counters(client, load);
			Stopwatch sw = Stopwatch.StartNew();
			bool quiet = K8sCluster.WaitFor(() =>
			{
				long[] now = Counters(client, load);

				if (now[0] != from[0] || now[1] != from[1] || now[2] != from[2])
				{
					Array.Copy(now, from, now.Length);
					return false;
				}

				return now[4] - from[4] >= windowMs && now[3] > from[3];
			}, boundMs, 50);
			long[] end = Counters(client, load);
			Console.WriteLine("L5-QUIET step=" + step + " windowMs=" + windowMs + " boundMs=" + boundMs +
				" waitedMs=" + sw.ElapsedMilliseconds + " errors=" + end[0] + " retries=" + end[1] +
				" failed=" + end[2] + " succeeded=" + end[3] + " quiet=" + quiet + " lastError=" +
				load.LastError);
			Assert.IsTrue(quiet, step + ": errors, retries or failed commands still grew, or no command " +
				"succeeded, in every " + windowMs + "ms window for " + boundMs + "ms");
		}

		public static long Errors(AerospikeClient client)
		{
			long errors = 0;

			foreach (Node node in client.Nodes)
			{
				errors += node.GetErrorCount() + node.GetTimeoutCount();
			}

			return errors;
		}

		private static long[] Counters(AerospikeClient client, Load load)
		{
			return new[]
			{
				Errors(client),
				client.Cluster.GetRetryCount(),
				Interlocked.Read(ref load.failed),
				Interlocked.Read(ref load.succeeded),
				Stopwatch.GetTimestamp() * 1000L / Stopwatch.Frequency
			};
		}

		private static string Format(IEnumerable<string> values)
		{
			return '[' + string.Join(", ", values) + ']';
		}

		private static string Format(IReadOnlyDictionary<string, int> values)
		{
			List<string> parts = new List<string>();

			foreach (KeyValuePair<string, int> entry in values)
			{
				parts.Add(entry.Key + '=' + entry.Value);
			}

			return '{' + string.Join(", ", parts) + '}';
		}

		/// <summary>
		/// Writes and reads on <see cref="LoadThreads"/> threads until disposed. A command that
		/// fails is counted and issued again until it succeeds, so the longest time from a
		/// command's first attempt to its success measures how long the client could not serve it.
		/// </summary>
		public sealed class Load : IDisposable
		{
			internal long succeeded;
			internal long failed;
			private string lastError;
			private long maxLatencyTicks;
			private long resetAtTicks = Stopwatch.GetTimestamp();
			private volatile bool stop;
			private readonly List<Task> tasks = new List<Task>();

			public long Succeeded => Interlocked.Read(ref succeeded);
			public long Failed => Interlocked.Read(ref failed);
			public string LastError => Volatile.Read(ref lastError);

			public Load(AerospikeClient client, string set)
			{
				WritePolicy wp = new WritePolicy(client.WritePolicyDefault)
				{
					maxRetries = LoadMaxRetries,
					sleepBetweenRetries = L5Environment.TendInterval
				};
				Policy rp = new Policy(client.ReadPolicyDefault)
				{
					maxRetries = LoadMaxRetries,
					sleepBetweenRetries = L5Environment.TendInterval
				};

				for (int t = 0; t < LoadThreads; t++)
				{
					int thread = t;
					tasks.Add(Task.Run(() =>
					{
						for (int i = 0; !stop; i++)
						{
							Key key = new Key(L5Environment.Namespace, set,
								"load-" + ((thread + i * LoadThreads) % LoadKeys));
							long first = Stopwatch.GetTimestamp();
							bool done = false;

							while (!done && !stop)
							{
								try
								{
									if (i % 2 == 0)
									{
										client.Put(wp, key, new Bin("v", i));
									}
									else
									{
										client.Get(rp, key);
									}

									Interlocked.Increment(ref succeeded);

									if (first - Volatile.Read(ref resetAtTicks) >= 0)
									{
										long latency = Stopwatch.GetTimestamp() - first;
										long current = Volatile.Read(ref maxLatencyTicks);

										while (latency > current &&
											Interlocked.CompareExchange(ref maxLatencyTicks, latency, current) != current)
										{
											current = Volatile.Read(ref maxLatencyTicks);
										}
									}

									done = true;
								}
								catch (Exception e)
								{
									Interlocked.Increment(ref failed);
									Volatile.Write(ref lastError, e.GetType().Name + ": " + e.Message);
								}
							}
						}
					}));
				}

				Assert.IsTrue(K8sCluster.WaitFor(() => Succeeded > 0, RoutingWaitMs, 10),
					"Load did not start within " + RoutingWaitMs + "ms: " + LastError);
			}

			/// <summary>
			/// Return the longest first-attempt-to-success time of the commands first attempted
			/// since the last reset, and reset it.
			/// </summary>
			public long ResetMaxLatencyMs()
			{
				Volatile.Write(ref resetAtTicks, Stopwatch.GetTimestamp());
				long ticks = Interlocked.Exchange(ref maxLatencyTicks, 0);
				return ticks * 1000L / Stopwatch.Frequency;
			}

			public long MaxLatencyMs()
			{
				return Volatile.Read(ref maxLatencyTicks) * 1000L / Stopwatch.Frequency;
			}

			public void Dispose()
			{
				stop = true;

				try
				{
					Task.WaitAll(tasks.ToArray(), TimeSpan.FromMilliseconds(LoadStopMs));
				}
				catch (AggregateException)
				{
					// Load threads exit once stop is set; ignore join failures on teardown.
				}
			}
		}
	}
}
