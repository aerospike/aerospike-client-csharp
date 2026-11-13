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
using System.Net.Sockets;
using Aerospike.Client;
using L2Node = Aerospike.Test.L2Topology.L2Node;

namespace Aerospike.Test
{
	/// <summary>
	/// Async counterpart of <see cref="TestL2IdleReset"/> against the default <see cref="L2Topology"/>,
	/// in the idle-reset profile. Every toxic the case adds is removed in <see cref="Restore"/>.
	/// </summary>
	[TestClass]
	public class TestL2AsyncIdleReset
	{
		private const string BLOCKER = "Async idle reset needs DISCOVERY_IDLE_RESET (run_l2.sh L2_PROFILE=idle-reset)";
		private const string SET = "l2fa";
		private const int WARM_CONCURRENCY = 8;
		private const int WARM_ROUNDS = 25;
		private const int KEY_COUNT = 100;
		private static readonly TimeSpan CommandTimeout = TimeSpan.FromMilliseconds(30000);

		private readonly List<string[]> toxics = new List<string[]>();

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		[TestCleanup]
		public void Restore()
		{
			foreach (string[] toxic in toxics)
			{
				L2Topology.RemoveToxic(toxic[0], toxic[1]);
			}
			toxics.Clear();
		}

		/// <summary>
		/// Idle flows torn down with RST, as an NLB tears down an idle flow. Every listener's
		/// connections are made to close with RST rather than FIN, and the listeners are then cycled
		/// while the client's async connections sit idle in the pool. With the default
		/// <see cref="ClientPolicy"/> the next async commands succeed and no command fails to the
		/// caller. A raw probe connection on every listener and the client's async connection
		/// counters prove the resets happened.
		/// </summary>
		[TestMethod]
		public async Task F1AsyncIdleResetAbsorbedByPool()
		{
			TestL2Topology.SkipUnless(L2Topology.IdleReset.Length > 0, BLOCKER);
			List<L2Node> nodes = L2Topology.Nodes();
			AsyncClientPolicy policy = new AsyncClientPolicy
			{
				tendInterval = L2Topology.TendInterval,
				timeout = 1000,
				asyncMaxCommands = 300,
				addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes))
			};
			Assert.AreEqual(new ClientPolicy().maxSocketIdle, policy.maxSocketIdle);

			foreach (L2Node node in nodes)
			{
				AddToxic(node.Proxy, "fa-rst", "reset_peer", 0.0, "{\"timeout\":0}");
			}

			using (AsyncClient client = new AsyncClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				L2Topology.AssertAllPartitionsOwned("async idle reset", client);

				WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
				Policy rp = new Policy(client.ReadPolicyDefault);
				List<string> warmErrors = new List<string>();

				for (int round = 0; round < WARM_ROUNDS; round++)
				{
					List<Task> futures = new List<Task>();

					for (int i = 0; i < WARM_CONCURRENCY; i++)
					{
						Key key = new Key(L2Topology.Namespace, SET, "fa-" + ((round * WARM_CONCURRENCY + i) % KEY_COUNT));
						futures.Add(client.Put(wp, CancellationToken.None, key, new Bin("v", round)));
						futures.Add(client.Get(rp, CancellationToken.None, key));
					}

					foreach (Task future in futures)
					{
						AerospikeException ae = await Await(future);

						if (ae != null)
						{
							warmErrors.Add(ae.GetType().Name + ' ' + TestL2Topology.Messages(ae));
						}
					}
				}
				Assert.AreEqual(0, warmErrors.Count, "Warm-up errors: " + TestL2Topology.Fmt(warmErrors));

				List<Socket> probes = new List<Socket>();
				long pooled = 0;

				try
				{
					foreach (L2Node node in nodes)
					{
						probes.Add(TestL2IdleReset.Probe(node));
					}

					Dictionary<string, long[]> idle = L2Topology.AsyncConnectionStats(client);

					foreach (long[] v in idle.Values)
					{
						pooled += v[L2Topology.InPool];
					}
					Console.WriteLine("L2-ASYNC-IDLE maxSocketIdle=" + policy.maxSocketIdle + " pooled=" + pooled + ' ' +
						L2Topology.Format(idle));
					Assert.IsTrue(pooled >= nodes.Count, "No idle pooled async connection to reset");

					foreach (L2Node node in nodes)
					{
						L2Topology.SetListener(node.Proxy, false);
						L2Topology.SetListener(node.Proxy, true);
					}

					for (int i = 0; i < nodes.Count; i++)
					{
						string result = TestL2IdleReset.ProbeResult(probes[i]);
						Console.WriteLine("L2-ASYNC-PROBE listener=" + nodes[i].Port + " result=" + result);
						Assert.IsTrue(result.StartsWith("RST"),
							"Listener " + nodes[i].Port + " did not reset an idle flow with RST: " + result);
					}
				}
				finally
				{
					foreach (Socket probe in probes)
					{
						probe.Close();
					}
				}

				ClusterStats before = client.GetClusterStats();
				Dictionary<string, long[]> start = L2Topology.AsyncConnectionStats(client);
				List<string> failures = new List<string>();
				int commands = 0;

				// Async pools are LIFO. Issue each phase at once so every idle pooled channel is checked out.
				foreach (bool write in new bool[] {true, false})
				{
					List<Task> futures = new List<Task>();

					for (int i = 0; i < KEY_COUNT; i++)
					{
						Key key = new Key(L2Topology.Namespace, SET, "fa-" + i);
						futures.Add(write
							? client.Put(wp, CancellationToken.None, key, new Bin("v", i))
							: client.Get(rp, CancellationToken.None, key));
					}

					for (int i = 0; i < KEY_COUNT; i++)
					{
						commands++;
						AerospikeException ae = await Await(futures[i]);

						if (ae != null)
						{
							failures.Add((write ? "put " : "get ") + i + ": " + ae.GetType().Name + ' ' +
								TestL2Topology.Messages(ae));
						}
					}
				}

				Dictionary<string, long[]> end = L2Topology.AsyncConnectionStats(client);
				List<string> undetected = new List<string>();
				long closed = 0;
				long errors = 0;

				foreach (KeyValuePair<string, long[]> entry in end)
				{
					long[] s = start[entry.Key];
					long nodeClosed = entry.Value[L2Topology.Closed] - s[L2Topology.Closed];

					if (nodeClosed == 0)
					{
						undetected.Add(entry.Key);
					}
					closed += nodeClosed;
					errors += entry.Value[L2Topology.Errors] - s[L2Topology.Errors];
				}

				long retries = client.GetClusterStats().RetryCount - before.RetryCount;
				Console.WriteLine("L2-ASYNC-RESULT commands=" + commands + " failedToCaller=" + failures.Count +
					" pooledReset=" + pooled + " closedDelta=" + closed + " errorDelta=" + errors +
					" retryDelta=" + retries + " writeMaxRetries=" + wp.maxRetries + " readMaxRetries=" + rp.maxRetries);
				Console.WriteLine("L2-ASYNC-STATS " + L2Topology.Format(end));

				foreach (string failure in failures)
				{
					Console.WriteLine("L2-ASYNC-FAILED " + failure);
				}
				Assert.IsTrue(undetected.Count == 0,
					"no reset pooled async connection was detected on " + TestL2Topology.Fmt(undetected));
				Assert.AreEqual(0, failures.Count,
					"commands failed to the caller after idle RST: " + TestL2Topology.Fmt(failures));
			}
		}

		private void AddToxic(string proxy, string name, string type, double toxicity, string attributes)
		{
			L2Topology.AddToxic(proxy, name, type, "upstream", toxicity, attributes);
			toxics.Add(new string[] {proxy, name});
		}

		private static async Task<AerospikeException> Await(Task future)
		{
			try
			{
				Task completed = await Task.WhenAny(future, Task.Delay(CommandTimeout));

				if (completed != future)
				{
					return new AerospikeException.Timeout(CommandTimeout.Milliseconds, true);
				}

				await future;
				return null;
			}
			catch (AerospikeException ae)
			{
				return ae;
			}
		}
	}
}
