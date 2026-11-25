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
using System.Text.RegularExpressions;
using Aerospike.Client;
using L2Node = Aerospike.Test.L2Topology.L2Node;
using Load = Aerospike.Test.L2Topology.Load;
using Outcome = Aerospike.Test.L2Topology.Outcome;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing strategy section 4.3.2 row F-3 against the port-range profile of
	/// test/discovery/run_l2.sh, standing in for NLB source NAT port exhaustion (section 4.7). Toxiproxy
	/// runs with a narrow local port range, which caps the upstream connections it can open to each
	/// node. In its own class and profile so that a failing F-3 does not fail the other rows.
	/// </summary>
	[TestClass]
	public class TestL2SourcePortRange
	{
		private const string BLOCKER = "F-3 needs DISCOVERY_PROXY_PORT_RANGE, Toxiproxy with a narrow " +
			"net.ipv4.ip_local_port_range (run_l2.sh L2_PROFILE=port-range)";
		private const string SET = "l2ports";
		private const int KEY_COUNT = 1000;
		private const int RECOVERY_KEY_COUNT = 100;
		private const int SOCKET_TIMEOUT = 2000;
		private const int TOTAL_TIMEOUT = 4000;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		/// <summary>
		/// F-3: with more concurrent commands than the proxy has source ports for, and maxConnsPerNode
		/// raised above that, the client surfaces a clear error within a bound from socketTimeout and
		/// totalTimeout rather than hanging, the error types and counts are observable from the client,
		/// and commands succeed again once the load drops.
		/// </summary>
		[TestMethod]
		public void F3SourcePortExhaustionSurfacesErrorAndRecovers()
		{
			TestL2Topology.SkipUnless(L2Topology.ProxyPortRange.Length > 0, BLOCKER);

			string[] range = Regex.Split(L2Topology.ProxyPortRange.Trim(), @"\s+");
			int ports = int.Parse(range[1]) - int.Parse(range[0]) + 1;
			List<L2Node> nodes = L2Topology.Nodes();
			int threads = 2 * ports * nodes.Count;
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes));
			policy.maxConnsPerNode = 4 * threads;
			Console.WriteLine("L2-F3-RANGE proxyLocalPortRange=" + L2Topology.ProxyPortRange + " ports=" + ports +
				" nodes=" + nodes.Count + " threads=" + threads + " maxConnsPerNode=" + policy.maxConnsPerNode +
				" defaultMaxConnsPerNode=" + new ClientPolicy().maxConnsPerNode);

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				L2Topology.AssertAllPartitionsOwned("F-3-before", client);

				WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
				wp.socketTimeout = SOCKET_TIMEOUT;
				wp.totalTimeout = TOTAL_TIMEOUT;
				long bound = wp.totalTimeout + wp.socketTimeout;
				Dictionary<string, long[]> start = L2Topology.ConnectionStats(client);
				long begin = TestL2Topology.NanoTime();

				Load load = new Load("l2-f3", threads, (thread, iteration) =>
					client.Put(wp, new Key(L2Topology.Namespace, SET, "f3-" + ((thread * 31 + iteration) % KEY_COUNT)),
						new Bin("v", iteration)));
				bool finished = false;

				try
				{
					L2Topology.WaitFor(() => load.Errors().Count >= threads);
				}
				finally
				{
					finished = load.Stop(bound);
				}

				List<Outcome> errors = load.Errors();
				long firstErrorMs = -1;
				long maxElapsed = 0;
				List<Outcome> timeouts = new List<Outcome>();

				foreach (Outcome outcome in errors)
				{
					long at = (outcome.StartNanos - begin) / 1000000L + outcome.ElapsedMs;
					firstErrorMs = firstErrorMs < 0 ? at : Math.Min(firstErrorMs, at);
					maxElapsed = Math.Max(maxElapsed, outcome.ElapsedMs);

					if (outcome.Error is AerospikeException ae && ae.Result == ResultCode.TIMEOUT)
					{
						timeouts.Add(outcome);
					}
				}

				Dictionary<string, long[]> end = L2Topology.ConnectionStats(client);
				long errorDelta = 0;
				long pooled = 0;

				foreach (KeyValuePair<string, long[]> entry in end)
				{
					long[] v = entry.Value;
					errorDelta += v[L2Topology.Errors] - start[entry.Key][L2Topology.Errors];
					pooled = Math.Max(pooled, v[L2Topology.InUse] + v[L2Topology.InPool]);
				}

				Console.WriteLine("L2-F3-LOAD commands=" + load.Outcomes.Count + " successes=" + load.Successes() +
					" errors=" + TestL2Topology.FmtMap(L2Topology.Kinds(errors)) + " firstErrorMs=" + firstErrorMs + " maxErrorElapsedMs=" + maxElapsed +
					" boundMs=" + bound + " threadsReturned=" + finished + " clientErrorDelta=" + errorDelta +
					" maxConnsPerNodeAfterLoad=" + pooled);
				Console.WriteLine("L2-F3-STATS " + L2Topology.Format(end));

				if (errors.Count > 0)
				{
					Console.WriteLine("L2-F3-ERROR " + errors[0].Kind() + ' ' + TestL2Topology.Messages(errors[0].Error));
				}

				Assert.IsTrue(finished, "F-3: command threads still blocked " + bound + "ms after stop");
				Assert.IsFalse(errors.Count == 0,
					"F-3: " + threads + " threads never exceeded the proxy's " + ports + " source ports per node");
				Assert.IsTrue(maxElapsed <= bound, "F-3: an error took " + maxElapsed + "ms, bound " + bound + "ms");
				Assert.IsTrue(errorDelta > 0, "F-3: client counted no error: " + L2Topology.Format(end));

				foreach (Outcome outcome in errors)
				{
					Assert.IsTrue(outcome.Error is AerospikeException, "F-3: not a client error: " + outcome.Kind());
				}
				Assert.AreEqual(0, timeouts.Count,
					"F-3: source port exhaustion surfaced as a client timeout: " + TestL2Topology.FmtMap(L2Topology.Kinds(timeouts)));

				Policy rp = new Policy(client.ReadPolicyDefault);
				long dropped = TestL2Topology.NanoTime();
				bool recovered = L2Topology.WaitFor(() =>
				{
					foreach (Node node in client.Nodes)
					{
						if (!node.Active)
						{
							return false;
						}
					}
					return RoundSucceeds(client, wp, rp, 4 * nodes.Count);
				});
				long recoveredMs = (TestL2Topology.NanoTime() - dropped) / 1000000L;
				List<string> failures = new List<string>();

				for (int i = 0; i < RECOVERY_KEY_COUNT; i++)
				{
					Key key = new Key(L2Topology.Namespace, SET, "f3-" + i);

					try
					{
						client.Put(wp, key, new Bin("v", i));
						Assert.AreEqual(i, client.Get(rp, key).GetInt("v"));
					}
					catch (AerospikeException ae)
					{
						failures.Add(i + ": " + ae.GetType().Name + ' ' + ae.Message);
					}
				}
				Console.WriteLine("L2-F3-RECOVERY recovered=" + recovered + " ms=" + recoveredMs + " keys=" + RECOVERY_KEY_COUNT +
					" failed=" + failures.Count + " nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)));
				Assert.IsTrue(recovered, "F-3: no successful round of commands after the load dropped");
				Assert.AreEqual(0, failures.Count, "F-3: commands failed after the load dropped: " + TestL2Topology.Fmt(failures));
			}
		}

		private static bool RoundSucceeds(AerospikeClient client, WritePolicy wp, Policy rp, int count)
		{
			try
			{
				for (int i = 0; i < count; i++)
				{
					Key key = new Key(L2Topology.Namespace, SET, "f3-round-" + i);
					client.Put(wp, key, new Bin("v", i));
					client.Get(rp, key);
				}
				return true;
			}
			catch (AerospikeException)
			{
				return false;
			}
		}
	}
}
