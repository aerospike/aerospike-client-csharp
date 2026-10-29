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
using Aerospike.Client;
using L2Node = Aerospike.Test.L2Topology.L2Node;
using Load = Aerospike.Test.L2Topology.Load;
using Outcome = Aerospike.Test.L2Topology.Outcome;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing strategy section 4.3.2 row F-4 against the default <see cref="L2Topology"/>: a Toxiproxy
	/// timeout toxic on one node's listener holds data with no reply and no close, as a conntrack
	/// mismatch does. No client marks down a node the cluster still lists (strategy TG-24), so F-4
	/// asserts bounded timeouts, no blocked threads, recovery, and the error-rate circuit breaker. Every
	/// toxic a case adds is removed in <see cref="Restore"/>.
	/// </summary>
	[TestClass]
	public class TestL2FaultInjection
	{
		private const string SET = "l2f";
		private const string TOXIC = "f4-hold";
		private const int THREADS = 4;
		private const int KEY_COUNT = 20;
		private const int SOCKET_TIMEOUT = 500;
		private const int TOTAL_TIMEOUT = 1500;
		private const int MAX_RETRIES = 2;
		private const int BREAKER_MAX_ERROR_RATE = 2;
		private const int BREAKER_WINDOW = 4;

		private readonly List<string[]> toxics = new List<string[]>();

		private delegate void Drop(AerospikeClient client, L2Node victim, WritePolicy wp, Load load, long injected, long timeoutBound);

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		[TestCleanup]
		public void Restore()
		{
			Log.SetCallback(null);

			foreach (string[] toxic in toxics)
			{
				L2Topology.RemoveToxic(toxic[0], toxic[1]);
			}
			toxics.Clear();
		}

		/// <summary>
		/// F-4: commands to the node fail with a timeout within a bound from socketTimeout and
		/// totalTimeout, every error is a timeout, no command thread stays blocked past the bound, and the
		/// node serves again once the toxic is removed. Whether the client marks the node down is
		/// recorded, not asserted (TG-24).
		/// </summary>
		[TestMethod]
		public void F4SilentDropTimesOutWithoutBlocking()
		{
			ClientPolicy policy = L2Topology.Policy();
			TestL2Topology.CaptureWarnings();
			Run(policy, (client, victim, wp, load, injected, timeoutBound) =>
			{
				Assert.IsTrue(L2Topology.WaitFor(() => Count(load.Errors(), ResultCode.TIMEOUT) >= THREADS),
					"F-4: fewer than " + THREADS + " timeouts after the drop");
				bool finished = load.Stop(timeoutBound);
				List<Outcome> errors = load.Errors();
				Node node = client.Cluster.GetNode(victim.Name);
				long firstTimeoutMs = -1;
				long maxElapsed = 0;

				foreach (Outcome outcome in errors)
				{
					maxElapsed = Math.Max(maxElapsed, outcome.ElapsedMs);
					Assert.AreEqual(ResultCode.TIMEOUT, ResultCodeOf(outcome),
						"F-4: error is not a timeout: " + outcome.Kind() + ' ' + TestL2Topology.Messages(outcome.Error));
					long at = (outcome.StartNanos - injected) / 1000000L + outcome.ElapsedMs;

					if (firstTimeoutMs < 0 || at < firstTimeoutMs)
					{
						firstTimeoutMs = at;
					}
				}

				int refreshFailures = 0;

				foreach (string warning in TestL2Topology.Warnings)
				{
					if (warning.Contains(victim.Name) && warning.Contains("refresh failed"))
					{
						refreshFailures++;
					}
				}

				Console.WriteLine("L2-F4-TIMEOUT victim=" + victim.Name + " listener=" + victim.Port + " firstTimeoutMs=" +
					firstTimeoutMs + " maxErrorElapsedMs=" + maxElapsed + " timeoutBoundMs=" + timeoutBound + " errors=" +
					TestL2Topology.FmtMap(L2Topology.Kinds(errors)) + " successes=" + load.Successes() + " threadsReturned=" + finished);
				Console.WriteLine("L2-F4-NODE active=" + (node != null && node.Active) + " nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) +
					" owners=" + TestL2Topology.FmtMap(L2Topology.Owners(client)) + " refreshFailedWarnings=" + refreshFailures);

				Assert.IsTrue(finished, "F-4: command threads still blocked " + timeoutBound + "ms after stop");
				Assert.IsTrue(firstTimeoutMs <= timeoutBound,
					"F-4: first timeout " + firstTimeoutMs + "ms after the drop, bound " + timeoutBound + "ms");
				Assert.IsTrue(maxElapsed <= timeoutBound, "F-4: an error took " + maxElapsed + "ms, bound " + timeoutBound + "ms");
			});
		}

		/// <summary>
		/// F-4 with a low maxErrorRate: the timeouts trip the node's error-rate circuit breaker within a
		/// bound from errorRateWindow and tendInterval, and commands started after the trip fail fast with
		/// MAX_ERROR_RATE instead of waiting out socketTimeout.
		/// </summary>
		[TestMethod]
		public void F4SilentDropTripsCircuitBreaker()
		{
			ClientPolicy policy = L2Topology.Policy();
			policy.maxErrorRate = BREAKER_MAX_ERROR_RATE;
			policy.errorRateWindow = BREAKER_WINDOW;
			Run(policy, (client, victim, wp, load, injected, timeoutBound) =>
			{
				long tripBound = (long)(policy.errorRateWindow + 1) * policy.tendInterval + timeoutBound;
				bool tripped = L2Topology.WaitFor(() => Count(load.Errors(), ResultCode.MAX_ERROR_RATE) > 0);
				bool finished = load.Stop(timeoutBound);
				List<Outcome> backoffs = new List<Outcome>();
				long trippedNanos = long.MaxValue;

				foreach (Outcome outcome in load.Errors())
				{
					if (ResultCodeOf(outcome) == ResultCode.MAX_ERROR_RATE)
					{
						backoffs.Add(outcome);
						trippedNanos = Math.Min(trippedNanos, outcome.StartNanos + outcome.ElapsedMs * 1000000L);
					}
				}

				long trippedMs = backoffs.Count == 0 ? -1 : (trippedNanos - injected) / 1000000L;
				int afterTrip = 0;
				long maxBackoffMs = 0;

				foreach (Outcome outcome in backoffs)
				{
					if (outcome.StartNanos >= trippedNanos)
					{
						afterTrip++;
						maxBackoffMs = Math.Max(maxBackoffMs, outcome.ElapsedMs);
					}
				}

				Console.WriteLine("L2-F4-BREAKER victim=" + victim.Name + " maxErrorRate=" + policy.maxErrorRate +
					" errorRateWindow=" + policy.errorRateWindow + " tendInterval=" + policy.tendInterval + " trippedMs=" + trippedMs +
					" tripBoundMs=" + tripBound + " backoffsAfterTrip=" + afterTrip + " maxBackoffElapsedMs=" + maxBackoffMs + " errors=" +
					TestL2Topology.FmtMap(L2Topology.Kinds(load.Errors())) + " threadsReturned=" + finished);

				Assert.IsTrue(tripped, "F-4: circuit breaker never tripped: " + TestL2Topology.FmtMap(L2Topology.Kinds(load.Errors())));
				Assert.IsTrue(finished, "F-4: command threads still blocked " + timeoutBound + "ms after stop");
				Assert.IsTrue(trippedMs <= tripBound, "F-4: breaker tripped " + trippedMs + "ms after the drop, bound " + tripBound + "ms");
				Assert.IsTrue(afterTrip > 0, "F-4: no command started after the breaker tripped");
				Assert.IsTrue(maxBackoffMs < wp.socketTimeout, "F-4: a command started after the trip took " + maxBackoffMs +
					"ms to fail, socketTimeout " + wp.socketTimeout + "ms");
			});
		}

		/// <summary>
		/// Load the victim node with writes, drop its traffic silently, run the check, then remove the
		/// toxic and assert the node serves again and every partition is owned.
		/// </summary>
		private void Run(ClientPolicy policy, Drop drop)
		{
			List<L2Node> nodes = L2Topology.Nodes();
			L2Node victim = nodes[nodes.Count - 1];
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes));

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				Node[] masters = L2Topology.AssertAllPartitionsOwned("F-4-before", client);
				Node node = client.Cluster.GetNode(victim.Name);
				List<Key> keys = new List<Key>();

				for (int i = 0; keys.Count < KEY_COUNT; i++)
				{
					Key key = new Key(L2Topology.Namespace, SET, "f4-" + i);

					if (ReferenceEquals(masters[(int)Partition.GetPartitionId(key.digest)], node))
					{
						keys.Add(key);
					}
				}

				WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
				wp.socketTimeout = SOCKET_TIMEOUT;
				wp.totalTimeout = TOTAL_TIMEOUT;
				wp.maxRetries = MAX_RETRIES;
				long timeoutBound = wp.totalTimeout + wp.socketTimeout;
				Load load = new Load("l2-f4", THREADS, (thread, iteration) =>
					client.Put(wp, keys[(thread + iteration * THREADS) % keys.Count], new Bin("v", iteration)));

				try
				{
					Assert.IsTrue(L2Topology.WaitFor(() => load.Successes() >= THREADS), "No command succeeded before the drop");
					long injected = TestL2Topology.NanoTime();
					AddToxic(victim.Proxy, TOXIC, "timeout", 1.0, "{\"timeout\":0}");
					drop(client, victim, wp, load, injected, timeoutBound);
				}
				finally
				{
					load.Stop(timeoutBound);
				}

				RemoveToxic(victim.Proxy, TOXIC);
				long removed = TestL2Topology.NanoTime();
				Key probe = keys[0];
				bool back = L2Topology.WaitFor(() => L2Topology.NodeNames(client).Contains(victim.Name) &&
					client.Cluster.GetNode(victim.Name).Active && L2Topology.Owners(client).ContainsKey(victim.Name) &&
					Writes(client, wp, probe));
				Console.WriteLine("L2-F4-RECOVERED back=" + back + " ms=" + (TestL2Topology.NanoTime() - removed) / 1000000L +
					" nodes=" + TestL2Topology.Fmt(L2Topology.NodeNames(client)) + " owners=" + TestL2Topology.FmtMap(L2Topology.Owners(client)));
				Assert.IsTrue(back, "F-4: " + victim.Name + " did not come back after the toxic was removed");
				L2Topology.AssertAllPartitionsOwned("F-4-after", client);
			}
		}

		private void AddToxic(string proxy, string name, string type, double toxicity, string attributes)
		{
			L2Topology.AddToxic(proxy, name, type, "upstream", toxicity, attributes);
			toxics.Add(new string[] {proxy, name});
		}

		private void RemoveToxic(string proxy, string name)
		{
			toxics.RemoveAll(toxic => toxic[0] == proxy && toxic[1] == name);
			L2Topology.RemoveToxic(proxy, name);
		}

		private static int ResultCodeOf(Outcome outcome)
		{
			return outcome.Error is AerospikeException ae ? ae.Result : ResultCode.CLIENT_ERROR;
		}

		private static long Count(List<Outcome> outcomes, int resultCode)
		{
			return outcomes.Count(o => ResultCodeOf(o) == resultCode);
		}

		private static bool Writes(AerospikeClient client, WritePolicy policy, Key key)
		{
			try
			{
				client.Put(policy, key, new Bin("v", 0));
				return true;
			}
			catch (Exception e) when (e is AerospikeException.InvalidNode || e is AerospikeException.Timeout ||
				e is AerospikeException.Connection || e is AerospikeException.Backoff)
			{
				return false;
			}
		}
	}
}
