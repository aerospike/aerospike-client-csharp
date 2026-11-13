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
using Aerospike.Client;
using L2Node = Aerospike.Test.L2Topology.L2Node;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing strategy section 4.3.2 row F-2 against the low-mtu profile of test/discovery/run_l2.sh,
	/// standing in for PrivateLink's MTU of 8500 with no path MTU discovery (section 4.7). The client
	/// network is created with a small MTU and the client writes and reads records well above it. In
	/// its own class and profile so that a failing F-2 does not fail the other rows.
	/// </summary>
	[TestClass]
	public class TestL2LowMtu
	{
		private const string BLOCKER = "F-2 needs DISCOVERY_CLIENT_NETWORK_MTU, a client network created with " +
			"a small MTU (run_l2.sh L2_PROFILE=low-mtu)";
		private const string SET = "l2mtu";
		private static readonly int[] RECORD_SIZES = {64 * 1024, 1024 * 1024};
		private const int SOCKET_TIMEOUT = 5000;
		private const int TOTAL_TIMEOUT = 10000;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			L2Topology.RequireEnv();
		}

		/// <summary>
		/// F-2: with the client network's MTU well below the record size, a write and a read of a 1 MiB
		/// value through every node's listener succeed, or fail with a diagnosable error, within a bound
		/// from socketTimeout and totalTimeout. A hang or a client timeout fails the row.
		/// </summary>
		[TestMethod]
		public void F2LargeRecordOverLowMtuNeverHangs()
		{
			TestL2Topology.SkipUnless(L2Topology.ClientNetworkMtu.Length > 0, BLOCKER);

			int mtu = int.Parse(File.ReadAllText("/sys/class/net/eth0/mtu").Trim());
			Console.WriteLine("L2-F2-MTU eth0=" + mtu + " network=" + L2Topology.ClientNetworkMtu);
			Assert.AreEqual(int.Parse(L2Topology.ClientNetworkMtu), mtu);

			List<L2Node> nodes = L2Topology.Nodes();
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes));

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				Node[] masters = L2Topology.AssertAllPartitionsOwned("F-2", client);
				WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
				wp.socketTimeout = SOCKET_TIMEOUT;
				wp.totalTimeout = TOTAL_TIMEOUT;
				Policy rp = new Policy(client.ReadPolicyDefault);
				rp.socketTimeout = SOCKET_TIMEOUT;
				rp.totalTimeout = TOTAL_TIMEOUT;
				long bound = TOTAL_TIMEOUT + SOCKET_TIMEOUT;
				Random random = new Random(mtu);

				foreach (Node node in client.Nodes)
				{
					Key key = KeyOn(masters, node);

					foreach (int size in RECORD_SIZES)
					{
						byte[] value = new byte[size];
						random.NextBytes(value);

						string write = Run(bound, () => client.Put(wp, key, new Bin("v", value)));
						string read = Run(bound, () =>
						{
							Record record = client.Get(rp, key);
							Assert.IsNotNull(record);
							CollectionAssert.AreEqual(value, (byte[])record.GetValue("v"));
						});
						Console.WriteLine("L2-F2-RECORD node=" + node.Name + " listener=" + node.Host.port + " mtu=" + mtu +
							" bytes=" + size + " write=" + write + " read=" + read + " boundMs=" + bound);
					}
				}
			}
		}

		private static Key KeyOn(Node[] masters, Node node)
		{
			for (int i = 0; ; i++)
			{
				Key key = new Key(L2Topology.Namespace, SET, "f2-" + i);

				if (ReferenceEquals(masters[(int)Partition.GetPartitionId(key.digest)], node))
				{
					return key;
				}
			}
		}

		/// <summary>
		/// Run a command with a bound and return <c>ok &lt;ms&gt;</c>, or <c>error &lt;ms&gt; &lt;message&gt;</c> for a
		/// diagnosable error. A hang past the bound or a client timeout fails the row.
		/// </summary>
		private static string Run(long bound, Action command)
		{
			Stopwatch watch = Stopwatch.StartNew();
			Task task = Task.Factory.StartNew(command, TaskCreationOptions.LongRunning);

			bool completed;

			try
			{
				completed = task.Wait(TimeSpan.FromMilliseconds(bound));
			}
			catch (AggregateException ae)
			{
				long ms = watch.ElapsedMilliseconds;
				Exception cause = ae.InnerException;

				if (cause is AerospikeException aerospike)
				{
					string message = TestL2Topology.Messages(aerospike);
					Console.WriteLine("L2-F2-ERROR " + ms + "ms " + aerospike.GetType().Name + ' ' + message);
					Assert.IsTrue(aerospike.Result != ResultCode.TIMEOUT,
						"F-2: client timeout instead of a diagnosable error after " + ms + "ms: " + message);
					return "error " + ms + "ms " + message;
				}

				if (cause is AssertFailedException)
				{
					throw cause;
				}
				throw;
			}

			if (!completed)
			{
				Assert.Fail("F-2: command still running after " + bound + "ms");
			}
			return "ok " + watch.ElapsedMilliseconds + "ms";
		}
	}
}
