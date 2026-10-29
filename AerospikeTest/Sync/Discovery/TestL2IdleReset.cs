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
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using Aerospike.Client;
using L2Node = Aerospike.Test.L2Topology.L2Node;
using Load = Aerospike.Test.L2Topology.Load;

namespace Aerospike.Test
{
	/// <summary>
	/// Testing strategy section 4.3.2 row F-1 against the default <see cref="L2Topology"/>, in its own
	/// run_l2.sh profile because it fails on Java until pooled sockets are checked on checkout. Every
	/// toxic the case adds is removed in <see cref="Restore"/>.
	/// </summary>
	[TestClass]
	public class TestL2IdleReset
	{
		private const string BLOCKER = "F-1 needs DISCOVERY_IDLE_RESET (run_l2.sh L2_PROFILE=idle-reset)";
		private const string SET = "l2f";
		private const int WARM_THREADS = 4;
		private const int WARM_ITERATIONS = 50;
		private const int KEY_COUNT = 100;
		private const int PROBE_TIMEOUT = 5000;

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
		/// F-1: idle flows torn down with RST, as an NLB tears down an idle flow. Every listener's
		/// connections are made to close with RST rather than FIN, and the listeners are then cycled
		/// while the client's connections sit idle in the pool. With the default <see cref="ClientPolicy"/>
		/// reaping the next commands succeed: the pool drops the dead sockets before use, or the policy's
		/// retry absorbs the error, and no command fails to the caller. A raw probe connection on every
		/// listener and the client's connection and error counters prove the resets happened.
		/// </summary>
		[TestMethod]
		public void F1IdleResetAbsorbedByPool()
		{
			TestL2Topology.SkipUnless(L2Topology.IdleReset.Length > 0, BLOCKER);
			List<L2Node> nodes = L2Topology.Nodes();
			ClientPolicy policy = L2Topology.Policy();
			policy.addressTranslator = new PortMapAddressTranslator(L2Topology.PortMapByEndpoint(nodes));
			Assert.AreEqual(new ClientPolicy().maxSocketIdle, policy.maxSocketIdle);

			foreach (L2Node node in nodes)
			{
				AddToxic(node.Proxy, "f1-rst", "reset_peer", 0.0, "{\"timeout\":0}");
			}

			using (AerospikeClient client = new AerospikeClient(policy, L2Topology.Seed()))
			{
				L2Topology.WaitNodes(client, L2Topology.NodeNames());
				L2Topology.AssertAllPartitionsOwned("F-1", client);

				WritePolicy wp = new WritePolicy(client.WritePolicyDefault);
				Policy rp = new Policy(client.ReadPolicyDefault);
				Load warm = new Load("l2-f1-warm", WARM_THREADS, (thread, iteration) =>
				{
					Key key = new Key(L2Topology.Namespace, SET, "f1-" + ((thread * WARM_ITERATIONS + iteration) % KEY_COUNT));
					client.Put(wp, key, new Bin("v", iteration));
					client.Get(rp, key);
				});
				L2Topology.WaitFor(() => warm.Outcomes.Count >= WARM_THREADS * WARM_ITERATIONS);
				Assert.IsTrue(warm.Stop(L2Topology.WaitMs), "Warm-up threads blocked");
				Assert.AreEqual(0, warm.Errors().Count, "Warm-up errors: " + TestL2Topology.FmtMap(L2Topology.Kinds(warm.Errors())));

				List<Socket> probes = new List<Socket>();
				long pooled = 0;

				try
				{
					foreach (L2Node node in nodes)
					{
						probes.Add(Probe(node));
					}

					Dictionary<string, long[]> idle = L2Topology.ConnectionStats(client);

					foreach (long[] v in idle.Values)
					{
						pooled += v[L2Topology.InPool];
					}
					Console.WriteLine("L2-F1-IDLE maxSocketIdle=" + policy.maxSocketIdle + " pooled=" + pooled + ' ' +
						L2Topology.Format(idle));
					Assert.IsTrue(pooled >= nodes.Count, "No idle pooled connection to reset");

					foreach (L2Node node in nodes)
					{
						L2Topology.SetListener(node.Proxy, false);
						L2Topology.SetListener(node.Proxy, true);
					}

					for (int i = 0; i < nodes.Count; i++)
					{
						string result = ProbeResult(probes[i]);
						Console.WriteLine("L2-F1-PROBE listener=" + nodes[i].Port + " result=" + result);
						Assert.IsTrue(result.StartsWith("RST"),
							"F-1: listener " + nodes[i].Port + " did not reset an idle flow with RST: " + result);
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
				Dictionary<string, long[]> start = L2Topology.ConnectionStats(client);
				List<string> failures = new List<string>();
				int commands = 0;

				for (int i = 0; i < KEY_COUNT; i++)
				{
					Key key = new Key(L2Topology.Namespace, SET, "f1-" + i);

					try
					{
						commands++;
						client.Put(wp, key, new Bin("v", i));
					}
					catch (AerospikeException ae)
					{
						failures.Add("put " + i + ": " + ae.GetType().Name + ' ' + TestL2Topology.Messages(ae));
					}

					try
					{
						commands++;
						client.Get(rp, key);
					}
					catch (AerospikeException ae)
					{
						failures.Add("get " + i + ": " + ae.GetType().Name + ' ' + TestL2Topology.Messages(ae));
					}
				}

				Dictionary<string, long[]> end = L2Topology.ConnectionStats(client);
				long closed = 0;
				long errors = 0;

				foreach (KeyValuePair<string, long[]> entry in end)
				{
					long[] s = start[entry.Key];
					closed += entry.Value[L2Topology.Closed] - s[L2Topology.Closed];
					errors += entry.Value[L2Topology.Errors] - s[L2Topology.Errors];
				}

				long retries = client.GetClusterStats().RetryCount - before.RetryCount;
				Console.WriteLine("L2-F1-RESULT commands=" + commands + " failedToCaller=" + failures.Count + " pooledReset=" +
					pooled + " closedDelta=" + closed + " errorDelta=" + errors + " retryDelta=" + retries +
					" writeMaxRetries=" + wp.maxRetries + " readMaxRetries=" + rp.maxRetries);
				Console.WriteLine("L2-F1-STATS " + L2Topology.Format(end));

				foreach (string failure in failures)
				{
					Console.WriteLine("L2-F1-FAILED " + failure);
				}
				Assert.IsTrue(closed >= pooled,
					"F-1: " + pooled + " reset pooled connections but only " + closed + " closed by the client");
				Assert.AreEqual(0, failures.Count, "F-1: commands failed to the caller after idle RST: " + TestL2Topology.Fmt(failures));
			}
		}

		private void AddToxic(string proxy, string name, string type, double toxicity, string attributes)
		{
			L2Topology.AddToxic(proxy, name, type, "upstream", toxicity, attributes);
			toxics.Add(new string[] {proxy, name});
		}

		/// <summary>
		/// Open a connection through a listener and complete one info request on it, so Toxiproxy has
		/// linked it to the node.
		/// </summary>
		private static Socket Probe(L2Node node)
		{
			Socket socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
			socket.ReceiveTimeout = PROBE_TIMEOUT;
			socket.SendTimeout = PROBE_TIMEOUT;
			Task connect = socket.ConnectAsync(L2Topology.Host, node.Port);

			if (!connect.Wait(PROBE_TIMEOUT))
			{
				socket.Close();
				throw new IOException("Connect to " + L2Topology.Host + ':' + node.Port + " timed out");
			}

			byte[] command = Encoding.UTF8.GetBytes("node\n");
			byte[] buffer = new byte[8 + command.Length];
			BinaryPrimitives.WriteInt64BigEndian(buffer, (2L << 56) | (1L << 48) | (long)command.Length);
			Buffer.BlockCopy(command, 0, buffer, 8, command.Length);
			socket.Send(buffer);

			byte[] headerBytes = new byte[8];
			ReadFully(socket, headerBytes);
			long header = BinaryPrimitives.ReadInt64BigEndian(headerBytes);
			byte[] reply = new byte[(int)(header & 0xFFFFFFFFFFFFL)];
			ReadFully(socket, reply);
			return socket;
		}

		private static void ReadFully(Socket socket, byte[] buffer)
		{
			int offset = 0;

			while (offset < buffer.Length)
			{
				int count = socket.Receive(buffer, offset, buffer.Length - offset, SocketFlags.None);

				if (count <= 0)
				{
					throw new EndOfStreamException();
				}
				offset += count;
			}
		}

		/// <summary>
		/// Read the next byte of a probe connection: RST when the peer reset it, FIN when it closed it.
		/// </summary>
		private static string ProbeResult(Socket socket)
		{
			try
			{
				byte[] one = new byte[1];
				int count = socket.Receive(one);
				return count <= 0 ? "FIN" : "DATA " + one[0];
			}
			catch (SocketException e)
			{
				return (e.SocketErrorCode == SocketError.ConnectionReset ? "RST " : "ERROR ") + e;
			}
			catch (IOException e)
			{
				return (e.Message != null && e.Message.Contains("reset", StringComparison.OrdinalIgnoreCase) ? "RST " : "ERROR ") + e;
			}
		}
	}
}
