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
using System.Text;
using Aerospike.Client;
using MessageList = Aerospike.Test.ConformanceEnv.MessageList;

namespace Aerospike.Test
{
	/// <summary>
	/// <see cref="ClientPolicy.useServicesAlternate"/> with nodes that have no alternate access address,
	/// against the two two node clusters of the aerospike-client-conformance alt-missing environment. In
	/// the missing cluster node A advertises 127.0.0.1 and its published host port as its alternate
	/// access address and node B has none. In the control cluster nodes C and D both advertise one. The
	/// clusters arrive as the environment variables of its values file:
	/// <list type="bullet">
	/// <item><c>ENV_HOST</c>: host every node is published on</item>
	/// <item><c>MISSING_NODE_A_NAME</c>, <c>MISSING_NODE_A_PORT</c>: missing cluster node with an alternate address</item>
	/// <item><c>MISSING_NODE_B_NAME</c>, <c>MISSING_NODE_B_PORT</c>: missing cluster node without an alternate address</item>
	/// <item><c>CONTROL_NODE_A_NAME</c>, <c>CONTROL_NODE_A_PORT</c>, <c>CONTROL_NODE_B_NAME</c>: control cluster nodes</item>
	/// </list>
	/// </summary>
	[TestClass]
	[DoNotParallelize]
	public class TestAlternateMissing
	{
		private const int TEND_INTERVAL = 250;
		private const int MISSING_TENDS = 8;
		private const int JOIN_WAIT_MS = 10000;
		private const string ALTERNATE_FLAG = "useServicesAlternate";

		// The client reports a node without an alternate address as a warning when it is a peer.
		// Capture warnings that name the flag together with errors.
		private static readonly MessageList errors = new MessageList();

		private static string host;
		private static string nodeA;
		private static int portA;
		private static string nodeB;
		private static int portB;
		private static string nodeC;
		private static int portC;
		private static string nodeD;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			ConformanceEnv.Require("ENV_HOST", "MISSING_NODE_A_NAME", "MISSING_NODE_A_PORT", "MISSING_NODE_B_NAME",
				"MISSING_NODE_B_PORT", "CONTROL_NODE_A_NAME", "CONTROL_NODE_A_PORT", "CONTROL_NODE_B_NAME");

			host = ConformanceEnv.Get("ENV_HOST");
			nodeA = ConformanceEnv.Get("MISSING_NODE_A_NAME");
			portA = ConformanceEnv.GetInt("MISSING_NODE_A_PORT");
			nodeB = ConformanceEnv.Get("MISSING_NODE_B_NAME");
			portB = ConformanceEnv.GetInt("MISSING_NODE_B_PORT");
			nodeC = ConformanceEnv.Get("CONTROL_NODE_A_NAME");
			portC = ConformanceEnv.GetInt("CONTROL_NODE_A_PORT");
			nodeD = ConformanceEnv.Get("CONTROL_NODE_B_NAME");

			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.ERROR || (level == Log.Level.WARN && message.Contains(ALTERNATE_FLAG)))
				{
					errors.Add(message);
				}
				Console.WriteLine("CLIENT-LOG " + level + ' ' + message);
			});
		}

		[ClassCleanup]
		public static void ClassCleanup()
		{
			Log.SetCallback(null);
		}

		[TestInitialize]
		public void ClearErrors()
		{
			errors.Clear();
		}

		[TestMethod]
		public void PeerWithoutAlternateIsNotAdded()
		{
			string peersAlt = Info.Request(host, portA, "peers-clear-alt");
			Console.WriteLine("ALT-MISSING-PEERS " + nodeA + " peers-clear-alt=" + peersAlt);

			using (AerospikeClient client = new AerospikeClient(NewPolicy(), new Host(host, portA)))
			{
				for (int i = 0; i < MISSING_TENDS; i++)
				{
					Thread.Sleep(TEND_INTERVAL);
					CollectionAssert.AreEqual(new List<string> {nodeA}, ConformanceEnv.NodeNames(client));
				}

				List<string> missing = MissingErrors(nodeB);
				Console.WriteLine("ALT-MISSING-RESULT peer-missing nodes=" + client.Nodes.Length + ' ' +
					ConformanceEnv.Fmt(ConformanceEnv.NodeNames(client)) + " errors=" + missing.Count);
				Assert.IsTrue(missing.Count > 0, "No error logged for node " + nodeB + ": " + ConformanceEnv.Fmt(errors.Snapshot()));
				Console.WriteLine("ALT-MISSING-LOG peer-missing " + missing[0]);
				Assert.IsTrue(missing.Count >= 2, "Node " + nodeB + " not retried on a later tend: " + ConformanceEnv.Fmt(missing));
			}
		}

		[TestMethod]
		public void SeedWithoutAlternateFailsInit()
		{
			AerospikeException caught = null;

			try
			{
				new AerospikeClient(NewPolicy(), new Host(host, portB)).Close();
			}
			catch (AerospikeException e)
			{
				caught = e;
			}

			Assert.IsNotNull(caught, "Expected init failure for seed " + nodeB + " without an alternate address");

			// The client wraps the missing alternate address error raised for the seed, so read the whole chain.
			string message = Messages(caught);
			Console.WriteLine("ALT-MISSING-RESULT seed-missing exception=" + caught.GetType().FullName);
			Console.WriteLine("ALT-MISSING-LOG seed-missing " + message);
			Assert.IsTrue(message.Contains(nodeB), message);
			Assert.IsTrue(message.Contains(ALTERNATE_FLAG), message);
		}

		[TestMethod]
		public void BothAlternateJoin()
		{
			using (AerospikeClient client = new AerospikeClient(NewPolicy(), new Host(host, portC)))
			{
				long deadline = Environment.TickCount64 + JOIN_WAIT_MS;

				while (client.Nodes.Length < 2 && Environment.TickCount64 < deadline)
				{
					Thread.Sleep(TEND_INTERVAL);
				}

				List<string> names = ConformanceEnv.NodeNames(client);
				Console.WriteLine("ALT-MISSING-RESULT control nodes=" + names.Count + ' ' + ConformanceEnv.Fmt(names) +
					" errors=" + errors.Count);
				CollectionAssert.AreEqual(new List<string> {nodeC, nodeD}, names);
				Assert.AreEqual(0, errors.Count, ConformanceEnv.Fmt(errors.Snapshot()));
			}
		}

		private static ClientPolicy NewPolicy()
		{
			ClientPolicy policy = new ClientPolicy();
			policy.useServicesAlternate = true;
			policy.tendInterval = TEND_INTERVAL;
			return policy;
		}

		private static List<string> MissingErrors(string nodeName)
		{
			List<string> list = new List<string>();

			foreach (string error in errors.Snapshot())
			{
				if (error.Contains("Node " + nodeName) && error.Contains(ALTERNATE_FLAG))
				{
					list.Add(error);
				}
			}
			return list;
		}

		private static string Messages(Exception t)
		{
			StringBuilder sb = new StringBuilder();

			for (Exception e = t; e != null; e = e.InnerException)
			{
				if (sb.Length > 0)
				{
					sb.Append(" <- ");
				}
				sb.Append(e.Message);
			}
			return sb.ToString();
		}
	}
}
