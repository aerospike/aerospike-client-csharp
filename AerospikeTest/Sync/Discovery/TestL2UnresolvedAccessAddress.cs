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

namespace Aerospike.Test
{
	/// <summary>
	/// A node that configures an alternate-access-address which does not resolve in its container,
	/// against the unresolved-access profile. <see cref="L2Topology.VerbatimAccessAddress"/> says
	/// whether the server under test advertises access-address hostnames verbatim (SERVER-1839).
	/// When it does, the node joins and advertises the hostname as configured; when it does not, the
	/// server resolves the hostname at startup, does not start, and names the address in its log.
	/// </summary>
	[TestClass]
	public class TestL2UnresolvedAccessAddress
	{
		private const string BLOCKER = "Unresolved access address needs DISCOVERY_UNRESOLVED_HOSTNAME and " +
			"DISCOVERY_CONTROL_DIR (L2_PROFILE=unresolved-access)";
		private const int StatusPollMs = 500;

		private string addedProxy;
		private string addedNode;

		[TestCleanup]
		public void Restore()
		{
			if (addedProxy != null)
			{
				L2Topology.RemoveListener(addedProxy);
				addedProxy = null;
			}

			if (addedNode != null)
			{
				L2Topology.Control("stop " + addedNode);
				addedNode = null;
				L2Topology.WaitClusterStable(L2Topology.Nodes().Count);
			}
		}

		/// <summary>
		/// Start a node whose alternate-access-address is DISCOVERY_UNRESOLVED_HOSTNAME with its own
		/// alternate-access-port. A server that advertises it verbatim joins and returns hostname:port
		/// unchanged in service-clear-alt; a server that resolves it exits at startup with a CRITICAL
		/// log line naming the hostname.
		/// </summary>
		[TestMethod]
		public void UnresolvedAlternateAccessAddress()
		{
			TestL2Topology.SkipUnless(L2Topology.UnresolvedHostname.Length > 0 && L2Topology.ControlDir.Length > 0, BLOCKER);

			List<L2Node> nodes = L2Topology.Nodes();
			L2Node first = nodes[0];
			string hostname = L2Topology.UnresolvedHostname;
			bool verbatim = "1".Equals(L2Topology.VerbatimAccessAddress);
			int port = L2Topology.NextListenerPort();
			string endpoint = hostname + ':' + port;
			string container = L2Topology.Control("start alternate-port=" + port)["container"];
			addedNode = container;
			Console.WriteLine("L2-UNRESOLVED-START container=" + container + " alternate=" + endpoint +
				" verbatimExpected=" + verbatim);

			if (verbatim)
			{
				L2Topology.AddListener(container, port, container + ':' + L2Topology.UpstreamPort(first.Proxy));
				addedProxy = container;
				L2Topology.WaitClusterStable(nodes.Count + 1);

				Dictionary<string, string> status = Status(container);
				AssertUnresolved(status);
				Assert.AreEqual("true", status["running"], container + " running");

				Dictionary<string, string> info = Info.Request(L2Topology.Host, port, "node", "service-clear-alt");
				string advertised = info["service-clear-alt"];
				string peer = L2Topology.PeerName(first, "peers-clear-alt", endpoint);
				Console.WriteLine("L2-UNRESOLVED-ADVERTISED node=" + info["node"] + " service-clear-alt=" + advertised +
					" listedBy=" + first.Name + " as=" + peer);
				Assert.AreEqual(endpoint, advertised);
				Assert.AreEqual(info["node"], peer);
			}
			else
			{
				bool exited = L2Topology.WaitFor(() => !"true".Equals(Status(container)["running"]), StatusPollMs);
				Dictionary<string, string> status = Status(container);
				List<string> lines = LogLines(status["log"], hostname);

				foreach (string line in lines)
				{
					Console.WriteLine("L2-UNRESOLVED-LOG " + line);
				}
				Assert.IsTrue(exited, container + " still running after " + L2Topology.WaitMs + "ms");
				AssertUnresolved(status);
				Assert.AreNotEqual("0", status["exit-code"], container + " exit code");
				Assert.IsTrue(lines.Exists(line => line.Contains("CRITICAL")),
					"No CRITICAL log line names " + hostname + ": " + string.Join(" | ", lines));
				L2Topology.WaitClusterStable(nodes.Count);
			}
		}

		private static void AssertUnresolved(Dictionary<string, string> status)
		{
			Console.WriteLine("L2-UNRESOLVED-LOOKUP getent hosts " + L2Topology.UnresolvedHostname + " in=" +
				status["lookup-in"] + " exit=" + status["lookup-exit"] + " output=[" + status["lookup"].Trim() + ']');
			Assert.IsFalse(string.IsNullOrEmpty(status["lookup-in"]), "No lookup");
			Assert.AreEqual("2", status["lookup-exit"], L2Topology.UnresolvedHostname + " resolves: " + status["lookup"]);
		}

		private static Dictionary<string, string> Status(string container)
		{
			return L2Topology.Control("status " + container);
		}

		/// <summary>
		/// Return the lines of a server log that name <paramref name="hostname"/>, other than the config echo.
		/// </summary>
		private static List<string> LogLines(string log, string hostname)
		{
			List<string> lines = new List<string>();

			foreach (string line in File.ReadAllLines(log))
			{
				if (line.Contains(hostname) && !line.Contains("alternate-access-address " + hostname))
				{
					lines.Add(line);
				}
			}
			return lines;
		}
	}
}
