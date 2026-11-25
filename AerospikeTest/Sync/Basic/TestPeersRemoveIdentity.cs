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
using System.Net;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Node equality is by name; remove sets must keep both instances when a move replaces a node.
	/// </summary>
	[TestClass]
	public class TestPeersRemoveIdentity
	{
		[TestMethod]
		public void RemoveNodeKeepsSameNameInstancesDistinct()
		{
			ClientPolicy policy = new ClientPolicy
			{
				failIfNotConnected = false,
				timeout = 50,
				tendInterval = 250
			};
			AerospikeClient client = new AerospikeClient(policy);
			Cluster cluster = new Cluster(client, policy, null, Array.Empty<Host>());

			try
			{
				Peers peers = new Peers(2);
				Node oldNode = NodeNamed(cluster, "BB9A");
				Node newNode = NodeNamed(cluster, "BB9A");

				Assert.AreEqual(oldNode, newNode);
				Assert.IsFalse(object.ReferenceEquals(oldNode, newNode));

				peers.RemoveNode(oldNode);
				Assert.AreEqual(1, peers.removeNodes.Count);
				Assert.AreEqual(1, peers.nodesToRemove.Count);
				Assert.IsTrue(peers.nodesToRemove.Contains(oldNode));
				Assert.IsFalse(peers.nodesToRemove.Contains(newNode));

				peers.RemoveNode(newNode);
				Assert.AreEqual(1, peers.removeNodes.Count);
				Assert.AreEqual(2, peers.nodesToRemove.Count);
				Assert.IsTrue(peers.nodesToRemove.Contains(oldNode));
				Assert.IsTrue(peers.nodesToRemove.Contains(newNode));
			}
			finally
			{
				// Policy-only client has no owned cluster; only close the test Cluster.
				cluster.Close();
			}
		}

		private static Node NodeNamed(Cluster cluster, string name)
		{
			NodeValidator nv = new NodeValidator
			{
				name = name,
				primaryHost = new Host("127.0.0.1", 3000),
				primaryAddress = new IPEndPoint(IPAddress.Loopback, 3000)
			};
			return new Node(cluster, nv);
		}
	}
}
