/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
using System;
using System.Net;
using System.Text;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class NodeValidator
	{
		internal string name;
		internal Host[] aliases;
		internal IPEndPoint address;
		internal Connection conn;
		internal bool hasBatchIndex;
		internal bool hasReplicasAll;
		internal bool hasDouble;
		internal bool hasGeo;

		/// <summary>
		/// Add node(s) referenced by seed host aliases. In most cases, aliases reference
		/// a single node.  If round robin DNS configuration is used, the seed host may have
		/// several aliases that reference different nodes in the cluster.
		/// </summary>
		public void SeedNodes(Cluster cluster, Host host, List<Node> list)
		{
			IPAddress[] addresses = SetAliases(cluster, host);
			Exception exception = null;
			bool found = false;

			foreach (IPAddress address in addresses)
			{
				try
				{
					ValidateAlias(cluster, address, host.port);
					found = true;

					if (!FindNodeName(list, name))
					{
						// New node found.
						Node node = cluster.CreateNode(this);
						cluster.AddAliases(node);
						list.Add(node);
					}
					else
					{
						// Node already referenced. Close connection.
						conn.Close();
					}
				}
				catch (Exception e)
				{
					// Log and continue to next address.
					if (Log.DebugEnabled())
					{
						Log.Debug("Alias " + address + " failed: " + Util.GetErrorMessage(e));
					}

					if (exception == null)
					{
						exception = e;
					}
				}
			}

			if (!found)
			{
				// Exception can't be null here because SetAliases()/Connection.GetHostAddresses()
				// will throw exception if aliases length is zero.
				throw exception;
			}
		}

		/// <summary>
		/// Verify that a host alias references a valid node.
		/// </summary>
		public void ValidateNode(Cluster cluster, Host host)
		{
			IPAddress[] addresses = SetAliases(cluster, host);
			Exception exception = null;

			foreach (IPAddress address in addresses)
			{
				try
				{
					ValidateAlias(cluster, address, host.port);
					return;
				}
				catch (Exception e)
				{
					// Log and continue to next address.
					if (Log.DebugEnabled())
					{
						Log.Debug("Alias " + address + " failed: " + Util.GetErrorMessage(e));
					}

					if (exception == null)
					{
						exception = e;
					}
				}
			}

			// Exception can't be null here because SetAliases()/Connection.GetHostAddresses()
			// will throw exception if aliases length is zero.
			throw exception;
		}

		private IPAddress[] SetAliases(Cluster cluster, Host host)
		{
			IPAddress[] addresses = Connection.GetHostAddresses(host.name, cluster.connectionTimeout);
			aliases = new Host[addresses.Length];

			for (int i = 0; i < addresses.Length; i++)
			{
				aliases[i] = new Host(addresses[i].ToString(), host.port);
			}
			return addresses;
		}

		private void ValidateAlias(Cluster cluster, IPAddress ipAddress, int port)
		{
			IPEndPoint address = new IPEndPoint(ipAddress, port);
			Connection conn = new Connection(address, cluster.connectionTimeout);

			try
			{
				if (cluster.user != null)
				{
					AdminCommand command = new AdminCommand(ThreadLocalData.GetBuffer(), 0);
					command.Authenticate(conn, cluster.user, cluster.password);
				}
				Dictionary<string, string> map = Info.Request(conn, "node", "features");
				string nodeName;

				if (map.TryGetValue("node", out nodeName))
				{
					this.name = nodeName;
					this.address = address;
					this.conn = conn;
					SetFeatures(map);
					return;
				}
				else
				{
					throw new AerospikeException.InvalidNode();
				}
			}
			catch (Exception)
			{
				conn.Close();
				throw;
			}
		}

		private void SetFeatures(Dictionary<string, string> map)
		{
			try
			{
				string features = map["features"];
				string[] list = features.Split(';');

				foreach (string feature in list)
				{
					if (feature.Equals("geo"))
					{
						this.hasGeo = true;
					}

					if (feature.Equals("float"))
					{
						this.hasDouble = true;
					}

					if (feature.Equals("batch-index"))
					{
						this.hasBatchIndex = true;
					}

					if (feature.Equals("replicas-all"))
					{
						this.hasReplicasAll = true;
					}

					if (this.hasDouble && this.hasBatchIndex && this.hasReplicasAll)
					{
						break;
					}
				}
			}
			catch (Exception)
			{
				// Unexpected exception. Use defaults.
			}
		}

		private static bool FindNodeName(List<Node> list, string name)
		{
			foreach (Node node in list)
			{
				if (node.Name.Equals(name))
				{
					return true;
				}
			}
			return false;
		}
	}
}
