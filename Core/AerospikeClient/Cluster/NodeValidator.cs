/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
		internal List<Host> aliases;
		internal Host primaryHost;
		internal IPEndPoint primaryAddress;
		internal Connection conn;
		internal uint features;

		/// <summary>
		/// Add node(s) referenced by seed host aliases. In most cases, aliases reference
		/// a single node.  If round robin DNS configuration is used, the seed host may have
		/// several aliases that reference different nodes in the cluster.
		/// </summary>
		public void SeedNodes(Cluster cluster, Host host, Dictionary<string, Node> nodesToAdd)
		{
			IPAddress[] addresses = SetAliases(cluster, host);
			Exception exception = null;
			bool found = false;

			foreach (IPAddress address in addresses)
			{
				try
				{
					ValidateAlias(cluster, address, host);
					found = true;

					if (! nodesToAdd.ContainsKey(name))
					{
						// New node found.
						Node node = cluster.CreateNode(this);
						nodesToAdd[name] = node;
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
					ValidateAlias(cluster, address, host);
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

			// Add capacity for current address aliases plus IPV6 address and hostname.
			aliases = new List<Host>(addresses.Length + 2);

			foreach (IPAddress address in addresses)
			{
				aliases.Add(new Host(address.ToString(), host.tlsName, host.port));
			}
			return addresses;
		}

		private void ValidateAlias(Cluster cluster, IPAddress ipAddress, Host alias)
		{
			IPEndPoint address = new IPEndPoint(ipAddress, alias.port);
			Connection conn = cluster.CreateConnection(alias.tlsName, address, cluster.connectionTimeout);

			try
			{
				if (cluster.user != null)
				{
					AdminCommand command = new AdminCommand(ThreadLocalData.GetBuffer(), 0);
					command.Authenticate(conn, cluster.user, cluster.password);
				}
				Dictionary<string, string> map;
				bool hasClusterName = cluster.HasClusterName;

				if (hasClusterName)
				{
					map = Info.Request(conn, "node", "features", "cluster-name");
				}
				else
				{
					map = Info.Request(conn, "node", "features");
				}
				
				string nodeName;

				if (! map.TryGetValue("node", out nodeName))
				{
					throw new AerospikeException.InvalidNode();
				}

				if (hasClusterName)
				{
					string id;

					if (! map.TryGetValue("cluster-name", out id) || ! cluster.clusterName.Equals(id))
					{
						throw new AerospikeException.InvalidNode("Node " + nodeName + ' ' + alias + ' ' + " expected cluster name '" + cluster.clusterName + "' received '" + id + "'");
					}
				}

				this.name = nodeName;
				this.primaryHost = alias;
				this.primaryAddress = address;
				this.conn = conn;
				SetFeatures(map);
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
				string featuresString = map["features"];
				string[] list = featuresString.Split(';');

				foreach (string feature in list)
				{
					if (feature.Equals("geo"))
					{
						this.features |= Node.HAS_GEO;
					}
					else if (feature.Equals("float"))
					{
						this.features |= Node.HAS_DOUBLE;
					}
					else if (feature.Equals("batch-index"))
					{
						this.features |= Node.HAS_BATCH_INDEX;
					}
					else if (feature.Equals("replicas-all"))
					{
						this.features |= Node.HAS_REPLICAS_ALL;
					}
					else if (feature.Equals("peers"))
					{
						this.features |= Node.HAS_PEERS;
					}
				}
			}
			catch (Exception)
			{
				// Unexpected exception. Use defaults.
			}
		}
	}
}
