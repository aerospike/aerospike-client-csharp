/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
		internal Connection primaryConn;
		internal byte[] sessionToken;
		internal DateTime? sessionExpiration;
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
						primaryConn.Close();
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
			Connection conn = (cluster.tlsPolicy != null) ?
				new TlsConnection(cluster.tlsPolicy, alias.tlsName, address, cluster.connectionTimeout, cluster.maxSocketIdleMillis, null) :
				new Connection(address, cluster.connectionTimeout, cluster.maxSocketIdleMillis, null);

			try
			{
				if (cluster.user != null)
				{
					// Login
					AdminCommand admin = new AdminCommand(ThreadLocalData.GetBuffer(), 0);
					admin.Login(cluster, conn, alias, out sessionToken, out sessionExpiration);

					if (cluster.tlsPolicy != null && cluster.tlsPolicy.forLoginOnly)
					{
						// Switch to using non-TLS socket.
						SwitchClear sc = new SwitchClear(cluster, conn, sessionToken);
						conn.Close();
						alias = sc.clearHost;
						address = sc.clearAddress;
						conn = sc.clearConn;
					}
				}

				Dictionary<string, string> map;
				bool hasClusterName = cluster.HasClusterName;

				if (hasClusterName)
				{
					map = Info.Request(conn, "node", "partition-generation", "features", "cluster-name");
				}
				else
				{
					map = Info.Request(conn, "node", "partition-generation", "features");
				}
				
				string nodeName;

				if (! map.TryGetValue("node", out nodeName))
				{
					throw new AerospikeException.InvalidNode();
				}

				string genString;
				int gen;

				if (!map.TryGetValue("partition-generation", out genString))
				{
					throw new AerospikeException.InvalidNode();
				}

				try
				{
					gen = Convert.ToInt32(genString);
				}
				catch (Exception)
				{
					throw new AerospikeException.InvalidNode("Invalid partition-generation: " + genString);
				}

				if (gen == -1)
				{
					throw new AerospikeException.InvalidNode("Node " + nodeName + ' ' + alias + " is not yet fully initialized");
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
				this.primaryConn = conn;
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
					else if (feature.Equals("replicas"))
					{
						this.features |= Node.HAS_REPLICAS;
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

	sealed class SwitchClear
	{
		internal Host clearHost;
		internal IPEndPoint clearAddress;
		internal Connection clearConn;

		// Switch from TLS connection to non-TLS connection.
		internal SwitchClear(Cluster cluster, Connection conn, byte[] sessionToken)
		{
			// Obtain non-TLS addresses.
			string command = cluster.useServicesAlternate ? "service-clear-alt" : "service-clear-std";
			string result = Info.Request(conn, command);
			List<Host> hosts = Host.ParseServiceHosts(result);

			// Find first valid non-TLS host.
			foreach (Host host in hosts)
			{
				try
				{
					clearHost = host;

					String alternativeHost;
					if (cluster.ipMap != null && cluster.ipMap.TryGetValue(clearHost.name, out alternativeHost))
					{
						clearHost = new Host(alternativeHost, clearHost.port);
					}

					IPAddress[] addresses = Connection.GetHostAddresses(clearHost.name, cluster.connectionTimeout);

					foreach (IPAddress ia in addresses)
					{
						try
						{
							clearAddress = new IPEndPoint(ia, clearHost.port);
							clearConn = new Connection(clearAddress, cluster.connectionTimeout, cluster.maxSocketIdleMillis, null);

							try
							{
								AdminCommand admin = new AdminCommand(ThreadLocalData.GetBuffer(), 0);

								if (! admin.Authenticate(cluster, clearConn, sessionToken))
								{
									throw new AerospikeException("Authentication failed");
								}
								return; // Authenticated clear connection.
							}
							catch (Exception)
							{
								clearConn.Close();
							}
						}
						catch (Exception)
						{
							// Try next address.
						}
					}
				}
				catch (Exception)
				{
					// Try next host.
				}
			}
			throw new AerospikeException("Invalid non-TLS address: " + result);
		}
	}
}
