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
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	/// <summary>
	/// Server node representation.  This class manages server node connections and health status.
	/// </summary>
	public class Node
	{
		/// <summary>
		/// Number of partitions for each namespace.
		/// </summary>
		public const int PARTITIONS = 4096;

		public const int HAS_GEO = (1 << 0);
		public const int HAS_DOUBLE = (1 << 1);
		public const int HAS_BATCH_INDEX = (1 << 2);
		public const int HAS_REPLICAS_ALL = (1 << 3);
		public const int HAS_PEERS = (1 << 4); 
		
		protected internal readonly Cluster cluster;
		private readonly string name;
		private readonly Host host;
		protected internal readonly List<Host> aliases;
		protected internal readonly IPEndPoint address;
		private Connection tendConnection;
		private readonly BlockingCollection<Connection> connectionQueue;
		private int connectionCount;
		protected internal int partitionGeneration = -1;
		protected internal int peersGeneration = -1;
		protected internal int peersCount;
		protected internal int referenceCount;
		protected internal int failures;
		protected internal readonly uint features;
		protected internal bool partitionChanged;
		protected internal volatile bool active = true;

		/// <summary>
		/// Initialize server node with connection parameters.
		/// </summary>
		/// <param name="cluster">collection of active server nodes</param>
		/// <param name="nv">connection parameters</param>
		public Node(Cluster cluster, NodeValidator nv)
		{
			this.cluster = cluster;
			this.name = nv.name;
			this.aliases = nv.aliases;
			this.host = nv.primaryHost;
			this.address = nv.primaryAddress;
			this.tendConnection = nv.conn;
			this.features = nv.features;

			connectionQueue = new BlockingCollection<Connection>(cluster.connectionQueueSize);
		}

		~Node()
		{
			// Close connections that slipped through the cracks on race conditions.
			CloseConnections();
		}

		/// <summary>
		/// Request current status from server node.
		/// </summary>
		public void Refresh(Peers peers)
		{
			if (!active)
			{
				return;
			}

			try
			{
				if (tendConnection.IsClosed())
				{
					tendConnection = cluster.CreateConnection(host.tlsName, address, cluster.connectionTimeout);
				}

				if (peers.usePeers)
				{
					Dictionary<string, string> infoMap = Info.Request(tendConnection, "node", "peers-generation", "partition-generation");
					VerifyNodeName(infoMap);
					VerifyPeersGeneration(infoMap, peers);
					VerifyPartitionGeneration(infoMap);
				}
				else
				{
					string[] commands = cluster.useServicesAlternate ?
						new string[] { "node", "partition-generation", "services-alternate" } :
						new string[] { "node", "partition-generation", "services" };

					Dictionary<string, string> infoMap = Info.Request(tendConnection, commands);
					VerifyNodeName(infoMap);
					VerifyPartitionGeneration(infoMap);
					AddFriends(infoMap, peers);
				}
				peers.refreshCount++;
				failures = 0;
			}
			catch (Exception e)
			{
				RefreshFailed(e);
			}
		}
	
		private void VerifyNodeName(Dictionary<string, string> infoMap)
		{
			// If the node name has changed, remove node from cluster and hope one of the other host
			// aliases is still valid.  Round-robbin DNS may result in a hostname that resolves to a
			// new address.
			string infoName = infoMap["node"];

			if (infoName == null || infoName.Length == 0)
			{
				throw new AerospikeException.Parse("Node name is empty");
			}

			if (!name.Equals(infoName))
			{
				// Set node to inactive immediately.
				active = false;
				throw new AerospikeException("Node name has changed. Old=" + name + " New=" + infoName);
			}
		}

		private void VerifyPeersGeneration(Dictionary<string, string> infoMap, Peers peers)
		{
			string genString = infoMap["peers-generation"];

			if (genString == null || genString.Length == 0)
			{
				throw new AerospikeException.Parse("peers-generation is empty");
			}

			int gen = Convert.ToInt32(genString);

			if (peersGeneration != gen)
			{
				peers.genChanged = true;
			}
		}

		private void VerifyPartitionGeneration(Dictionary<string, string> infoMap)
		{
			string genString = infoMap["partition-generation"];

			if (genString == null || genString.Length == 0)
			{
				throw new AerospikeException.Parse("partition-generation is empty");
			}

			int gen = Convert.ToInt32(genString);

			if (partitionGeneration != gen)
			{
				this.partitionChanged = true;
			}
		}

		private void AddFriends(Dictionary<string, string> infoMap, Peers peers)
		{
			// Parse the service addresses and add the friends to the list.
			String command = cluster.useServicesAlternate ? "services-alternate" : "services";
			string friendString = infoMap[command];

			if (friendString == null || friendString.Length == 0)
			{
				peersCount = 0;
				return;
			}

			string[] friendNames = friendString.Split(';');
			peersCount = friendNames.Length;

			foreach (string friend in friendNames)
			{
				string[] friendInfo = friend.Split(':');
				string hostname = friendInfo[0];
				string alternativeHost;

				if (cluster.ipMap != null && cluster.ipMap.TryGetValue(hostname, out alternativeHost))
				{
					hostname = alternativeHost;
				}

				int port = Convert.ToInt32(friendInfo[1]);
				Host host = new Host(hostname, port);
				Node node;

				// Check global aliases for existing cluster.
				if (!cluster.aliases.TryGetValue(host, out node))
				{
					// Check local aliases for this tend iteration.	
					if (!peers.hosts.Contains(host))
					{
						PrepareFriend(host, peers);
					}
				}
				else
				{
					node.referenceCount++;
				}
			}
		}

		private bool PrepareFriend(Host host, Peers peers)
		{
			try
			{
				NodeValidator nv = new NodeValidator();
				nv.ValidateNode(cluster, host);

				// Check for duplicate nodes in nodes slated to be added.
				Node node;
				if (peers.nodes.TryGetValue(nv.name, out node))
				{
					// Duplicate node name found.  This usually occurs when the server 
					// services list contains both internal and external IP addresses 
					// for the same node.
					nv.conn.Close();
					peers.hosts.Add(host);
					node.aliases.Add(host);
					return true;
				}

				// Check for duplicate nodes in cluster.
				if (cluster.nodesMap.TryGetValue(nv.name, out node))
				{
					nv.conn.Close();
					peers.hosts.Add(host);
					node.aliases.Add(host);
					node.referenceCount++;
					cluster.aliases[host] = node;
					return true;
				}

				node = cluster.CreateNode(nv);
				peers.hosts.Add(host);
				peers.nodes[nv.name] = node;
				return true;
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn("Add node " + host + " failed: " + Util.GetErrorMessage(e));
				}
				return false;
			}
		}

		protected internal void RefreshPeers(Peers peers)
		{
			// Do not refresh peers when node connection has already failed during this cluster tend iteration.
			if (failures > 0 || !active)
			{
				return;
			}

			try
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Update peers for node " + this);
				}

				PeerParser parser = new PeerParser(cluster, tendConnection, peers.peers);
				peersGeneration = parser.generation;
				peersCount = peers.peers.Count;

				foreach (Peer peer in peers.peers)
				{
					if (FindPeerNode(cluster, peers, peer.nodeName))
					{
						// Node already exists. Do not even try to connect to hosts.				
						continue;
					}

					// Find first host that connects.
					foreach (Host host in peer.hosts)
					{
						try
						{
							// Attempt connection to host.
							NodeValidator nv = new NodeValidator();
							nv.ValidateNode(cluster, host);

							if (!peer.nodeName.Equals(nv.name))
							{
								// Must look for new node name in the unlikely event that node names do not agree. 
								if (Log.WarnEnabled())
								{
									Log.Warn("Peer node " + peer.nodeName + " is different than actual node " + nv.name + " for host " + host);
								}

								if (FindPeerNode(cluster, peers, nv.name))
								{
									// Node already exists. Do not even try to connect to hosts.				
									nv.conn.Close();
									break;
								}
							}

							// Create new node.
							Node node = cluster.CreateNode(nv);
							peers.nodes[nv.name] = node;
							break;
						}
						catch (Exception e)
						{
							if (Log.WarnEnabled())
							{
								Log.Warn("Add node " + host + " failed: " + Util.GetErrorMessage(e));
							}
						}
					}
				}
				peers.refreshCount++;
			}
			catch (Exception e)
			{
				RefreshFailed(e);
			}
		}

		private static bool FindPeerNode(Cluster cluster, Peers peers, string nodeName)
		{
			// Check global node map for existing cluster.
			Node node;
			if (cluster.nodesMap.TryGetValue(nodeName, out node))
			{
				node.referenceCount++;
				return true;
			}

			// Check local node map for this tend iteration.
			if (peers.nodes.TryGetValue(nodeName, out node))
			{
				node.referenceCount++;
				return true;
			}
			return false;
		}

		protected internal void RefreshPartitions(Peers peers)
		{
			// Do not refresh partitions when node connection has already failed during this cluster tend iteration.
			// Also, avoid "split cluster" case where this node thinks it's a 1-node cluster.
			// Unchecked, such a node can dominate the partition map and cause all other
			// nodes to be dropped.
			if (failures > 0 || ! active || (peersCount == 0 && peers.refreshCount > 1))
			{
				return;
			}

			try
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Update partition map for node " + this);
				}
				PartitionParser parser = new PartitionParser(tendConnection, this, cluster.partitionMap, Node.PARTITIONS, cluster.requestProleReplicas);

				if (parser.IsPartitionMapCopied)
				{
					cluster.partitionMap = parser.PartitionMap;
				}
				partitionGeneration = parser.Generation;
			}
			catch (Exception e)
			{
				RefreshFailed(e);
			}
		}

		private void RefreshFailed(Exception e)
		{
			failures++;

			if (!tendConnection.IsClosed())
			{
				tendConnection.Close();
			}

			// Only log message if cluster is still active.
			if (cluster.tendValid && Log.WarnEnabled())
			{
				Log.Warn("Node " + this + " refresh failed: " + Util.GetErrorMessage(e));
			}
		}

		/// <summary>
		/// Get a socket connection from connection pool to the server node.
		/// </summary>
		/// <param name="timeoutMillis">connection timeout value in milliseconds if a new connection is created</param>	
		/// <exception cref="AerospikeException">if a connection could not be provided</exception>
		public Connection GetConnection(int timeoutMillis)
		{
			Connection conn;
			while (connectionQueue.TryTake(out conn))
			{
				if (conn.IsValid())
				{
					try
					{
						conn.SetTimeout(timeoutMillis);
						return conn;
					}
					catch (Exception e)
					{
						// Set timeout failed. Something is probably wrong with timeout
						// value itself, so don't empty queue retrying.  Just get out.
						CloseConnection(conn);
						throw new AerospikeException.Connection(e);
					}
				}
				CloseConnection(conn);
			}

			if (Interlocked.Increment(ref connectionCount) <= cluster.connectionQueueSize)
			{
				try
				{
					conn = cluster.CreateConnection(host.tlsName, address, timeoutMillis);
				}
				catch (Exception)
				{
					Interlocked.Decrement(ref connectionCount);
					throw;
				}

				if (cluster.user != null)
				{
					try
					{
						AdminCommand command = new AdminCommand(ThreadLocalData.GetBuffer(), 0);
						command.Authenticate(conn, cluster.user, cluster.password);
					}
					catch (Exception)
					{
						// Socket not authenticated.  Do not put back into pool.
						CloseConnection(conn);
						throw;
					}
				}
				return conn;
			}
			else
			{
				Interlocked.Decrement(ref connectionCount);
				throw new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS,
					"Node " + this + " max connections " + cluster.connectionQueueSize + " would be exceeded.");
			}
		}

		/// <summary>
		/// Put connection back into connection pool.
		/// </summary>
		/// <param name="conn">socket connection</param>
		public void PutConnection(Connection conn)
		{
			conn.UpdateLastUsed();

			if (!active || !connectionQueue.TryAdd(conn))
			{
				CloseConnection(conn);
			}
		}

		/// <summary>
		/// Close connection and decrement connection count.
		/// </summary>
		public void CloseConnection(Connection conn)
		{
			Interlocked.Decrement(ref connectionCount);
			conn.Close();
		}

		/// <summary>
		/// Return server node IP address and port.
		/// </summary>
		public Host Host
		{
			get
			{
				return host;
			}
		}

		/// <summary>
		/// Return whether node is currently active.
		/// </summary>
		public bool Active
		{
			get
			{
				return active;
			}
		}

		/// <summary>
		/// Return server node name.
		/// </summary>
		public string Name
		{
			get
			{
				return name;
			}
		}

		/// <summary>
		/// Use new batch protocol if server supports it and useBatchDirect is not set.
		/// </summary>
		public bool UseNewBatch(BatchPolicy policy)
		{
			return !policy.useBatchDirect && HasBatchIndex;
		}

		/// <summary>
		/// Does server support batch index protocol.
		/// </summary>
		public bool HasBatchIndex
		{
			get { return (features & HAS_BATCH_INDEX) != 0; }
		}

		/// <summary>
		/// Does server support double particle types.
		/// </summary>
		public bool HasDouble
		{
			get { return (features & HAS_DOUBLE) != 0; }
		}

		/// <summary>
		/// Does server support replicas-all info command.
		/// </summary>
		public bool HasReplicasAll
		{
			get { return (features & HAS_REPLICAS_ALL) != 0; }
		}

		/// <summary>
		/// Does server support peers info command.
		/// </summary>
		public bool HasPeers
		{
			get { return (features & HAS_PEERS) != 0; }
		}
	
		/// <summary>
		/// Return node name and host address in string format.
		/// </summary>
		public override sealed string ToString()
		{
			return name + ' ' + host;
		}

		/// <summary>
		/// Get node name hash code.
		/// </summary>
		public override sealed int GetHashCode()
		{
			return name.GetHashCode();
		}

		/// <summary>
		/// Return if node names are equal.
		/// </summary>
		public override sealed bool Equals(object obj)
		{
			Node other = (Node) obj;
			return this.name.Equals(other.name);
		}

		/// <summary>
		/// Close all server node socket connections.
		/// </summary>
		public void Close()
		{
			active = false;
			CloseConnections();
			GC.SuppressFinalize(this);
		}

		protected internal virtual void CloseConnections()
		{
			// Close tend connection after making reference copy.
			Connection conn = tendConnection;
			conn.Close();

			// Empty connection pool.
			while (connectionQueue.TryTake(out conn))
			{
				conn.Close();
			}
		}
	}
}
