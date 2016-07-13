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

		protected internal readonly Cluster cluster;
		private readonly string name;
		private readonly Host host;
		private Host[] aliases;
		protected internal readonly IPEndPoint address;
		private Connection tendConnection;
		private readonly BlockingCollection<Connection> connectionQueue;
		private int connectionCount;
		protected internal int partitionGeneration = -1;
		protected internal int referenceCount;
		protected internal int failures;
		protected internal readonly bool hasGeo;
		protected internal readonly bool hasDouble;
		protected internal readonly bool hasBatchIndex;
		protected internal readonly bool hasReplicasAll;
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
			this.address = nv.address;
			this.tendConnection = nv.conn;
			this.hasGeo = nv.hasGeo;
			this.hasDouble = nv.hasDouble;
			this.hasBatchIndex = nv.hasBatchIndex;
			this.hasReplicasAll = nv.hasReplicasAll;

			// Assign host to first IP alias because the server identifies nodes 
			// by IP address (not hostname). 
			this.host = aliases[0];

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
		/// <param name="friends">other nodes in the cluster, populated by this method</param>
		/// <exception cref="Exception">if status request fails</exception>
		public void Refresh(List<Host> friends)
		{
			if (tendConnection.IsClosed())
			{
				tendConnection = new Connection(address, cluster.connectionTimeout);
			}

			try
			{
				string[] commands = cluster.useServicesAlternate ? 
					new string[] {"node", "partition-generation", "services-alternate"} : 
					new string[] {"node", "partition-generation", "services"};

				Dictionary<string, string> infoMap = Info.Request(tendConnection, commands);
				VerifyNodeName(infoMap);

				if (AddFriends(infoMap, friends))
				{
					UpdatePartitions(tendConnection, infoMap);
				}
			}
			catch (Exception)
			{
				// Swallow exception if node was closed in another thread.
				if (! tendConnection.IsClosed())
				{
					tendConnection.Close();
					throw;
				}
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
				Log.Info("NAME ERROR");
				active = false;
				throw new AerospikeException("Node name has changed. Old=" + name + " New=" + infoName);
			}
		}

		private bool AddFriends(Dictionary<string, string> infoMap, List<Host> friends)
		{
			// Parse the service addresses and add the friends to the list.
			String command = cluster.useServicesAlternate ? "services-alternate" : "services";
			string friendString = infoMap[command];

			if (friendString == null || friendString.Length == 0)
			{
				// Detect "split cluster" case where this node thinks it's a 1-node cluster.
				// Unchecked, such a node can dominate the partition map and cause all other
				// nodes to be dropped.
				int nodeCount = cluster.Nodes.Length;

				if (nodeCount > 2)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn("Node " + this + " thinks it owns cluster, but client sees " + nodeCount + " nodes.");
					}
					return false;
				}
				return true;
			}

			string[] friendNames = friendString.Split(';');

			foreach (string friend in friendNames)
			{
				string[] friendInfo = friend.Split(':');
				string host = friendInfo[0];
				string alternativeHost;

				if (cluster.ipMap != null && cluster.ipMap.TryGetValue(host, out alternativeHost))
				{
					host = alternativeHost;
				}

				int port = Convert.ToInt32(friendInfo[1]);
				Host alias = new Host(host, port);
				Node node;

				if (cluster.aliases.TryGetValue(alias, out node))
				{
					node.referenceCount++;
				}
				else
				{
					if (!FindAlias(friends, alias))
					{
						friends.Add(alias);
					}
				}
			}
			return true;
		}

		private static bool FindAlias(List<Host> friends, Host alias)
		{
			foreach (Host host in friends)
			{
				if (host.Equals(alias))
				{
					return true;
				}
			}
			return false;
		}

		private void UpdatePartitions(Connection conn, Dictionary<string, string> infoMap)
		{
			string genString = infoMap["partition-generation"];

			if (genString == null || genString.Length == 0)
			{
				throw new AerospikeException.Parse("partition-generation is empty");
			}

			int generation = Convert.ToInt32(genString);

			if (partitionGeneration != generation)
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Node " + this + " partition generation " + generation + " changed.");
				}
				partitionGeneration = cluster.UpdatePartitions(conn, this);
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
					conn = new Connection(address, timeoutMillis, cluster.maxSocketIdleMillis);
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
		/// Return server node IP address aliases.
		/// </summary>
		public Host[] Aliases
		{
			get
			{
				return aliases;
			}
		}

		/// <summary>
		/// Add node alias to list.
		/// </summary>
		public void AddAlias(Host aliasToAdd)
		{
			// Aliases are only referenced in the cluster tend thread,
			// so synchronization is not necessary.
			Host[] tmpAliases = new Host[aliases.Length + 1];
			int count = 0;

			foreach (Host host in aliases)
			{
				tmpAliases[count++] = host;
			}
			tmpAliases[count] = aliasToAdd;
			aliases = tmpAliases;
		}

		/// <summary>
		/// Use new batch protocol if server supports it and useBatchDirect is not set.
		/// </summary>
		public bool UseNewBatch(BatchPolicy policy)
		{
			return !policy.useBatchDirect && hasBatchIndex;
		}

		public bool HasBatchIndex {get{return hasBatchIndex;}}
	
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
