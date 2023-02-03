/* 
 * Copyright 2012-2022 Aerospike, Inc.
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

		public const int HAS_PARTITION_SCAN = (1 << 0);
		public const int HAS_QUERY_SHOW = (1 << 1);
		public const int HAS_BATCH_ANY = (1 << 2);
		public const int HAS_PARTITION_QUERY = (1 << 3);

		private static readonly string[] INFO_PERIODIC = new string[] { "node", "peers-generation", "partition-generation" };
		private static readonly string[] INFO_PERIODIC_REB = new string[] { "node", "peers-generation", "partition-generation", "rebalance-generation" }; 

		protected internal readonly Cluster cluster;
		private readonly string name;
		protected internal readonly Host host;
		protected internal readonly List<Host> aliases;
		protected internal readonly IPEndPoint address;
		private Connection tendConnection;
		private byte[] sessionToken;
		private DateTime? sessionExpiration;
		private volatile Dictionary<string,int> racks;
		private readonly Pool<Connection>[] connectionPools;
		protected uint connectionIter;
		protected internal int connsOpened = 1;
		protected internal int connsClosed;
		private volatile int errorCount;
		protected internal int peersGeneration = -1;
		protected internal int partitionGeneration = -1;
		protected internal int rebalanceGeneration = -1;
		protected internal int peersCount;
		protected internal int referenceCount;
		protected internal int failures;
		protected internal readonly uint features;
		private volatile int performLogin;
		protected internal bool partitionChanged = true;
		protected internal bool rebalanceChanged;
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
			this.tendConnection = nv.primaryConn;
			this.sessionToken = nv.sessionToken;
			this.sessionExpiration = nv.sessionExpiration;
			this.features = nv.features;
			this.rebalanceChanged = cluster.rackAware;
			this.racks = cluster.rackAware ? new Dictionary<string, int>() : null;

			connectionPools = new Pool<Connection>[cluster.connPoolsPerNode];
			int min = cluster.minConnsPerNode / cluster.connPoolsPerNode;
			int remMin = cluster.minConnsPerNode - (min * cluster.connPoolsPerNode);
			int max = cluster.maxConnsPerNode / cluster.connPoolsPerNode;
			int remMax = cluster.maxConnsPerNode - (max * cluster.connPoolsPerNode);

			for (int i = 0; i < connectionPools.Length; i++)
			{
				int minSize = i < remMin ? min + 1 : min;
				int maxSize = i < remMax ? max + 1 : max;

				Pool<Connection> pool = new Pool<Connection>(minSize, maxSize);
				connectionPools[i] = pool;
			}
		}

		~Node()
		{
			// Close connections that slipped through the cracks on race conditions.
			CloseConnections();
		}

		public virtual void CreateMinConnections()
		{
			// Create sync connections.
			foreach (Pool<Connection> pool in connectionPools)
			{
				if (pool.minSize > 0)
				{
					CreateConnections(pool, pool.minSize);
				}
			}
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
					tendConnection = CreateConnection(cluster.connectionTimeout, null);

					if (cluster.authEnabled)
					{
						if (ShouldLogin())
						{
							Login();
						}
						else
						{
							byte[] token = sessionToken;

							if (token != null)
							{
								if (!AdminCommand.Authenticate(cluster, tendConnection, token))
								{
									// Authentication failed. Session token probably expired.
									// Login again to get new session token.
									Login();
								}
							}
						}
					}
				}
				else
				{
					if (cluster.authEnabled && ShouldLogin())
					{
						Login();
					}
				}

				string[] commands = cluster.rackAware ? INFO_PERIODIC_REB : INFO_PERIODIC;
				Dictionary<string, string> infoMap = Info.Request(tendConnection, commands);

				VerifyNodeName(infoMap);
				VerifyPeersGeneration(infoMap, peers);
				VerifyPartitionGeneration(infoMap);

				if (cluster.rackAware)
				{
					VerifyRebalanceGeneration(infoMap);
				}
				peers.refreshCount++;

				// Reload peers, partitions and racks if there were failures on previous tend.
				if (failures > 0)
				{
					peers.genChanged = true;
					partitionChanged = true;
					rebalanceChanged = cluster.rackAware;
				}
				failures = 0;
			}
			catch (Exception e)
			{
				peers.genChanged = true;
				RefreshFailed(e);
			}
		}

		private bool ShouldLogin()
		{
			return performLogin > 0 || (sessionExpiration.HasValue && 
				DateTime.Compare(DateTime.UtcNow, sessionExpiration.Value) >= 0);
		}

		private void Login()
		{
			if (Log.InfoEnabled())
			{
				Log.Info(cluster.context, "Login to " + this);
			}

			try
			{
				AdminCommand admin = new AdminCommand(ThreadLocalData.GetBuffer(), 0);
				byte[] token;
				admin.Login(cluster, tendConnection, out token, out sessionExpiration);
				Volatile.Write(ref sessionToken, token);
				Interlocked.Exchange(ref performLogin, 0);
			}
			catch (Exception)
			{
				Interlocked.Exchange(ref performLogin, 1);
				throw;
			}
		}
	
		public void SignalLogin()
		{
			// Only login when sessionToken is supported
			// and login not already been requested.
			if (Interlocked.CompareExchange(ref performLogin, 1, 0) == 0)
			{
				cluster.InterruptTendSleep();
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

				if (peersGeneration > gen)
				{
					if (Log.InfoEnabled())
					{
						Log.Info(cluster.context, "Quick node restart detected: node=" + this + " oldgen=" + peersGeneration + " newgen=" + gen);
					}
					Restart();
				}
			}
		}

		private void Restart()
		{
			try
			{
				// Reset error rate.
				if (cluster.maxErrorRate > 0)
				{
					ResetErrorCount();
				}

				// Login when user authentication is enabled.
				if (cluster.authEnabled)
				{
					Login();
				}

				// Balance connections.
				BalanceConnections();
			}
			catch (Exception e)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn(cluster.context, "Node restart failed: " + this + ' ' + Util.GetErrorMessage(e));
				}
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

		private void VerifyRebalanceGeneration(Dictionary<string, string> infoMap)
		{
			string genString = infoMap["rebalance-generation"];

			if (genString == null || genString.Length == 0)
			{
				throw new AerospikeException.Parse("rebalance-generation is empty");
			}

			int gen = Convert.ToInt32(genString);

			if (rebalanceGeneration != gen)
			{
				this.rebalanceChanged = true;
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
					Log.Debug(cluster.context, "Update peers for node " + this);
				}

				PeerParser parser = new PeerParser(cluster, tendConnection, peers.peers);
				peersCount = peers.peers.Count;

				bool peersValidated = true;

				foreach (Peer peer in peers.peers)
				{
					if (FindPeerNode(cluster, peers, peer.nodeName))
					{
						// Node already exists. Do not even try to connect to hosts.				
						continue;
					}

					bool nodeValidated = false;

					// Find first host that connects.
					foreach (Host host in peer.hosts)
					{
						if (peers.HasFailed(host))
						{
							continue;
						}

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
									Log.Warn(cluster.context, "Peer node " + peer.nodeName + " is different than actual node " + nv.name + " for host " + host);
								}

								if (FindPeerNode(cluster, peers, nv.name))
								{
									// Node already exists. Do not even try to connect to hosts.				
									nv.primaryConn.Close();
									nodeValidated = true;
									break;
								}
							}

							// Create new node.
							Node node = cluster.CreateNode(nv, true);
							peers.nodes[nv.name] = node;
							nodeValidated = true;
							break;
						}
						catch (Exception e)
						{
							peers.Fail(host);

							if (Log.WarnEnabled())
							{
								Log.Warn(cluster.context, "Add node " + host + " failed: " + Util.GetErrorMessage(e));
							}
						}
					}

					if (! nodeValidated)
					{
						peersValidated = false;
					}
				}

				// Only set new peers generation if all referenced peers are added to the cluster.
				if (peersValidated)
				{
					peersGeneration = parser.generation;
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
					Log.Debug(cluster.context, "Update partition map for node " + this);
				}
				PartitionParser parser = new PartitionParser(tendConnection, this, cluster.partitionMap, Node.PARTITIONS);

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

		protected internal void RefreshRacks()
		{
			// Do not refresh racks when node connection has already failed during this cluster tend iteration.
			if (failures > 0 || !active)
			{
				return;
			}

			try
			{
				if (Log.DebugEnabled())
				{
					Log.Debug(cluster.context, "Update racks for node " + this);
				}
				RackParser parser = new RackParser(tendConnection);

				rebalanceGeneration = parser.Generation;
				racks = parser.Racks;
			}
			catch (Exception e)
			{
				RefreshFailed(e);
			}
		}

		private void RefreshFailed(Exception e)
		{
			failures++;

			if (! tendConnection.IsClosed())
			{
				IncrErrorCount();
				Interlocked.Increment(ref connsClosed);
				tendConnection.Close();
			}

			// Only log message if cluster is still active.
			if (cluster.tendValid && Log.WarnEnabled())
			{
				Log.Warn(cluster.context, "Node " + this + " refresh failed: " + Util.GetErrorMessage(e));
			}
		}

		private void CreateConnections(Pool<Connection> pool, int count)
		{
			// Create sync connections.
			while (count > 0)
			{
				Connection conn;

				try
				{
					conn = CreateConnection(pool);
				}
				catch (Exception e)
				{
					// Failing to create min connections is not considered fatal.
					// Log failure and return.
					if (Log.DebugEnabled())
					{
						Log.Debug(cluster.context, "Failed to create connection: " + e.Message);
					}
					return;
				}

				if (! pool.Enqueue(conn))
				{
					CloseConnection(conn);
					break;
				}
				count--;
			}
		}

		private Connection CreateConnection(Pool<Connection> pool)
		{
			pool.IncrTotal();

			Connection conn;

			try
			{
				conn = CreateConnection(cluster.connectionTimeout, pool);
			}
			catch (Exception)
			{
				pool.DecrTotal();
				throw;
			}

			byte[] token = sessionToken;

			if (token != null)
			{
				try
				{
					if (!AdminCommand.Authenticate(cluster, conn, token))
					{
						Interlocked.Exchange(ref performLogin, 1);
						throw new AerospikeException("Authentication failed");
					}
				}
				catch (Exception)
				{
					// Socket not authenticated.  Do not put back into pool.
					CloseConnectionOnError(conn);
					throw;
				}
			}
			return conn;
		}

		/// <summary>
		/// Get a socket connection from connection pool to the server node.
		/// </summary>
		/// <param name="timeoutMillis">connection timeout value in milliseconds if a new connection is created</param>	
		/// <exception cref="AerospikeException">if a connection could not be provided</exception>
		public Connection GetConnection(int timeoutMillis)
		{
			uint max = (uint)cluster.connPoolsPerNode;
			uint initialIndex;
			bool backward;

			if (max == 1)
			{
				initialIndex = 0;
				backward = false;
			}
			else
			{
				uint iter = connectionIter++; // not atomic by design
				initialIndex = iter % max;
				backward = true;
			}

			Pool<Connection> pool = connectionPools[initialIndex];
			uint queueIndex = initialIndex;
			Connection conn;

			while (true)
			{
				if (pool.TryDequeue(out conn))
				{
					// Found socket.
					// Verify that socket is active and receive buffer is empty.
					if (cluster.IsConnCurrentTran(conn.LastUsed))
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
							CloseConnectionOnError(conn);
							throw new AerospikeException.Connection(e);
						}
					}
					CloseConnection(conn);
				}
				else if (pool.IncrTotal() <= pool.Capacity)
				{
					// Socket not found and queue has available slot.
					// Create new connection.
					try
					{
						conn = CreateConnection(timeoutMillis, pool);
					}
					catch (Exception)
					{
						pool.DecrTotal();
						throw;
					}

					if (cluster.authEnabled)
					{
						byte[] token = SessionToken;

						if (token != null)
						{
							try
							{
								if (!AdminCommand.Authenticate(cluster, conn, token))
								{
									SignalLogin();
									throw new AerospikeException("Authentication failed");
								}
							}
							catch (Exception)
							{
								// Socket not authenticated.  Do not put back into pool.
								CloseConnectionOnError(conn);
								throw;
							}
						}
					}
					return conn;
				}
				else
				{
					// Socket not found and queue is full.  Try another queue.
					pool.DecrTotal();

					if (backward)
					{
						if (queueIndex > 0)
						{
							queueIndex--;
						}
						else
						{
							queueIndex = initialIndex;

							if (++queueIndex >= max)
							{
								break;
							}
							backward = false;
						}
					}
					else if (++queueIndex >= max)
					{
						break;
					}
					pool = connectionPools[queueIndex];
				}
			}
			throw new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS,
				"Node " + this + " max connections " + cluster.maxConnsPerNode + " would be exceeded.");
		}

		private Connection CreateConnection(int timeout, Pool<Connection> pool)
		{
			try
			{
				Connection conn = cluster.UseTls() ?
					new TlsConnection(cluster, host.tlsName, address, timeout, pool) :
					new Connection(address, timeout, pool);

				Interlocked.Increment(ref connsOpened);
				return conn;
			}
			catch (Exception)
			{
				IncrErrorCount();
				throw;
			}
		}

		/// <summary>
		/// Put connection back into connection pool.
		/// </summary>
		/// <param name="conn">socket connection</param>
		public void PutConnection(Connection conn)
		{
			if (active)
			{
				conn.pool.Enqueue(conn);
			}
			else
			{
				CloseConnection(conn);
			}
		}

		/// <summary>
		/// Close pooled connection on error.
		/// </summary>
		public void CloseConnectionOnError(Connection conn)
		{
			IncrErrorCount();
			CloseConnection(conn);
		}

		/// <summary>
		/// Close pooled connection.
		/// </summary>
		public void CloseConnection(Connection conn)
		{
			conn.pool.DecrTotal();
			Interlocked.Increment(ref connsClosed);
			conn.Close();
		}

		public virtual void BalanceConnections()
		{
			foreach (Pool<Connection> pool in connectionPools)
			{
				int excess = pool.Excess();

				if (excess > 0)
				{
					CloseIdleConnections(pool, excess);
				}
				else if (excess < 0 && ErrorCountWithinLimit())
				{
					CreateConnections(pool, -excess);
				}
			}
		}

		private void CloseIdleConnections(Pool<Connection> pool, int count)
		{
			while (count > 0)
			{
				Connection conn;

				if (!pool.TryDequeueLast(out conn))
				{
					break;
				}

				if (cluster.IsConnCurrentTrim(conn.LastUsed))
				{
					if (!pool.EnqueueLast(conn))
					{
						CloseConnection(conn);
					}
					break;
				}
				CloseConnection(conn);
				count--;
			}
		}

		public ConnectionStats GetConnectionStats()
		{
			int inPool = 0;
			int inUse = 0;

			foreach (Pool<Connection> pool in connectionPools)
			{
				int tmp = pool.Count;
				inPool += tmp;
				tmp = pool.Total - tmp;

				// Timing issues may cause values to go negative. Adjust.
				if (tmp < 0)
				{
					tmp = 0;
				}
				inUse += tmp;
			}
			return new ConnectionStats(inPool, inUse, connsOpened, connsClosed);
		}

		public void IncrErrorCount()
		{
			if (cluster.maxErrorRate > 0)
			{
				Interlocked.Increment(ref errorCount);
			}
		}

		public void ResetErrorCount()
		{
			errorCount = 0;
		}

		public bool ErrorCountWithinLimit()
		{
			return cluster.maxErrorRate <= 0 || errorCount <= cluster.maxErrorRate;
		}

		public void ValidateErrorCount()
		{
			if (!ErrorCountWithinLimit())
			{
				throw new AerospikeException.Backoff(ResultCode.MAX_ERROR_RATE);
			}
		}

		/// <summary>
		/// Return if this node has the same rack as the client for the
		/// given namespace.
		/// </summary>
		public bool HasRack(string ns, int rackId)
		{
			// Must copy map reference for copy on write semantics to work.
			Dictionary<string,int> map = this.racks;

			if (map == null)
			{
				return false;
			}

			int r;

			if (! map.TryGetValue(ns, out r))
			{
				return false;
			}

			return r == rackId;
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

		public byte[] SessionToken
		{
			get { return Volatile.Read(ref sessionToken); }
		}

		public bool HasQueryShow
		{
			get { return (features & HAS_QUERY_SHOW) != 0; }
		}

		public bool HasBatchAny
		{
			get { return (features & HAS_BATCH_ANY) != 0; }
		}

		public bool HasPartitionQuery
		{
			get { return (features & HAS_PARTITION_QUERY) != 0; }
		}

		/// <summary>
		/// Return node name, host address and cluster id in string format.
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

			// Empty connection pools.
			foreach (Pool<Connection> pool in connectionPools)
			{
				//Log.Debug("Close node " + this + " connection pool count " + pool.total);
				while (pool.TryDequeue(out conn))
				{
					conn.Close();
				}
			}
		}
		
		/// <summary>
		/// Aerospike cluster which contains this node
		/// </summary>
		public Cluster Cluster
		{
			get
			{
				return cluster;
			}
		}

		/// <summary>
		/// This node's network address
		/// </summary>
		public IPEndPoint NodeAddress
		{
			get
			{
				return address;
			}
		}
	}
}
