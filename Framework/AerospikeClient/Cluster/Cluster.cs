/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Net;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.IO;

namespace Aerospike.Client
{
	public class Cluster
	{
		// Expected cluster name.
		protected internal readonly String clusterName;

		// Initial host nodes specified by user.
		private volatile Host[] seeds;

		// All host aliases for all nodes in cluster.
		// Only accessed within cluster tend thread.
		protected internal readonly Dictionary<Host, Node> aliases;

		// Map of active nodes in cluster.
		// Only accessed within cluster tend thread.
		protected internal readonly Dictionary<string, Node> nodesMap;
	
		// Active nodes in cluster.
		private volatile Node[] nodes;

		// Hints for best node for a partition
		protected internal volatile Dictionary<string, Partitions> partitionMap;

		// IP translations.
		protected internal readonly Dictionary<string, string> ipMap;

        // TLS connection policy.
		protected internal readonly TlsPolicy tlsPolicy;
            
		// Authentication mode.
		protected internal readonly AuthMode authMode;

		// User name in UTF-8 encoded bytes.
        protected internal readonly byte[] user;

		// Password in UTF-8 encoded bytes.
		protected internal byte[] password;

		// Password in hashed format in bytes.
		protected internal byte[] passwordHash;

		// Random node index.
		private int nodeIndex;

		// Random partition replica index. 
		internal int replicaIndex;

		// Minimum sync connections per node.
		internal readonly int minConnsPerNode;

		// Maximum sync connections per node.
		internal readonly int maxConnsPerNode;

		// Sync connection pools per node. 
		protected internal readonly int connPoolsPerNode;

		// Initial connection timeout.
		protected internal readonly int connectionTimeout;

		// Login timeout.
		protected internal readonly int loginTimeout;

		// Maximum socket idle to validate connections in transactions.
		private readonly double maxSocketIdleMillisTran;

		// Maximum socket idle to trim peak connections to min connections.
		private readonly double maxSocketIdleMillisTrim;
	
		// Rack id.
		public readonly int rackId;

		// Interval in milliseconds between cluster tends.
		private readonly int tendInterval;

		// Cluster tend counter
		private uint tendCount;

		// Tend thread variables.
		private Thread tendThread;
		private CancellationTokenSource cancel;
		private CancellationToken cancelToken;
		internal volatile bool tendValid;

		// Should use "services-alternate" instead of "services" in info request?
		protected internal readonly bool useServicesAlternate;

		// Request server rack ids.
		internal readonly bool rackAware;

		// Does cluster support partition scans.
		internal bool hasPartitionScan;

		private bool statsEnabled;
		private uint reportInterval;
		private volatile LatencyWriter latencyWriter;
		
		public Cluster(ClientPolicy policy, Host[] hosts)
		{
			this.clusterName = policy.clusterName;
			tlsPolicy = policy.tlsPolicy;
			this.authMode = policy.authMode;

			// Default TLS names when TLS enabled.
			if (tlsPolicy != null)
			{
				bool useClusterName = HasClusterName;

				for (int i = 0; i < hosts.Length; i++)
				{
					Host host = hosts[i];

					if (host.tlsName == null)
					{
						string tlsName = useClusterName ? clusterName : host.name;
						hosts[i] = new Host(host.name, tlsName, host.port);
					}
				}
			}
			else
			{
				if (authMode == AuthMode.EXTERNAL)
				{
					throw new AerospikeException("TLS is required for authentication mode: " + authMode);
				}
			}

			this.seeds = hosts;

			if (policy.user != null && policy.user.Length > 0)
			{
				this.user = ByteUtil.StringToUtf8(policy.user);

				// Only store clear text password if external authentication is used.
				if (authMode != AuthMode.INTERNAL)
				{
					this.password = ByteUtil.StringToUtf8(policy.password);
				}

				string pass = policy.password;

				if (pass == null)
				{
					pass = "";
				}

				if (!(pass.Length == 60 && pass.StartsWith("$2a$")))
				{
					pass = AdminCommand.HashPassword(pass);
				}
				this.passwordHash = ByteUtil.StringToUtf8(pass);
			}

			if (policy.maxSocketIdle < 0)
			{
				throw new AerospikeException("Invalid maxSocketIdle: " + policy.maxSocketIdle);
			}

			if (policy.maxSocketIdle == 0)
			{
				maxSocketIdleMillisTran = 0.0;
				maxSocketIdleMillisTrim = 55000.0;
			}
			else
			{
				maxSocketIdleMillisTran = (double)(policy.maxSocketIdle * 1000);
				maxSocketIdleMillisTrim = maxSocketIdleMillisTran;
			}

			minConnsPerNode = policy.minConnsPerNode;
			maxConnsPerNode = policy.maxConnsPerNode;

			if (minConnsPerNode > maxConnsPerNode)
			{
				throw new AerospikeException("Invalid connection range: " + minConnsPerNode + " - " + maxConnsPerNode);
			}

			connPoolsPerNode = policy.connPoolsPerNode;
			connectionTimeout = policy.timeout;
			loginTimeout = policy.loginTimeout;
			tendInterval = policy.tendInterval;
			ipMap = policy.ipMap;
			useServicesAlternate = policy.useServicesAlternate;
			rackAware = policy.rackAware;
			rackId = policy.rackId;

			aliases = new Dictionary<Host, Node>();
			nodesMap = new Dictionary<string, Node>();
			nodes = new Node[0];
			partitionMap = new Dictionary<string, Partitions>();
			cancel = new CancellationTokenSource();
			cancelToken = cancel.Token;

			Log.Warn("ClientPolicy:" +
				" user=" + policy.user +
				" minConnsPerNode=" + policy.minConnsPerNode +
				" maxConnsPerNode=" + policy.maxConnsPerNode +
				" maxSocketIdle=" + policy.maxSocketIdle +
				" rackId=" + policy.rackId
				);

			Log.Warn("WritePolicy Default:" +
				" socketTimeout=" + policy.writePolicyDefault.socketTimeout +
				" totalTimeout=" + policy.writePolicyDefault.totalTimeout +
				" maxRetries=" + policy.writePolicyDefault.maxRetries +
				" commitLevel=" + policy.writePolicyDefault.commitLevel +
				" recordExistsAction=" + policy.writePolicyDefault.recordExistsAction +
				" expiration=" + policy.writePolicyDefault.expiration +
				" sendKey=" + policy.writePolicyDefault.sendKey +
				" durableDelete=" + policy.writePolicyDefault.durableDelete
				);

			Log.Warn("ReadPolicy Default:" + 
				" socketTimeout=" + policy.readPolicyDefault.socketTimeout + 
				" totalTimeout=" + policy.readPolicyDefault.totalTimeout + 
				" maxRetries=" + policy.readPolicyDefault.maxRetries + 
				" readModeAP=" + policy.readPolicyDefault.readModeAP + 
				" readModeSC=" + policy.readPolicyDefault.readModeSC
				);

			Log.Warn("BatchPolicy Default:" +
				" socketTimeout=" + policy.batchPolicyDefault.socketTimeout +
				" totalTimeout=" + policy.batchPolicyDefault.totalTimeout +
				" maxRetries=" + policy.batchPolicyDefault.maxRetries +
				" readModeAP=" + policy.batchPolicyDefault.readModeAP +
				" readModeSC=" + policy.batchPolicyDefault.readModeSC + 
				" maxConcurrentThreads=" + policy.batchPolicyDefault.maxConcurrentThreads + 
				" allowInline=" + policy.batchPolicyDefault.allowInline + 
				" allowProleReads=" + policy.batchPolicyDefault.allowProleReads
				);
		}

		public virtual void InitTendThread(bool failIfNotConnected)
		{
			// Tend cluster until all nodes identified.
			WaitTillStabilized(failIfNotConnected);

			if (Log.DebugEnabled())
			{
				foreach (Host host in seeds)
				{
					Log.Debug("Add seed " + host);
				}
			}

			// Add other nodes as seeds, if they don't already exist.
			List<Host> seedsToAdd = new List<Host>(nodes.Length);
			foreach (Node node in nodes)
			{
				Host host = node.Host;
				if (!FindSeed(host))
				{
					seedsToAdd.Add(host);
				}	
			}

			if (seedsToAdd.Count > 0)
			{
				AddSeeds(seedsToAdd.ToArray());
			}

			// Run cluster tend thread.
			tendValid = true;
			tendThread = new Thread(new ThreadStart(this.Run));
			tendThread.Name = "tend";
			tendThread.IsBackground = true;
			tendThread.Start();
		}

		public void AddSeeds(Host[] hosts)
		{
			// Use copy on write semantics.
			Host[] seedArray = new Host[seeds.Length + hosts.Length];
			int count = 0;

			// Add existing seeds.
			foreach (Host seed in seeds)
			{
				seedArray[count++] = seed;
			}

			// Add new seeds
			foreach (Host host in hosts)
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Add seed " + host);
				}
				seedArray[count++] = host;
			}

			// Replace nodes with copy.
			seeds = seedArray;
		}

		private bool FindSeed(Host search)
		{
			foreach (Host seed in seeds)
			{
				if (seed.Equals(search))
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Tend the cluster until it has stabilized and return control.
		/// This helps avoid initial database request timeout issues when
		/// a large number of threads are initiated at client startup.
		/// 
		/// At least two cluster tends are necessary. The first cluster
		/// tend finds a seed node and obtains the seed's partition maps 
		/// and peer nodes.  The second cluster tend requests partition 
		/// maps from the peer nodes.
		/// 
		/// A third cluster tend is allowed if some peers nodes can't
		/// be contacted.  If peer nodes are still unreachable, an
		/// exception is thrown.
		/// </summary>
		private void WaitTillStabilized(bool failIfNotConnected)
		{
			int count = -1;

			for (int i = 0; i < 3; i++)
			{
				Tend(failIfNotConnected);

				// Check to see if cluster has changed since the last Tend().
				// If not, assume cluster has stabilized and return.
				if (count == nodes.Length)
				{
					return;
				}

				count = nodes.Length;
			}

			string message = "Cluster not stabilized after multiple tend attempts";

			if (failIfNotConnected)
			{
				throw new AerospikeException(message);
			}
			else
			{
				Log.Warn(message);
			}
		}

		public void Run()
		{
			while (tendValid)
			{
				// Tend cluster.
				try
				{
					Tend(false);
				}
				catch (Exception e)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn("Cluster tend failed: " + Util.GetErrorMessage(e));
					}
				}

				if (tendValid)
				{
					// Sleep for tend interval.
					if (cancelToken.WaitHandle.WaitOne(tendInterval))
					{
						// Cancel signal received.
						if (tendValid)
						{
							// Reset cancel token.
							cancel = new CancellationTokenSource();
							cancelToken = cancel.Token;
						}
					}
				}
			}
		}

		/// <summary>
		/// Check health of all nodes in the cluster.
		/// </summary>
		private void Tend(bool failIfNotConnected)
		{
			// Initialize tend iteration node statistics.
			Peers peers = new Peers(nodes.Length + 16);

			// Clear node reference counts.
			foreach (Node node in nodes)
			{
				node.referenceCount = 0;
				node.partitionChanged = false;
				node.rebalanceChanged = false;

				if (!node.HasPeers)
				{
					peers.usePeers = false;
				}
			}

			// All node additions/deletions are performed in tend thread.		
			// If active nodes don't exist, seed cluster.
			if (nodes.Length == 0)
			{
				SeedNode(peers, failIfNotConnected);
			}
			else
			{
				// Refresh all known nodes.
				foreach (Node node in nodes)
				{
					node.Refresh(peers);
				}

				// Refresh peers when necessary.
				if (peers.genChanged)
				{
					// Refresh peers for all nodes that responded the first time even if only one node's peers changed.
					peers.refreshCount = 0;

					foreach (Node node in nodes)
					{
						node.RefreshPeers(peers);
					}
				}
			}

			// Refresh partition map when necessary.
			foreach (Node node in nodes)
			{
				if (node.partitionChanged)
				{
					node.RefreshPartitions(peers);
				}

				if (node.rebalanceChanged)
				{
					node.RefreshRacks();
				}
			}

			if (peers.genChanged || !peers.usePeers)
			{
				// Handle nodes changes determined from refreshes.
				List<Node> removeList = FindNodesToRemove(peers.refreshCount);

				// Remove nodes in a batch.
				if (removeList.Count > 0)
				{
					RemoveNodes(removeList);
				}
			}
	
			// Add nodes in a batch.
			if (peers.nodes.Count > 0)
			{
				AddNodes(peers.nodes);
			}

			uint count = ++tendCount;

			// Balance connections every 30 tend intervals.
			if (count % 30 == 0)
			{
				foreach (Node node in nodes)
				{
					node.BalanceConnections();
				}
			}

			if (statsEnabled && (count % reportInterval) == 0)
			{
				LatencyWriter lw = latencyWriter;
				lw.Write(this);
			}
		}

		public void EnableStats(StatsPolicy policy)
		{
			if (statsEnabled)
			{
				LatencyWriter lw = latencyWriter;
				lw.Close(this);
			}

			latencyWriter = new LatencyWriter(policy);
			reportInterval = policy.reportInterval;
			statsEnabled = true;
		}

		public void DisableStats()
		{
			if (statsEnabled)
			{
				statsEnabled = false;

				LatencyWriter lw = latencyWriter;
				lw.Close(this);
			}
		}

		public bool StatsEnabled
		{
			get { return statsEnabled; }
		}

		public void AddConnLatency(long elapsed)
		{
			LatencyWriter lw = latencyWriter;
			lw.connLatency.Add(elapsed);
		}

		public void AddWriteLatency(long elapsed)
		{
			LatencyWriter lw = latencyWriter;
			lw.writeLatency.Add(elapsed);
		}

		public void AddReadLatency(long elapsed)
		{
			LatencyWriter lw = latencyWriter;
			lw.readLatency.Add(elapsed);
		}

		public void AddBatchLatency(long elapsed)
		{
			LatencyWriter lw = latencyWriter;
			lw.batchLatency.Add(elapsed);
		}

		private bool SeedNode(Peers peers, bool failIfNotConnected)
		{
			// Must copy array reference for copy on write semantics to work.
			Host[] seedArray = seeds;
			Exception[] exceptions = null;
			NodeValidator nv = new NodeValidator();

			for (int i = 0; i < seedArray.Length; i++)
			{
				Host seed = seedArray[i];

				try
				{
					Node node = nv.SeedNode(this, seed, peers);

					if (node != null)
					{
						AddNode(node);
						return true;
					}
				}
				catch (Exception e)
				{
					// Store exception and try next seed.
					if (failIfNotConnected)
					{
						if (exceptions == null)
						{
							exceptions = new Exception[seedArray.Length];
						}
						exceptions[i] = e;
					}
					else
					{
						if (Log.WarnEnabled())
						{
							Log.Warn("Seed " + seed + " failed: " + Util.GetErrorMessage(e));
						}

					}
				}
			}

			// No seeds valid. Use fallback node if it exists.
			if (nv.fallback != null)
			{
				AddNode(nv.fallback);
				return true;
			}

			if (failIfNotConnected)
			{
				StringBuilder sb = new StringBuilder(500);
				sb.AppendLine("Failed to connect to host(s): ");

				for (int i = 0; i < seedArray.Length; i++)
				{
					sb.Append(seedArray[i]);
					sb.Append(' ');

					Exception ex = exceptions[i];

					if (ex != null)
					{
						sb.AppendLine(ex.Message);
					}
				}
				throw new AerospikeException.Connection(sb.ToString());
			}
			return false;
		}

		private void AddNode(Node node)
		{
			node.CreateMinConnections();

			Dictionary<string, Node> nodesToAdd = new Dictionary<string, Node>(1);
			nodesToAdd[node.Name] = node;
			AddNodes(nodesToAdd);
		}

		protected internal virtual Node CreateNode(NodeValidator nv, bool createMinConn)
		{
			Node node = new Node(this, nv);

			if (createMinConn)
			{
				node.CreateMinConnections();
			}
			return node;
		}

		private List<Node> FindNodesToRemove(int refreshCount)
		{
			List<Node> removeList = new List<Node>();

			foreach (Node node in nodes)
			{
				if (!node.Active)
				{
					// Inactive nodes must be removed.
					removeList.Add(node);
					continue;
				}

				if (refreshCount == 0 && node.failures >= 5)
				{
					// All node info requests failed and this node had 5 consecutive failures.
					// Remove node.  If no nodes are left, seeds will be tried in next cluster
					// tend iteration.
					removeList.Add(node);
					continue;
				}

				if (nodes.Length > 1 && refreshCount >= 1 && node.referenceCount == 0)
				{
					// Node is not referenced by other nodes.
					// Check if node responded to info request.
					if (node.failures == 0)
					{
						// Node is alive, but not referenced by other nodes.  Check if mapped.
						if (! FindNodeInPartitionMap(node))
						{
							// Node doesn't have any partitions mapped to it.
							// There is no point in keeping it in the cluster.
							removeList.Add(node);
						}
					}
					else
					{
						// Node not responding. Remove it.
						removeList.Add(node);
					}
				}
			}
			return removeList;
		}

		private bool FindNodeInPartitionMap(Node filter)
		{
			foreach (Partitions partitions in partitionMap.Values)
			{
				foreach (Node[] nodeArray in partitions.replicas)
				{
					foreach (Node node in nodeArray)
					{
						// Use reference equality for performance.
						if (node == filter)
						{
							return true;
						}
					}
				}
			}
			return false;
		}

		/// <summary>
		/// Add nodes using copy on write semantics.
		/// </summary>
		private void AddNodes(Dictionary<string,Node> nodesToAdd)
		{
			// Add all nodes at once to avoid copying entire array multiple times.		
			// Create temporary nodes array.
			Node[] nodeArray = new Node[nodes.Length + nodesToAdd.Count];
			int count = 0;

			// Add existing nodes.
			foreach (Node node in nodes)
			{
				nodeArray[count++] = node;
			}

			// Add new nodes
			foreach (Node node in nodesToAdd.Values)
			{
				if (Log.InfoEnabled())
				{
					Log.Info("Add node " + node);
				}

				nodeArray[count++] = node;
				nodesMap[node.Name] = node;

				// Add node's aliases to global alias set.
				// Aliases are only used in tend thread, so synchronization is not necessary.
				foreach (Host alias in node.aliases)
				{
					aliases[alias] = node;
				}
			}
			hasPartitionScan = Cluster.SupportsPartitionScan(nodeArray);

			// Replace nodes with copy.
			nodes = nodeArray;
		}

		private void RemoveNodes(List<Node> nodesToRemove)
		{
			// There is no need to delete nodes from partitionWriteMap because the nodes 
			// have already been set to inactive. Further connection requests will result 
			// in an exception and a different node will be tried.

			// Cleanup node resources.
			foreach (Node node in nodesToRemove)
			{
				// Remove node from map.
				nodesMap.Remove(node.Name);

				// Remove node's aliases from cluster alias set.
				// Aliases are only used in tend thread, so synchronization is not necessary.
				foreach (Host alias in node.aliases)
				{
					// Log.debug("Remove alias " + alias);
					aliases.Remove(alias);
				}

				if (tendValid)
				{
					node.Close();
				}
				else
				{
					return;
				}
			}

			// Remove all nodes at once to avoid copying entire array multiple times.
			RemoveNodesCopy(nodesToRemove);
		}
	
		/// <summary>
		/// Remove nodes using copy on write semantics.
		/// </summary>
		private void RemoveNodesCopy(List<Node> nodesToRemove)
		{
			// Create temporary nodes array.
			// Since nodes are only marked for deletion using node references in the nodes array,
			// and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
			// in nodesToRemove exist.  Therefore, we know the final array size. 
			Node[] nodeArray = new Node[nodes.Length - nodesToRemove.Count];
			int count = 0;

			// Add nodes that are not in remove list.
			foreach (Node node in nodes)
			{
				if (FindNode(node, nodesToRemove))
				{
					if (tendValid && Log.InfoEnabled())
					{
						Log.Info("Remove node " + node);
					}
				}
				else
				{
					nodeArray[count++] = node;
				}
			}

			// Do sanity check to make sure assumptions are correct.
			if (count < nodeArray.Length)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn("Node remove mismatch. Expected " + nodeArray.Length + " Received " + count);
				}
				// Resize array.
				Node[] nodeArray2 = new Node[count];
				Array.Copy(nodeArray, 0, nodeArray2, 0, count);
				nodeArray = nodeArray2;
			}
			hasPartitionScan = Cluster.SupportsPartitionScan(nodeArray);

			// Replace nodes with copy.
			nodes = nodeArray;
		}

		private static bool FindNode(Node search, List<Node> nodeList)
		{
			foreach (Node node in nodeList)
			{
				if (node.Equals(search))
				{
					return true;
				}
			}
			return false;
		}

		internal bool IsConnCurrentTran(DateTime lastUsed)
		{
			return maxSocketIdleMillisTran == 0.0 || DateTime.UtcNow.Subtract(lastUsed).TotalMilliseconds <= maxSocketIdleMillisTran;
		}

		internal bool IsConnCurrentTrim(DateTime lastUsed)
		{
			return DateTime.UtcNow.Subtract(lastUsed).TotalMilliseconds <= maxSocketIdleMillisTrim;
		}

		public ClusterStats GetStats()
		{
			// Must copy array reference for copy on write semantics to work.
			Node[] nodeArray = nodes;
			NodeStats[] nodeStats = new NodeStats[nodeArray.Length];
			int count = 0;

			foreach (Node node in nodeArray)
			{
				nodeStats[count++] = new NodeStats(node);
			}
			return new ClusterStats(nodeStats);
		}
		
		public bool Connected
		{
			get
			{
				// Must copy array reference for copy on write semantics to work.
				Node[] nodeArray = nodes;

				if (nodeArray.Length > 0 && tendValid)
				{
					// Even though nodes exist, they may not be currently responding.  Check further.
					foreach (Node node in nodeArray)
					{
						// Mark connected if any node is active and cluster tend consecutive info request 
						// failures are less than 5.
						if (node.active && node.failures < 5)
						{
							return true;
						}
					}
				}
				return false; 
			}
		}

		public bool HasClusterName
		{
			get { return clusterName != null && clusterName.Length > 0; }
		}

		public Node GetRandomNode()
		{
			// Must copy array reference for copy on write semantics to work.
			Node[] nodeArray = nodes;
    
			for (int i = 0; i < nodeArray.Length; i++)
			{
				// Must handle concurrency with other non-tending threads, so nodeIndex is consistent.
				int index = Math.Abs(nodeIndex % nodeArray.Length);
				Interlocked.Increment(ref nodeIndex);
				Node node = nodeArray[index];

				if (node.Active)
				{
					return node;
				}
			}
			throw new AerospikeException.InvalidNode("Cluster is empty");
		}

		public Node[] Nodes
		{
			get
			{
				// Must copy array reference for copy on write semantics to work.
				Node[] nodeArray = nodes;
				return nodeArray;
			}
		}

		public Node[] ValidateNodes()
		{
			// Must copy array reference for copy on write semantics to work.
			Node[] nodeArray = nodes;

			if (nodeArray.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Cluster is empty");
			}
			return nodeArray;
		}
		
		public Node GetNode(string nodeName)
		{
			Node node = FindNode(nodeName);

			if (node == null)
			{
				throw new AerospikeException.InvalidNode("Invalid node name: " + nodeName);
			}
			return node;
		}

		private Node FindNode(string nodeName)
		{
			// Must copy array reference for copy on write semantics to work.
			Node[] nodeArray = nodes;

			foreach (Node node in nodeArray)
			{
				if (node.Name.Equals(nodeName))
				{
					return node;
				}
			}
			return null;
		}

		public void PrintPartitionMap()
		{
			foreach (KeyValuePair<String, Partitions> entry in partitionMap)
			{
				String ns = entry.Key;
				Partitions partitions = entry.Value;
				Node[][] replicas = partitions.replicas;

				for (int i = 0; i < replicas.Length; i++)
				{
					Node[] nodeArray = replicas[i];
					int max = nodeArray.Length;

					for (int j = 0; j < max; j++)
					{
						Node node = nodeArray[j];

						if (node != null)
						{
							Log.Info(ns + ',' + i + ',' + j + ',' + node);
						}
					}
				}
			}
		}

		protected internal void ChangePassword(byte[] user, byte[] password, byte[] passwordHash)
		{
			if (this.user != null && Util.ByteArrayEquals(user, this.user))
			{
				this.passwordHash = passwordHash;

				// Only store clear text password if external authentication is used.
				if (authMode != AuthMode.INTERNAL)
				{
					this.password = password;
				}
			}
		}

		private static bool SupportsPartitionScan(Node[] nodes)
		{
			if (nodes.Length == 0)
			{
				return false;
			}

			foreach (Node node in nodes)
			{
				if (! node.HasPartitionScan) {
					return false;
				}
			}
			return true;
		}

		public void InterruptTendSleep()
		{
			// Interrupt tendThread's sleep(), so node refreshes will be performed sooner.
			cancel.Cancel();
		}

		public void Close()
		{
			tendValid = false;
			cancel.Cancel();

			// Must copy array reference for copy on write semantics to work.
			Node[] nodeArray = nodes;

			foreach (Node node in nodeArray)
			{
				node.Close();
			}
		}
	}
}
