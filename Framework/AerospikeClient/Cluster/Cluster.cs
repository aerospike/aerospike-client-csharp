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
using System.Collections.Generic;
using System.Text;
using System.Threading;

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

		// Log context.
		internal readonly Log.Context context;
            
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

		// Max errors per node per errorRateWindow.
		internal int maxErrorRate;

		// Number of tend iterations defining window for maxErrorRate.
		internal int errorRateWindow;

		// Initial connection timeout.
		protected internal readonly int connectionTimeout;

		// Login timeout.
		protected internal readonly int loginTimeout;

		// Maximum socket idle to validate connections in transactions.
		private readonly double maxSocketIdleMillisTran;

		// Maximum socket idle to trim peak connections to min connections.
		private readonly double maxSocketIdleMillisTrim;

		// Rack ids.
		public readonly int[] rackIds;

		// Count of add node failures in the most recent cluster tend iteration.
		private int invalidNodeCount;

		// Interval in milliseconds between cluster tends.
		private readonly int tendInterval;

		// Cluster tend counter
		private int tendCount;

		// Tend thread variables.
		private Thread tendThread;
		private CancellationTokenSource cancel;
		private CancellationToken cancelToken;
		internal volatile bool tendValid;

		// Should use "services-alternate" instead of "services" in info request?
		protected internal readonly bool useServicesAlternate;

		// Request server rack ids.
		internal readonly bool rackAware;

		// Is authentication enabled
		public readonly bool authEnabled;

		// Does cluster support query by partition.
		internal bool hasPartitionQuery;

		public Cluster(ClientPolicy policy, Host[] hosts)
		{
			// Disable log subscribe requirement to avoid a breaking change in a minor release.
			// TODO: Reintroduce requirement in the next major client release.
			/*
			if (!Log.IsSet())
			{
				throw new AerospikeException("Log.SetCallback() or Log.SetCallbackStandard() must be called." + System.Environment.NewLine +
					"See https://developer.aerospike.com/client/csharp/usage/logging for details.");
			}
			*/

			this.clusterName = (policy.clusterName != null)? policy.clusterName : "";
			this.context = new Log.Context(this.clusterName);

			if (Log.DebugEnabled())
			{
				Log.Debug(context, "Create cluster " + clusterName);
			}

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
				if (authMode == AuthMode.EXTERNAL || authMode == AuthMode.PKI)
				{
					throw new AerospikeException("TLS is required for authentication mode: " + authMode);
				}
			}

			this.seeds = hosts;

			if (policy.authMode == AuthMode.PKI)
			{
				this.authEnabled = true;
				this.user = null;
			}
			else if (policy.user != null && policy.user.Length > 0)
			{
				this.authEnabled = true;
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

				pass = AdminCommand.HashPassword(pass);
				this.passwordHash = ByteUtil.StringToUtf8(pass);
			}
			else
			{
				this.authEnabled = false;
				this.user = null;
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
			maxErrorRate = policy.maxErrorRate;
			errorRateWindow = policy.errorRateWindow;
			connectionTimeout = policy.timeout;
			loginTimeout = policy.loginTimeout;
			tendInterval = policy.tendInterval;
			ipMap = policy.ipMap;
			useServicesAlternate = policy.useServicesAlternate;
			rackAware = policy.rackAware;

			if (policy.rackIds != null && policy.rackIds.Count > 0)
			{
				rackIds = policy.rackIds.ToArray();
			}
			else
			{
				rackIds = new int[] { policy.rackId };
			}

			aliases = new Dictionary<Host, Node>();
			nodesMap = new Dictionary<string, Node>();
			nodes = new Node[0];
			partitionMap = new Dictionary<string, Partitions>();
			cancel = new CancellationTokenSource();
			cancelToken = cancel.Token;
		}

		public virtual void InitTendThread(bool failIfNotConnected)
		{
			// Tend cluster until all nodes identified.
			WaitTillStabilized(failIfNotConnected);

			if (Log.DebugEnabled())
			{
				foreach (Host host in seeds)
				{
					Log.Debug(context, "Add seed " + host);
				}
			}

			// Add other nodes as seeds, if they don't already exist.
			List<Host> seedsToAdd = new List<Host>(nodes.Length);
			foreach (Node node in nodes)
			{
				Host host = node.host;

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
					Log.Debug(context, "Add seed " + host);
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
		/// </summary>
		private void WaitTillStabilized(bool failIfNotConnected)
		{
			// Tend now requests partition maps in same iteration as the nodes
			// are added, so there is no need to call tend twice anymore.
			Tend(failIfNotConnected, true);

			if (nodes.Length == 0)
			{
				string message = "Cluster seed(s) failed";

				if (failIfNotConnected)
				{
					throw new AerospikeException(message);
				}
				else
				{
					Log.Warn(context, message);
				}
			}
		}

		public void Run()
		{
			while (tendValid)
			{
				// Tend cluster.
				try
				{
					Tend(false, false);
				}
				catch (Exception e)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn(context, "Cluster tend failed: " + Util.GetErrorMessage(e));
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
		private void Tend(bool failIfNotConnected, bool isInit)
		{
			// Initialize tend iteration node statistics.
			Peers peers = new Peers(nodes.Length + 16);

			// Clear node reference counts.
			foreach (Node node in nodes)
			{
				node.referenceCount = 0;
				node.partitionChanged = false;
				node.rebalanceChanged = false;
			}

			// All node additions/deletions are performed in tend thread.		
			// If active nodes don't exist, seed cluster.
			if (nodes.Length == 0)
			{
				SeedNode(peers, failIfNotConnected);

				// Abort cluster init if all peers of the seed are not reachable and failIfNotConnected is true.
				if (isInit && failIfNotConnected && nodes.Length == 1 && peers.InvalidCount > 0)
				{
					peers.ClusterInitError();
				}
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

					// Handle nodes changes determined from refreshes.
					List<Node> removeList = FindNodesToRemove(peers.refreshCount);

					// Remove nodes in a batch.
					if (removeList.Count > 0)
					{
						RemoveNodes(removeList);
					}
				}

				// Add peer nodes to cluster.
				if (peers.nodes.Count > 0)
				{
					AddNodes(peers.nodes);
					RefreshPeers(peers);
				}
			}

			invalidNodeCount = peers.InvalidCount;
	
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

			tendCount++;
	
			// Balance connections every 30 tend intervals.
			if (tendCount % 30 == 0)
			{
				foreach (Node node in nodes)
				{
					node.BalanceConnections();
				}
			}

			// Reset connection error window for all nodes every connErrorWindow tend iterations.
			if (maxErrorRate > 0 && tendCount % errorRateWindow == 0)
			{
				foreach (Node node in nodes)
				{
					node.ResetErrorCount();
				}
			}
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
						AddSeedAndPeers(node, peers);
						return true;
					}
				}
				catch (Exception e)
				{
					peers.Fail(seed);

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
							Log.Warn(context, "Seed " + seed + " failed: " + Util.GetErrorMessage(e));
						}

					}
				}
			}

			// No seeds valid. Use fallback node if it exists.
			if (nv.fallback != null)
			{
				// When a fallback is used, peers refreshCount is reset to zero.
				// refreshCount should always be one at this point.
				peers.refreshCount = 1;
				AddSeedAndPeers(nv.fallback, peers);
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

		private void AddSeedAndPeers(Node seed, Peers peers)
		{
			seed.CreateMinConnections();
			nodesMap.Clear();

			AddNodes(seed, peers);

			if (peers.nodes.Count > 0)
			{
				RefreshPeers(peers);
			}
		}

		private void RefreshPeers(Peers peers)
		{
			// Iterate until peers have been refreshed and all new peers added.
			while (true)
			{
				// Copy peer node references to array.
				Node[] nodeArray = new Node[peers.nodes.Count];
				int count = 0;

				foreach (Node node in peers.nodes.Values)
				{
					nodeArray[count++] = node;
				}

				// Reset peer nodes.
				peers.nodes.Clear();

				// Refresh peers of peers in order retrieve the node's peersCount
				// which is used in RefreshPartitions(). This call might add even
				// more peers.
				foreach (Node node in nodeArray)
				{
					node.RefreshPeers(peers);
				}

				if (peers.nodes.Count > 0)
				{
					// Add new peer nodes to cluster.
					AddNodes(peers.nodes);
				}
				else
				{
					break;
				}
			}
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

		private void AddNodes(Node seed, Peers peers)
		{
			// Add all nodes at once to avoid copying entire array multiple times.		
			// Create temporary nodes array.
			Node[] nodeArray = new Node[peers.nodes.Count + 1];
			int count = 0;

			// Add seed.
			nodeArray[count++] = seed;
			AddNode(seed);

			// Add peers.
			foreach (Node peer in peers.nodes.Values)
			{
				nodeArray[count++] = peer;
				AddNode(peer);
			}
			hasPartitionQuery = Cluster.SupportsPartitionQuery(nodeArray);

			// Replace nodes with copy.
			nodes = nodeArray;
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
				nodeArray[count++] = node;
				AddNode(node);
			}
			hasPartitionQuery = Cluster.SupportsPartitionQuery(nodeArray);

			// Replace nodes with copy.
			nodes = nodeArray;
		}

		private void AddNode(Node node)
		{
			if (Log.InfoEnabled())
			{
				Log.Info(context, "Add node " + node);
			}

			nodesMap[node.Name] = node;

			// Add node's aliases to global alias set.
			// Aliases are only used in tend thread, so synchronization is not necessary.
			foreach (Host alias in node.aliases)
			{
				aliases[alias] = node;
			}
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
						Log.Info(context, "Remove node " + node);
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
					Log.Warn(context, "Node remove mismatch. Expected " + nodeArray.Length + " Received " + count);
				}
				// Resize array.
				Node[] nodeArray2 = new Node[count];
				Array.Copy(nodeArray, 0, nodeArray2, 0, count);
				nodeArray = nodeArray2;
			}
			hasPartitionQuery = Cluster.SupportsPartitionQuery(nodeArray);

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

		internal bool UseTls()
		{
			return tlsPolicy != null && !tlsPolicy.forLoginOnly;
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
			return new ClusterStats(nodeStats, invalidNodeCount);
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
							Log.Info(context, ns + ',' + i + ',' + j + ',' + node);
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

		/// <summary>
		/// Set max errors allowed within configurable window for all nodes.
		/// For performance reasons, maxErrorRate is not declared volatile,
		/// so we are relying on cache coherency for other threads to
		/// recognize this change.
		/// </summary>
		public void SetMaxErrorRate(int rate)
		{
			this.maxErrorRate = rate;
		}

		/// <summary>
		/// The number of cluster tend iterations that defines the window for maxErrorRate.
		/// For performance reasons, errorRateWindow is not declared volatile,
		/// so we are relying on cache coherency for other threads to
		/// recognize this change.
		/// </summary>
		public void SetErrorRateWindow(int window)
		{
			this.errorRateWindow = window;
		}

		private static bool SupportsPartitionQuery(Node[] nodes)
		{
			if (nodes.Length == 0)
			{
				return false;
			}

			foreach (Node node in nodes)
			{
				if (!node.HasPartitionQuery)
				{
					return false;
				}
			}
			return true;
		}
		
		/// <summary>
		/// Return count of add node failures in the most recent cluster tend iteration.
		/// </summary>
		public int InvalidNodeCount
		{
			get { return invalidNodeCount; }
		}

		public void InterruptTendSleep()
		{
			// Interrupt tendThread's sleep(), so node refreshes will be performed sooner.
			cancel.Cancel();
		}

		public void Close()
		{
			if (Log.DebugEnabled())
			{
				Log.Debug(context, "Close cluster " + clusterName);
			}

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
