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
using Aerospike.Client.Config;
using System.Diagnostics;
using System.Text;

namespace Aerospike.Client
{
	public class Cluster
	{
		private const double MAX_SOCKET_IDLE_TRIM_DEFAULT_SECS = 55000.0;

		/// <summary>
		/// Minimum tend interval in milliseconds.
		/// </summary>
		private const int TEND_INTERVAL_MIN_MS = 250;

		// Pointer to client
		internal protected readonly AerospikeClient client;

		// Config Data
		private IConfigurationData configData;

		// Expected cluster name.
		protected internal readonly String clusterName;

		// Application identifier. May be null.
		protected internal string appId;

		// Initial host nodes specified by user.
		private volatile Host[] seeds;

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

		// Count of connections in shutdown queue. 
		private int recoverCount;

		// Thread-safe queue of sync connections to be closed.
		private Pool<ConnectionRecover> recoverQueue;

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
		protected internal int connectionTimeout;

		// Login timeout.
		protected internal int loginTimeout;

		// Maximum socket idle to validate connections in commands.
		private double maxSocketIdleMillisTran;

		// Maximum socket idle to trim peak connections to min connections.
		private double maxSocketIdleMillisTrim;

		// Rack ids.
		public int[] rackIds;

		// Count of add node failures in the most recent cluster tend iteration.
		private int invalidNodeCount;

		// Interval in milliseconds between cluster tends.
		private int tendInterval;

		private bool failIfNotConnected;

		// Cluster tend counter
		private int tendCount;

		// Milliseconds between dynamic configuration check for file modifications.
		private readonly int configInterval;

		// Dynamic configuration path. If not null, dynamic configuration is enabled.
		private readonly string configPath;

		// Tend thread variables.
		private Thread tendThread;
		private CancellationTokenSource cancel;
		private CancellationToken cancelToken;
		internal volatile bool tendValid;

		// Should use "services-alternate" instead of "services" in info request?
		protected internal bool useServicesAlternate;

		// Request server rack ids.
		internal bool rackAware;

		// Is authentication enabled
		public readonly bool authEnabled;

		// Does cluster support query by partition.
		internal bool hasPartitionQuery;

		public bool MetricsEnabled;
		public MetricsPolicy MetricsPolicy;
		private volatile IMetricsListener metricsListener;
		private readonly List<IMetricsExporter> activeExporters = new();
		private readonly List<IMetricsExporter> ownedExporters = new(); // Exporters created internally (client owns these)
		internal readonly object metricsLock = new();
		private volatile int retryCount;
		private volatile int commandCount;
		private volatile int delayQueueTimeoutCount;
		
		// CPU/Memory tracking for metrics snapshots
		private DateTime prevCpuTime;
		private TimeSpan prevCpuUsage;
		
		// Metrics export thread
		private Thread metricsThread;
		private CancellationTokenSource metricsCancellation;
		private volatile bool metricsThreadRunning;
		
		// Cached base labels for metrics (rebuilt when metrics enabled)
		private KeyValuePair<string, string>[] cachedBaseLabels;
		
		// Cached latency bucket bound strings (e.g., "1ms", "2ms", "4ms", ...)
		private string[] cachedLatencyBucketBounds;
		
		// Cached latency type names (lowercase) indexed by LatencyType enum
		private static readonly string[] LatencyTypeNames = BuildLatencyTypeNames();
		
		// Cached histogram labels per (nodeName, namespace, operationIndex, bucketIndex)
		// Avoids allocating ~50 arrays per namespace per node per export
		private Dictionary<(string nodeName, string ns, int opIndex, int bucketIndex), KeyValuePair<string, string>[]> histogramLabelCache;
		
		private static string[] BuildLatencyTypeNames()
		{
			int max = Latency.GetMax();
			var names = new string[max];
			for (int i = 0; i < max; i++)
			{
				names[i] = Latency.LatencyTypeToString((Latency.LatencyType)i).ToLowerInvariant();
			}
			return names;
		}

		public Cluster(AerospikeClient client, ClientPolicy policy, string configPath, Host[] hosts)
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

			this.client = client;
			this.configPath = configPath;
			this.clusterName = (policy.clusterName != null) ? policy.clusterName : "";
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
				if (!string.IsNullOrEmpty(policy.password))
				{
					throw new AerospikeException(ResultCode.INVALID_CREDENTIAL, "Authentication failed (65): Password authentication is disabled for PKI-only users. Please authenticate using your certificate.");
				}
				this.user = ByteUtil.StringToUtf8(policy.user);
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

			minConnsPerNode = policy.minConnsPerNode;
			maxConnsPerNode = policy.maxConnsPerNode;

			if (minConnsPerNode > maxConnsPerNode)
			{
				throw new AerospikeException("Invalid connection range: " + minConnsPerNode + " - " + maxConnsPerNode);
			}

			connPoolsPerNode = policy.connPoolsPerNode;
			nodesMap = new Dictionary<string, Node>();
			nodes = new Node[0];
			partitionMap = new Dictionary<string, Partitions>();
			recoverCount = 0;
			recoverQueue = new Pool<ConnectionRecover>(256, 10000);
			cancel = new CancellationTokenSource();
			cancelToken = cancel.Token;

			configInterval = client.configProvider != null ? client.configProvider.Interval : IConfigProvider.DEFAULT_CONFIG_INTERVAL;
			
			// Initialize CPU tracking for metrics
			prevCpuTime = DateTime.UtcNow;
			prevCpuUsage = Process.GetCurrentProcess().TotalProcessorTime;
		}

		public void StartTendThread(ClientPolicy policy)
		{
			if (policy.forceSingleNode)
			{
				// Communicate with the first seed node only.
				// Do not run cluster tend thread.
				try
				{
					ForceSingleNode();
				}
				catch (Exception)
				{
					Close();
					throw;
				}
			}
			else
			{
				InitTendThread();
			}
		}

		public void ForceSingleNode()
		{
			// Initialize tendThread, but do not start it.
			tendValid = true;
			tendThread = new Thread(new ThreadStart(this.Run));

			// Validate first seed.
			Host seed = seeds[0];
			NodeValidator nv = new();
			Node node = null;

			try
			{
				node = nv.SeedNode(this, seed, null);
			}
			catch (Exception e)
			{
				throw new AerospikeException("Seed " + seed + " failed: " + e.Message, e);
			}

			node.CreateMinConnections();

			// Add seed node to nodes.
			Dictionary<string, Node> nodesToAdd = new(1);
			nodesToAdd[node.Name] = node;
			AddNodes(nodesToAdd);

			// Initialize partitionMaps.
			Peers peers = new(nodes.Length + 16);
			node.RefreshPartitions(peers);

			// Set partition maps for all namespaces to point to same node.
			foreach (Partitions partitions in partitionMap.Values)
			{
				foreach (Node[] nodeArray in partitions.replicas)
				{
					int max = nodeArray.Length;

					for (int i = 0; i < max; i++)
					{
						nodeArray[i] = node;
					}
				}
			}
		}

		public virtual void InitTendThread()
		{
			// Tend cluster until all nodes identified.
			WaitTillStabilized();

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

			if (configData != null && configData.dynamicConfig.metrics.enable.HasValue &&
				configData.dynamicConfig.metrics.enable.Value)
			{
				lock (metricsLock)
				{
					EnableMetricsInternal(MetricsPolicy);
				}
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
		private void WaitTillStabilized()
		{
			// Tend now requests partition maps in same iteration as the nodes
			// are added, so there is no need to call tend twice anymore.
			Tend(true);

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
					Tend(false);
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
		private void Tend(bool isInit)
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
				SeedNode(peers);

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
					FindNodesToRemove(peers);

					// Remove nodes in a batch.
					if (peers.removeNodes.Count > 0)
					{
						RemoveNodes(peers.removeNodes);
					}
				}

				// Add peer nodes to cluster.
				if (peers.nodes.Count > 0)
				{
					AddNodes(peers.nodes);
					RefreshPeers(peers);
				}
			}

			invalidNodeCount += peers.InvalidCount;

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
					node.ResetErrorRate();
				}
			}

			// Perform legacy metrics snapshot (IMetricsListener only - runs on tend thread)
			lock (metricsLock)
			{
				if (MetricsEnabled && metricsListener != null && (tendCount % MetricsPolicy.Interval) == 0)
				{
					metricsListener.OnSnapshot(this);
				}
			}

			// Convert config interval from a millisecond duration to the number of cluster tend
			// iterations.
			int interval = configInterval / tendInterval;

			// Check configuration file for updates.
			if (configPath != null && tendCount % interval == 0)
			{
				try
				{
					LoadConfiguration();
				}
				catch (Exception ex)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn("Dynamic configuration failed: " + ex.Message);
					}
				}
			}

			ProcessRecoverQueue();
		}

		private bool SeedNode(Peers peers)
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

		private void FindNodesToRemove(Peers peers)
		{
			int refreshCount = peers.refreshCount;
			HashSet<Node> removeNodes = peers.removeNodes;

			foreach (Node node in nodes)
			{
				if (!node.Active)
				{
					// Inactive nodes must be removed.
					removeNodes.Add(node);
					continue;
				}

				if (refreshCount == 0 && node.failures >= 5)
				{
					// All node info requests failed and this node had 5 consecutive failures.
					// Remove node.  If no nodes are left, seeds will be tried in next cluster
					// tend iteration.
					removeNodes.Add(node);
					continue;
				}

				if (nodes.Length > 1 && refreshCount >= 1 && node.referenceCount == 0)
				{
					// Node is not referenced by other nodes.
					// Check if node responded to info request.
					if (node.failures == 0)
					{
						// Node is alive, but not referenced by other nodes.  Check if mapped.
						if (!FindNodeInPartitionMap(node))
						{
							// Node doesn't have any partitions mapped to it.
							// There is no point in keeping it in the cluster.
							removeNodes.Add(node);
						}
					}
					else
					{
						// Node not responding. Remove it.
						removeNodes.Add(node);
					}
				}
			}
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
		private void AddNodes(Dictionary<string, Node> nodesToAdd)
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
		}

		private void RemoveNodes(HashSet<Node> nodesToRemove)
		{
			// There is no need to delete nodes from partitionWriteMap because the nodes 
			// have already been set to inactive. Further connection requests will result 
			// in an exception and a different node will be tried.

			// Cleanup node resources.
			foreach (Node node in nodesToRemove)
			{
				// Remove node from map.
				nodesMap.Remove(node.Name);

				lock (metricsLock)
				{
					if (MetricsEnabled)
					{
						// Clear histogram label cache entries for this node
						InvalidateHistogramCacheForNode(node.Name);
						
						// Flush node metrics before removal via legacy listener.
						try
						{
							metricsListener?.OnNodeClose(node);
						}
						catch (Exception e)
						{
							Log.Warn("Write metrics failed on " + node + ": " + Util.GetErrorMessage(e));
						}
					}
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
		private void RemoveNodesCopy(HashSet<Node> nodesToRemove)
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
				if (nodesToRemove.Contains(node))
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

		public void RecoverConnection(ConnectionRecover cs)
		{
			// Many cloud providers encounter performance problems when sockets are
			// closed by the client when the server still has data left to write.
			// The solution is to shutdown the socket and give the server time to
			// respond before closing the socket.
			//
			// Put connection on a queue for later closing.
			if (cs.IsComplete())
			{
				return;
			}

			// Do not let queue get out of control.
			if (Interlocked.Increment(ref recoverCount) < 10000)
			{
				recoverQueue.EnqueueLast(cs);
			}
			else
			{
				Interlocked.Decrement(ref recoverCount);
				cs.Abort();
			}
		}

		private void ProcessRecoverQueue()
		{
			ConnectionRecover last = recoverQueue.PeekLast();

			if (last == default)
			{
				return;
			}

			// Thread local can be used here because this method
			// is only called from the cluster tend thread.
			byte[] buf = ThreadLocalData.GetBuffer();
			ConnectionRecover cs;

			while (recoverQueue.TryDequeueLast(out cs) && cs != default)
			{
				if (cs.Drain(buf))
				{
					Interlocked.Decrement(ref recoverCount);
				}
				else
				{
					recoverQueue.EnqueueLast(cs);
				}

				if (cs == last)
				{
					break;
				}
			}
		}

		private void LoadConfiguration()
		{
			if (client.configProvider == null)
			{
				var provider = YamlConfigProvider.CreateConfigProvider(configPath, client);

				if (provider == null)
				{
					// Failed to read configuration file. Warning was already logged.
					return;
				}

				client.configProvider = provider;
			}
			else
			{
				try
				{
					if (!client.configProvider.LoadConfig())
					{
						return;
					}
				}
				catch (Exception e)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn(e.Message);
					}
					return;
				}
			}

			configData = client.configProvider.ConfigurationData;
			client.MergeDefaultPoliciesWithConfig();

			lock (metricsLock)
			{
				UpdateClusterConfig(false);
				this.MetricsPolicy = MergeMetricsPolicyWithConfig(MetricsPolicy);

				if (MetricsEnabled && MetricsPolicy.restartRequired)
				{
					DisableMetricsInternal();
					EnableMetricsInternal(MetricsPolicy);
					return;
				}

				if (configData != null && configData.HasMetrics())
				{
					if (!MetricsEnabled && configData.dynamicConfig.metrics.enable.Value)
					{
						EnableMetricsInternal(MetricsPolicy);
					}
					else if (MetricsEnabled && !configData.dynamicConfig.metrics.enable.Value)
					{
						DisableMetricsInternal();
					}
				}
			}
		}

		private MetricsPolicy MergeMetricsPolicyWithConfig(MetricsPolicy metricsPolicy)
		{
			metricsPolicy ??= new MetricsPolicy();
			return new MetricsPolicy(metricsPolicy, configData);
		}

		/// <summary>
		/// Enable metrics collection for the cluster.
		/// </summary>
		/// <param name="policy"></param>
		public void EnableMetrics(MetricsPolicy policy)
		{
			if (configData != null)
			{
				if (configData.dynamicConfig.metrics.enable.HasValue)
				{
					if (!configData.dynamicConfig.metrics.enable.Value)
					{
						Log.Error("When a config exists, metrics can not be enabled via EnableMetrics unless they" +
							" are enabled in the config provider.");
						return;
					}
				}
			}

			lock (metricsLock)
			{
				EnableMetricsInternal(policy);
			}
		}

		/// <summary>
		/// Enable metrics for internal use. The metrics lock needs to be obtained before calling this method.
		/// </summary>
		private void EnableMetricsInternal(MetricsPolicy policy)
		{
			MetricsPolicy mergedMp = MergeMetricsPolicyWithConfig(policy);
			
			#pragma warning disable CS0618 // Type or member is obsolete
			IMetricsListener listener = mergedMp.Listener;
			#pragma warning restore CS0618

			// In case metrics was enabled before this call, disable the previous
			if (MetricsEnabled)
			{
				StopMetricsThread();
				this.metricsListener?.OnDisable(this);
				
				// Only dispose internally-created exporters (user-provided exporters are owned by user)
				DisposeOwnedExporters();
				activeExporters.Clear();
			}

			this.MetricsPolicy = mergedMp;

			Node[] nodeArray = nodes;

			foreach (Node node in nodeArray)
			{
				node.EnableMetrics(MetricsPolicy);
			}

			// Enable legacy listener if present
			if (listener != null)
			{
				this.metricsListener = listener;
				metricsListener.OnEnable(this, MetricsPolicy);
			}
			// If no exporters configured and no legacy listener, use default MetricsWriter as exporter
			else if (mergedMp.Exporters.Count == 0)
			{
				#pragma warning disable CS0618 // Using obsolete ReportDir/ReportSizeLimit for default exporter backward compatibility
				var defaultExporter = new MetricsWriter(
					mergedMp.ReportDir, 
					mergedMp.LatencyColumns, 
					mergedMp.LatencyShift, 
					mergedMp.ReportSizeLimit);
				#pragma warning restore CS0618
				mergedMp.Exporters.Add(defaultExporter);
				ownedExporters.Add(defaultExporter); // Client owns this, will dispose it
			}

			// Add all configured exporters to active list
			foreach (IMetricsExporter exporter in mergedMp.Exporters)
			{
				activeExporters.Add(exporter);
			}

			// Build cached base labels BEFORE enabling metrics (prevents race with metrics thread)
			BuildCachedBaseLabels();
			
			MetricsEnabled = true;
			MetricsPolicy.restartRequired = false;
			
			// Start the metrics export thread if we have exporters
			if (activeExporters.Count > 0)
			{
				StartMetricsThread();
			}
		}

		/// <summary>
		/// Build and cache the base labels and latency bucket bounds that don't change between exports.
		/// </summary>
		private void BuildCachedBaseLabels()
		{
			var labels = new List<KeyValuePair<string, string>>
			{
				new("cluster", clusterName ?? ""),
				new("client_type", "csharp"),
				new("client_version", client.clientVersion)
			};
			
			if (appId != null)
			{
				labels.Add(new("app_id", appId));
			}

			// Add custom labels from policy
			if (MetricsPolicy.labels != null)
			{
				foreach (var kvp in MetricsPolicy.labels)
				{
					labels.Add(new(kvp.Key, kvp.Value));
				}
			}

			cachedBaseLabels = labels.ToArray();
			
			// Build cached latency bucket bound strings based on policy
			int latencyColumns = MetricsPolicy.LatencyColumns;
			int latencyShift = MetricsPolicy.LatencyShift;
			cachedLatencyBucketBounds = new string[latencyColumns];
			
			for (int i = 0; i < latencyColumns; i++)
			{
				cachedLatencyBucketBounds[i] = ComputeLatencyBucketBound(i, latencyShift);
			}
			
			// Initialize histogram label cache
			// Estimate: nodes * namespaces * operations * buckets
			histogramLabelCache = new Dictionary<(string, string, int, int), KeyValuePair<string, string>[]>(1000);
		}
		
		/// <summary>
		/// Remove histogram label cache entries for a specific node.
		/// Called when a node is removed from the cluster.
		/// </summary>
		private void InvalidateHistogramCacheForNode(string nodeName)
		{
			if (histogramLabelCache == null)
			{
				return;
			}
			
			// Find and remove all entries for this node
			var keysToRemove = histogramLabelCache.Keys
				.Where(k => k.nodeName == nodeName)
				.ToList();
			
			foreach (var key in keysToRemove)
			{
				histogramLabelCache.Remove(key);
			}
		}
		
		/// <summary>
		/// Compute the upper bound label for a latency bucket.
		/// Based on LatencyBuckets.GetIndex() logic:
		/// Bucket 0: limit = 1ms, Bucket n: limit = 2^(n * latencyShift) ms
		/// Examples: shift=1 gives 1ms,2ms,4ms,8ms...; shift=3 gives 1ms,8ms,64ms,512ms...
		/// </summary>
		private static string ComputeLatencyBucketBound(int bucketIndex, int latencyShift)
		{
			// Bucket 0 is always <=1ms
			if (bucketIndex == 0)
			{
				return "1ms";
			}
			
			// Bucket n (n > 0): bound = 1 << (n * latencyShift)
			int boundMs = 1 << (bucketIndex * latencyShift);
			
			if (boundMs >= 1000)
			{
				return $"{boundMs / 1000}s";
			}
			return $"{boundMs}ms";
		}

		public void DisableMetrics()
		{
			if (configData != null)
			{
				if (configData.dynamicConfig.metrics.enable.HasValue)
				{
					if (configData.dynamicConfig.metrics.enable.Value)
					{
						Log.Error("Metrics can not be disabled via DisableMetrics() when they are enabled via config.");
						return;
					}
				}
			}

			lock (metricsLock)
			{
				DisableMetricsInternal();
			}
		}

		/// <summary>
		/// Disable metrics for internal use. The metrics lock needs to be obtained before calling this method.
		/// </summary>
		private void DisableMetricsInternal()
		{
			if (MetricsEnabled)
			{
				MetricsEnabled = false;
				StopMetricsThread();
				metricsListener?.OnDisable(this);
				
				// Only dispose internally-created exporters (user-provided exporters are owned by user)
				DisposeOwnedExporters();
				activeExporters.Clear();
				
				// Clear cached labels
				cachedBaseLabels = null;
				cachedLatencyBucketBounds = null;
				histogramLabelCache?.Clear();
				
				foreach (Node node in nodes)
				{
					node.DisableMetrics();
				}
			}
		}

		/// <summary>
		/// Dispose exporters that were created internally by the client.
		/// User-provided exporters are NOT disposed - the user is responsible for their lifecycle.
		/// </summary>
		private void DisposeOwnedExporters()
		{
			foreach (var exporter in ownedExporters)
			{
				if (exporter is IDisposable disposable)
				{
					try
					{
						disposable.Dispose();
					}
					catch (Exception e)
					{
						if (Log.WarnEnabled())
						{
							Log.Warn(context, "Failed to dispose exporter: " + Util.GetErrorMessage(e));
						}
					}
				}
			}
			ownedExporters.Clear();
		}

		/// <summary>
		/// Build a list of metrics for export.
		/// Must be called within metricsLock.
		/// </summary>
		private List<Metric> BuildMetrics()
		{
			// Defensive check - cache should be built before metrics are enabled
			if (cachedBaseLabels == null || cachedLatencyBucketBounds == null)
			{
				if (Log.WarnEnabled())
				{
					Log.Warn(context, "Metrics cache not initialized, skipping export");
				}
				return new List<Metric>();
			}
			
			// Pre-allocate list capacity to reduce resizing
			// Estimate: 9 cluster metrics + nodes * ~100 metrics each (connections + namespace metrics + latency buckets)
			int estimatedCapacity = 9 + nodes.Length * 100;
			var metrics = new List<Metric>(estimatedCapacity);
			var timestamp = DateTime.UtcNow;
			
			GetCpuMemoryUsage(out double cpu, out long memory);
			
			ThreadPool.GetMaxThreads(out int workerThreadsMax, out int completionPortThreadsMax);
			ThreadPool.GetAvailableThreads(out int workerThreads, out int completionPortThreads);
			int asyncThreadsInUse = workerThreadsMax - workerThreads;
			int asyncCompletionPortsInUse = completionPortThreadsMax - completionPortThreads;

			// Use cached base labels (built once when metrics enabled)
			var clusterLabels = cachedBaseLabels;

			// Cluster-level gauges (all share same labels array)
			metrics.Add(new Metric("aerospike.client.cpu_percent", cpu, MetricType.Gauge, timestamp, clusterLabels));
			metrics.Add(new Metric("aerospike.client.memory_bytes", memory, MetricType.Gauge, timestamp, clusterLabels));
			metrics.Add(new Metric("aerospike.client.recover_queue_size", GetRecoverQueueSize(), MetricType.Gauge, timestamp, clusterLabels));
			metrics.Add(new Metric("aerospike.client.async_threads_in_use", asyncThreadsInUse, MetricType.Gauge, timestamp, clusterLabels));
			metrics.Add(new Metric("aerospike.client.async_completion_ports_in_use", asyncCompletionPortsInUse, MetricType.Gauge, timestamp, clusterLabels));

			// Cluster-level counters (all share same labels array)
			metrics.Add(new Metric("aerospike.client.commands_total", GetCommandCount(), MetricType.Counter, timestamp, clusterLabels));
			metrics.Add(new Metric("aerospike.client.retries_total", GetRetryCount(), MetricType.Counter, timestamp, clusterLabels));
			metrics.Add(new Metric("aerospike.client.delay_queue_timeouts_total", GetDelayQueueTimeoutCount(), MetricType.Counter, timestamp, clusterLabels));
			metrics.Add(new Metric("aerospike.client.invalid_nodes_total", InvalidNodeCount, MetricType.Counter, timestamp, clusterLabels));

			// Node-level metrics
			Node[] nodeArray = nodes;
			foreach (Node node in nodeArray)
			{
				BuildNodeMetrics(metrics, node, clusterLabels, timestamp);
			}

			return metrics;
		}

		/// <summary>
		/// Build metrics for a single node.
		/// </summary>
		private void BuildNodeMetrics(List<Metric> metrics, Node node, KeyValuePair<string, string>[] baseLabels, DateTime timestamp)
		{
			// Build node labels array - extends base labels
			var nodeLabels = new KeyValuePair<string, string>[baseLabels.Length + 3];
			Array.Copy(baseLabels, nodeLabels, baseLabels.Length);
			int nodeBaseIndex = baseLabels.Length;
			nodeLabels[nodeBaseIndex] = new("node", node.Name);
			nodeLabels[nodeBaseIndex + 1] = new("node_address", node.Host.name);
			nodeLabels[nodeBaseIndex + 2] = new("node_port", node.Host.port.ToString());

			// Sync connection metrics - reuse same labels array with conn_type appended
			var syncStats = node.GetConnectionStats();
			var syncLabels = AppendLabel(nodeLabels, "conn_type", "sync");
			metrics.Add(new Metric("aerospike.node.connections.in_use", syncStats.inUse, MetricType.Gauge, timestamp, syncLabels));
			metrics.Add(new Metric("aerospike.node.connections.in_pool", syncStats.inPool, MetricType.Gauge, timestamp, syncLabels));
			metrics.Add(new Metric("aerospike.node.connections.opened_total", syncStats.opened, MetricType.Counter, timestamp, syncLabels));
			metrics.Add(new Metric("aerospike.node.connections.closed_total", syncStats.closed, MetricType.Counter, timestamp, syncLabels));

			// Async connection metrics
			if (node is AsyncNode asyncNode)
			{
				var asyncStats = asyncNode.GetAsyncConnectionStats();
				var asyncLabels = AppendLabel(nodeLabels, "conn_type", "async");
				metrics.Add(new Metric("aerospike.node.connections.in_use", asyncStats.inUse, MetricType.Gauge, timestamp, asyncLabels));
				metrics.Add(new Metric("aerospike.node.connections.in_pool", asyncStats.inPool, MetricType.Gauge, timestamp, asyncLabels));
				metrics.Add(new Metric("aerospike.node.connections.opened_total", asyncStats.opened, MetricType.Counter, timestamp, asyncLabels));
				metrics.Add(new Metric("aerospike.node.connections.closed_total", asyncStats.closed, MetricType.Counter, timestamp, asyncLabels));
			}

			// Namespace metrics
			NodeMetrics nodeMetrics = node.GetMetrics();
			if (nodeMetrics?.Histograms != null)
			{
				var histoMap = nodeMetrics.Histograms.histoMap;
				int latencyTypeMax = Latency.GetMax();

				foreach (string ns in histoMap.Keys)
				{
					// Namespace labels - extends node labels
					var nsLabels = AppendLabel(nodeLabels, "namespace", ns);

					// Namespace counters (all share same nsLabels array)
					metrics.Add(new Metric("aerospike.namespace.errors_total", node.GetErrorCountByNS(ns), MetricType.Counter, timestamp, nsLabels));
					metrics.Add(new Metric("aerospike.namespace.timeouts_total", node.GetTimeoutCountbyNS(ns), MetricType.Counter, timestamp, nsLabels));
					metrics.Add(new Metric("aerospike.namespace.key_busy_total", node.GetKeyBusyCountByNS(ns), MetricType.Counter, timestamp, nsLabels));
					metrics.Add(new Metric("aerospike.namespace.bytes_in_total", node.GetBytesInByNS(ns), MetricType.Counter, timestamp, nsLabels));
					metrics.Add(new Metric("aerospike.namespace.bytes_out_total", node.GetBytesOutByNS(ns), MetricType.Counter, timestamp, nsLabels));

					// Latency histograms - use cached labels per (node, namespace, operation, bucket)
					LatencyBuckets[] latencyBuckets = nodeMetrics.Histograms.GetBuckets(ns);
					string nodeName = node.Name;
					
					for (int i = 0; i < latencyTypeMax; i++)
					{
						LatencyBuckets buckets = latencyBuckets[i];
						int bucketMax = buckets.GetMax();

						for (int j = 0; j < bucketMax; j++)
						{
							// Try to get cached labels for this (node, namespace, operation, bucket) tuple
							var cacheKey = (nodeName, ns, i, j);
							if (!histogramLabelCache.TryGetValue(cacheKey, out var histLabels))
							{
								// Build and cache the labels
								histLabels = new KeyValuePair<string, string>[nsLabels.Length + 2];
								nsLabels.CopyTo(histLabels, 0);
								histLabels[nsLabels.Length] = new("operation", LatencyTypeNames[i]);
								histLabels[nsLabels.Length + 1] = new("le", cachedLatencyBucketBounds[j]);
								histogramLabelCache[cacheKey] = histLabels;
							}
							
							metrics.Add(new Metric("aerospike.latency.bucket", buckets.GetBucket(j), MetricType.Histogram, timestamp, histLabels));
						}
					}
				}
			}
		}

		/// <summary>
		/// Create a new labels array with one additional label appended.
		/// </summary>
		private static KeyValuePair<string, string>[] AppendLabel(KeyValuePair<string, string>[] baseLabels, string key, string value)
		{
			var result = new KeyValuePair<string, string>[baseLabels.Length + 1];
			baseLabels.CopyTo(result, 0);
			result[baseLabels.Length] = new(key, value);
			return result;
		}

		/// <summary>
		/// Start the dedicated metrics export thread.
		/// </summary>
		private void StartMetricsThread()
		{
			if (metricsThreadRunning)
			{
				return;
			}

			metricsCancellation = new CancellationTokenSource();
			metricsThreadRunning = true;
			
			metricsThread = new Thread(MetricsThreadRun)
			{
				Name = "aerospike-metrics",
				IsBackground = true
			};
			metricsThread.Start();
		}

		/// <summary>
		/// Stop the dedicated metrics export thread.
		/// </summary>
		private void StopMetricsThread()
		{
			if (!metricsThreadRunning)
			{
				return;
			}

			metricsThreadRunning = false;
			metricsCancellation?.Cancel();

			// Give the thread a chance to exit gracefully
			if (metricsThread != null && metricsThread.IsAlive)
			{
				metricsThread.Join(TimeSpan.FromSeconds(5));
			}

			metricsCancellation?.Dispose();
			metricsCancellation = null;
			metricsThread = null;
		}

		/// <summary>
		/// Metrics export thread main loop.
		/// </summary>
		private void MetricsThreadRun()
		{
			// Calculate interval in milliseconds
			// MetricsPolicy.Interval is in "tend iterations", so multiply by tendInterval
			int intervalMs = MetricsPolicy.Interval * tendInterval;
			
			// Minimum interval of 1 second
			if (intervalMs < 1000)
			{
				intervalMs = 1000;
			}

			if (Log.DebugEnabled())
			{
				Log.Debug(context, $"Metrics export thread started with interval {intervalMs}ms");
			}

			try
			{
				while (metricsThreadRunning && tendValid)
				{
					try
					{
						// Wait for the interval or until cancelled
						if (metricsCancellation.Token.WaitHandle.WaitOne(intervalMs))
						{
							// Cancelled
							break;
						}

						// Build and export metrics
						ExportMetrics();
					}
					catch (Exception e)
					{
						if (Log.WarnEnabled())
						{
							Log.Warn(context, "Metrics export failed: " + Util.GetErrorMessage(e));
						}
					}
				}
			}
			finally
			{
				if (Log.DebugEnabled())
				{
					Log.Debug(context, "Metrics export thread stopped");
				}
			}
		}

		/// <summary>
		/// Build metrics and send to all exporters.
		/// </summary>
		private void ExportMetrics()
		{
			List<IMetricsExporter> exporters;
			List<Metric> metrics;

			lock (metricsLock)
			{
				if (!MetricsEnabled || activeExporters.Count == 0)
				{
					return;
				}

				// Copy exporter list to avoid holding lock during export
				exporters = new List<IMetricsExporter>(activeExporters);
				metrics = BuildMetrics();
			}

			// Export outside the lock to avoid blocking other operations
			// Use async export when available for better scalability
			var asyncExporters = new List<(IAsyncMetricsExporter exporter, Task task)>();
			
			foreach (IMetricsExporter exporter in exporters)
			{
				try
				{
					if (exporter is IAsyncMetricsExporter asyncExporter)
					{
						// Start async export, collect task for later await
						var task = asyncExporter.ExportAsync(metrics, metricsCancellation?.Token ?? CancellationToken.None);
						asyncExporters.Add((asyncExporter, task));
					}
					else
					{
						// Sync export
						exporter.Export(metrics);
					}
				}
				catch (Exception e)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn(context, "Exporter failed: " + Util.GetErrorMessage(e));
					}
				}
			}
			
			// Wait for all async exports to complete
			foreach (var (asyncExporter, task) in asyncExporters)
			{
				try
				{
					task.GetAwaiter().GetResult();
				}
				catch (Exception e)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn(context, "Async exporter failed: " + Util.GetErrorMessage(e));
					}
				}
			}
		}

		/// <summary>
		/// Get CPU and memory usage for metrics.
		/// </summary>
		internal void GetCpuMemoryUsage(out double cpu, out long memory)
		{
			Process currentProcess = Process.GetCurrentProcess();
			memory = currentProcess.WorkingSet64 + currentProcess.VirtualMemorySize64 + currentProcess.PagedMemorySize64;

			var currentTime = DateTime.UtcNow;
			var currentCpuUsage = currentProcess.TotalProcessorTime;

			var cpuUsedMs = (currentCpuUsage - prevCpuUsage).TotalMilliseconds;
			var totalMsPassed = (currentTime - prevCpuTime).TotalMilliseconds;

			if (totalMsPassed > 0)
			{
				cpu = cpuUsedMs / (Environment.ProcessorCount * totalMsPassed) * 100;
			}
			else
			{
				cpu = 0;
			}

			prevCpuTime = currentTime;
			prevCpuUsage = currentCpuUsage;
		}

		protected virtual AerospikeClient GetAerospikeClient()
		{
			return client;
		}

		protected virtual void UpdateClientPolicy()
		{
			var client = GetAerospikeClient();
			client.clientPolicy = new ClientPolicy(client.GetClientPolicy(), client.configProvider);
		}

		/// <summary>
		/// Update dynamic configuration values in the cluster. Since appId is used in metrics, 
		/// the metrics lock must be obtained before calling this method.
		/// </summary>
		/// <exception cref="AerospikeException"></exception>
		protected internal void UpdateClusterConfig(bool init)
		{
			UpdateClientPolicy();
			if (init)
			{
				configData = client?.configProvider?.ConfigurationData ?? null; // Needed to turn on metrics at init
			}
			var clientPolicy = client.GetClientPolicy();
			if (clientPolicy.tendInterval < TEND_INTERVAL_MIN_MS)
			{
				throw new AerospikeException("Invalid tendInterval: " + clientPolicy.tendInterval + ". min: " + TEND_INTERVAL_MIN_MS);
			}

			if (configInterval < clientPolicy.tendInterval)
			{
				throw new AerospikeException("Dynamic config interval " + configInterval +
					" ms must be greater than or equal to tend interval " + clientPolicy.tendInterval);
			}

			appId = clientPolicy.AppId;
			connectionTimeout = clientPolicy.timeout;
			errorRateWindow = clientPolicy.errorRateWindow;
			maxErrorRate = clientPolicy.maxErrorRate;
			failIfNotConnected = clientPolicy.failIfNotConnected;
			loginTimeout = clientPolicy.loginTimeout;
			if (clientPolicy.maxSocketIdle < 0)
			{
				throw new AerospikeException("Invalid maxSocketIdle: " + clientPolicy.maxSocketIdle);
			}

			if (clientPolicy.maxSocketIdle == 0)
			{
				maxSocketIdleMillisTran = 0.0;
				maxSocketIdleMillisTrim = MAX_SOCKET_IDLE_TRIM_DEFAULT_SECS;
			}
			else
			{
				maxSocketIdleMillisTran = (double)(clientPolicy.maxSocketIdle * 1000);
				maxSocketIdleMillisTrim = maxSocketIdleMillisTran;
			}
			rackAware = clientPolicy.rackAware;

			if (init || !RackIdsEqual(clientPolicy.rackIds, this.rackIds))
			{
				if (clientPolicy.rackIds != null && clientPolicy.rackIds.Count > 0)
				{
					rackIds = [.. clientPolicy.rackIds];
				}
				else
				{
					rackIds = [clientPolicy.rackId];
				}

				if (init)
				{
					foreach (Node node in nodes)
					{
						if (rackAware && node.racks == null)
						{
							node.racks = [];
						}
						else if (!rackAware && node.racks != null)
						{
							node.racks = null;
						}
					}
				}
			}
			tendInterval = clientPolicy.tendInterval;
			useServicesAlternate = clientPolicy.useServicesAlternate;
		}

		internal static bool RackIdsEqual(List<int> rackIds1, int[] rackIds2)
		{
			if (rackIds1 == null)
			{
				return rackIds2 == null;
			}
			else if (rackIds2 == null)
			{
				return false;
			}

			if (rackIds1.Count != rackIds2.Length)
			{
				return false;
			}

			for (int i = 0; i < rackIds2.Length; i++)
			{
				int r1 = rackIds1[i];
				int r2 = rackIds2[i];

				if (r1 != r2)
				{
					return false;
				}
			}

			return true;
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
			return new ClusterStats(this, nodeStats);
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
		/// Increment command count when metrics are enabled.
		/// </summary>
		public void AddCommandCount()
		{
			if (MetricsEnabled)
			{
				Interlocked.Increment(ref commandCount);
			}
		}

		/// <summary>
		/// Return command count. The value is cumulative and not reset per metrics interval.
		/// </summary>
		public int GetCommandCount()
		{
			return commandCount;
		}

		/// <summary>
		/// Increment command retry count. There can be multiple retries for a single command.
		/// </summary>
		public void AddRetry()
		{
			Interlocked.Increment(ref retryCount);
		}

		/// <summary>
		/// Add command retry count. There can be multiple retries for a single command.
		/// </summary>
		public void AddRetries(int count)
		{
			Interlocked.Add(ref retryCount, count);
		}

		/// <summary>
		/// Return command retry count. The value is cumulative and not reset per metrics interval.
		/// </summary>
		public int GetRetryCount()
		{
			return retryCount;
		}

		/// <summary>
		/// Increment async delay queue timeout count.
		/// </summary>
		public void AddDelayQueueTimeout()
		{
			Interlocked.Increment(ref delayQueueTimeoutCount);
		}

		/// <summary>
		/// Increment async delay queue timeout count.
		/// </summary>
		public long GetDelayQueueTimeoutCount()
		{
			return delayQueueTimeoutCount;
		}

		/// <summary>
		/// Return connection recoverQueue size. The queue contains connections that have timed out and
		/// need to be drained before returning the connection to a connection pool. The recoverQueue
		/// is only used when <see cref="Policy.TimeoutDelay"/> is true.
		/// <p>
		/// Since recoverQueue is a linked list where the size() calculation is expensive, a separate
		/// counter is used to track recoverQueue.size().
		/// </p>
		/// </summary>
		public int GetRecoverQueueSize()
		{
			return recoverCount;
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

			// Stop metrics thread first (outside of lock to avoid deadlock)
			StopMetricsThread();

			lock (metricsLock)
			{
				try
				{
					if (MetricsEnabled)
					{
						MetricsEnabled = false;
						metricsListener?.OnDisable(this);
						activeExporters.Clear();
						
						foreach (Node node in nodes)
						{
							node.DisableMetrics();
						}
					}
				}
				catch (Exception e)
				{
					Log.Warn("DisableMetrics failed: " + Util.GetErrorMessage(e));
				}
			}

			// Must copy array reference for copy on write semantics to work.
			Node[] nodeArray = nodes;

			foreach (Node node in nodeArray)
			{
				node.Close();
			}
		}
	}
}
