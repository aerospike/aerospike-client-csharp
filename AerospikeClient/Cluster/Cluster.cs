/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
using System.Collections.ObjectModel;
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
		private readonly object recoverLock = new();
		private bool recoverAccepting = true;

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
		internal bool MetricsOperationalEnabled =>
			MetricsEnabled && MetricsPolicy?.OperationalEnabled == true;
		private MetricsPolicy metricsPolicyBase = new();
		internal volatile IMetricsListener metricsListener;
		private readonly MetricsManager metricsManager = new();
		private MetricsStore metricsStore
		{
			get => metricsManager.Store;
			set => metricsManager.Store = value;
		}
		internal MetricsStore MetricsStore => metricsStore;
		private MetricsPublication metricsPublication
		{
			get => metricsManager.Publication;
			set => metricsManager.Publication = value;
		}
		internal bool metricsStopping;
		private long metricsTopologyVersion;
		[ThreadStatic]
		private static MetricsCallContext apiMetricsContext;
		[ThreadStatic]
		private static bool apiMetricsDelegation;
		private object metricsLifecycleLock => metricsManager.LifecycleLock;
		internal readonly object metricsLock = new();
		private volatile int retryCount;
		private volatile int commandCount;
		private volatile int delayQueueTimeoutCount;

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
			MetricsPolicy initialMetricsPolicy = new()
			{
				OperationalEnabled = false,
				UsageEnabled = false
			};
			metricsStore = metricsManager.CreateStore(
				initialMetricsPolicy,
				false);
		}

		internal MetricsNodeGeneration RegisterMetricsNode(Node node)
		{
			return metricsStore.RegisterNode(node);
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

			// Always bind and validate the metrics section, even when metrics resolves
			// disabled, so contradictory startup aliases are never silently accepted.
			MetricsPolicy = MergeMetricsPolicyWithConfig(metricsPolicyBase);
			MetricsPolicy.Validate();

			if (configData.GetMetricsEnabled() == true)
			{
				lock (metricsLock)
				{
					metricsManager.EnableConfigured(this, MetricsPolicy);
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
			MetricsStore tendMetricsStore = metricsStore;
			long tendMetricsStart =
				tendMetricsStore?.StartTend() ?? long.MinValue;

			// Initialize tend iteration node statistics.
			Peers peers = new Peers(nodes.Length + 16);
			try
			{

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

			// Perform metrics snapshot
			lock (metricsLock)
			{
				if (MetricsEnabled &&
					!metricsStopping &&
					metricsListener != null &&
					(tendCount % MetricsPolicy.Interval) == 0)
				{
					metricsListener.OnSnapshot(this);
				}
			}

			// Convert config interval from a millisecond duration to the number of cluster tend
			// iterations.
			int interval = Math.Max(1, configInterval / tendInterval);

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
			finally
			{
				if (tendMetricsStart != long.MinValue)
				{
					tendMetricsStore?.CompleteTend(
						tendMetricsStart,
						peers.InvalidCount);
				}
			}
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

			try
			{
				if (createMinConn)
				{
					node.CreateMinConnections();
				}
				return node;
			}
			catch
			{
				node.Close();
				throw;
			}
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

			PublishAddedNodes(nodeArray, nodeArray);
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

			PublishAddedNodes(nodeArray, nodesToAdd.Values);
		}

		private void AddNode(Node node)
		{
			if (Log.InfoEnabled())
			{
				Log.Info(context, "Add node " + node);
			}

			nodesMap[node.Name] = node;
		}

		private void PublishAddedNodes(
			Node[] nodeArray,
			IEnumerable<Node> addedNodes)
		{
			lock (metricsLock)
			{
				List<Node> added = [.. addedNodes];
				NodeMetrics[] staged = MetricsEnabled
					? new NodeMetrics[added.Count]
					: null;

				try
				{
					if (staged != null)
					{
						for (int i = 0; i < added.Count; i++)
						{
							staged[i] = metricsStore.CreateNodeMetrics(
								added[i],
								MetricsPolicy);
						}
					}
				}
				catch
				{
					DisposeNodeMetrics(staged);
					throw;
				}

				Interlocked.Increment(ref metricsTopologyVersion);

				try
				{
					for (int i = 0; i < added.Count; i++)
					{
						metricsStore.AdmitNode(added[i]);
						added[i].ResetMetricsDepartureTransfer();
						added[i].ReplaceMetrics(staged?[i]);
					}
					nodes = nodeArray;

					foreach (Node node in added)
					{
						node.CompleteAdmissionMetrics();
					}
				}
				finally
				{
					Interlocked.Increment(ref metricsTopologyVersion);
				}
			}
		}

		private void RemoveNodes(HashSet<Node> nodesToRemove)
		{
			lock (metricsLifecycleLock)
			{
				RemoveNodesInternal(nodesToRemove);
			}
		}

		private void RemoveNodesInternal(HashSet<Node> nodesToRemove)
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
					if (MetricsEnabled &&
						!metricsStopping &&
						metricsListener != null)
					{
						// Flush node metrics before removal.
						try
						{
							metricsListener.OnNodeClose(node);
						}
						catch (Exception e)
						{
							Log.Warn("Write metrics failed on " + node + ": " + Util.GetErrorMessage(e));
						}
					}
				}

			}

			Node[] nodeArray = CreateNodesAfterRemoval(nodesToRemove);

			lock (metricsLock)
			{
				Interlocked.Increment(ref metricsTopologyVersion);

				try
				{
					foreach (Node node in nodesToRemove)
					{
						TransferNodeDeparture(node);
					}
					nodes = nodeArray;
				}
				finally
				{
					Interlocked.Increment(ref metricsTopologyVersion);
				}
			}
		}

		internal void TransferNodeDeparture(Node node)
		{
			metricsManager.TransferNodeDeparture(this, node);
		}

		/// <summary>
		/// Remove nodes using copy on write semantics.
		/// </summary>
		private Node[] CreateNodesAfterRemoval(HashSet<Node> nodesToRemove)
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
			return nodeArray;
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

			bool admitted = false;

			lock (recoverLock)
			{
				if (recoverAccepting &&
					recoverCount < recoverQueue.Capacity &&
					recoverQueue.EnqueueLast(cs))
				{
					recoverCount++;
					cs.MarkEnqueued();
					admitted = true;
				}
			}

			if (!admitted)
			{
				cs.Abort();
			}
		}

		private void ProcessRecoverQueue()
		{
			List<ConnectionRecover> recoveries;

			lock (recoverLock)
			{
				if (recoverCount == 0)
				{
					return;
				}

				recoveries = new List<ConnectionRecover>(recoverCount);

				while (recoverQueue.TryDequeueLast(out ConnectionRecover cs))
				{
					recoverCount--;
					recoveries.Add(cs);
				}
			}

			// Thread local can be used here because this method
			// is only called from the cluster tend thread.
			byte[] buf = ThreadLocalData.GetBuffer();

			foreach (ConnectionRecover cs in recoveries)
			{
				if (!cs.Drain(buf))
				{
					bool requeued = false;

					lock (recoverLock)
					{
						if (recoverAccepting &&
							recoverQueue.Enqueue(cs))
						{
							recoverCount++;
							requeued = true;
						}
					}

					if (!requeued)
					{
						cs.Abort();
					}
				}
			}
		}

		private void LoadConfiguration()
		{
			lock (metricsLifecycleLock)
			{
				LoadConfigurationInternal();
			}
		}

		private void LoadConfigurationInternal()
		{
			YamlConfigProvider yamlProvider = client.configProvider as YamlConfigProvider;
			YamlConfigProvider.StagedConfig stagedConfig = null;
			IConfigProvider candidateProvider;
			bool publishProvider = false;

			if (client.configProvider == null)
			{
				YamlConfigProvider provider =
					YamlConfigProvider.CreateConfigProvider(configPath, client);

				if (provider == null)
				{
					// Failed to read configuration file. Warning was already logged.
					return;
				}

				yamlProvider = provider;
				candidateProvider = provider;
				publishProvider = true;
			}
			else if (yamlProvider != null)
			{
				try
				{
					if (!yamlProvider.TryStage(out stagedConfig))
					{
						return;
					}
					candidateProvider =
						new YamlConfigProvider.CandidateProvider(stagedConfig);
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
				candidateProvider = client.configProvider;
			}

			IConfigurationData candidateConfig = candidateProvider.ConfigurationData;
			MetricsPolicy candidatePolicy = new MetricsPolicy(metricsPolicyBase, candidateConfig);
			candidatePolicy.Validate();
			ClientPolicy basePolicy =
				client.clientPolicyBase ?? client.GetClientPolicy();
			ClientPolicy candidateClientPolicy =
				new(basePolicy, candidateProvider);
			ValidateClusterConfig(candidateClientPolicy, true);
			AerospikeClient.MergedPolicyBundle candidatePolicies =
				client.CreateMergedPolicies(candidateProvider);

			lock (metricsLock)
			{
				MetricsPolicy previousMetricsPolicy = MetricsPolicy;

				if (stagedConfig != null && !yamlProvider.IsCurrent(stagedConfig))
				{
					return;
				}

				candidatePolicy.restartRequired = MetricsPolicy != null &&
					(candidatePolicy.LatencyColumns != MetricsPolicy.LatencyColumns ||
					candidatePolicy.LatencyShift != MetricsPolicy.LatencyShift ||
					candidatePolicy.LatencyUnit != MetricsPolicy.LatencyUnit);
				bool desiredMetricsEnabled = MetricsEnabled;

				if (candidateConfig != null && candidateConfig.HasMetrics())
				{
					desiredMetricsEnabled =
						candidateConfig.GetMetricsEnabled().Value;
				}
				MetricsManager.DeliveryCandidate deliveryCandidate =
					desiredMetricsEnabled
						? metricsManager.PrepareDelivery(
							this,
							candidatePolicy,
							previousMetricsPolicy)
						: null;
				metricsManager.ApplyConfiguration(
					this,
					candidatePolicy,
					previousMetricsPolicy,
					desiredMetricsEnabled,
					deliveryCandidate,
					candidateClientPolicy.AppId);

				if (stagedConfig != null)
				{
					yamlProvider.CommitApplied(stagedConfig);
				}
				if (publishProvider)
				{
					client.configProvider = candidateProvider;
				}

				client.clientPolicy = candidateClientPolicy;
				client.PublishMergedPolicies(candidatePolicies);
				configData = candidateConfig;
				ApplyClusterConfig(candidateClientPolicy, false);
			}

			if (stagedConfig != null)
			{
				yamlProvider.LogApplied(stagedConfig);
			}
			else if (publishProvider)
			{
				yamlProvider.LogInitialApplied();
			}
		}

		internal MetricsPolicy MergeMetricsPolicyWithConfig(MetricsPolicy metricsPolicy)
		{
			metricsPolicy ??= new MetricsPolicy();
			return new MetricsPolicy(metricsPolicy, configData);
		}

		internal void SetMetricsPolicyBase(MetricsPolicy policy)
		{
			metricsPolicyBase = new MetricsPolicy(
				policy ?? new MetricsPolicy());
		}

		/// <summary>
		/// Enable metrics collection for the cluster.
		/// </summary>
		/// <param name="policy"></param>
		public void EnableMetrics(MetricsPolicy policy)
		{
			metricsManager.Enable(this, policy);
		}

		internal bool AllowExplicitMetricsEnable()
		{
			bool? enabled = configData?.GetMetricsEnabled();

			if (enabled.HasValue && !enabled.Value)
			{
				Log.Error("When a config exists, metrics can not be enabled via EnableMetrics unless they" +
					" are enabled in the config provider.");
				return false;
			}
			return true;
		}

		public void DisableMetrics()
		{
			metricsManager.Disable(this);
		}

		internal bool AllowExplicitMetricsDisable()
		{
			bool? enabled = configData?.GetMetricsEnabled();

			if (enabled.HasValue && enabled.Value)
			{
				Log.Error("Metrics can not be disabled via DisableMetrics() when they are enabled via config.");
				return false;
			}
			return true;
		}

		private static void DisposeNodeMetrics(NodeMetrics[] metrics)
		{
			foreach (NodeMetrics item in metrics)
			{
				item?.Dispose();
			}
		}

		internal void PublishMetrics(
			MetricsStore store,
			MetricsPolicy policy,
			string publicationAppId = null,
			bool overridePublicationAppId = false)
		{
			if (store == null)
			{
				return;
			}

			metricsPublication = CreateMetricsPublication(
				store,
				store.RuntimeConfig,
				policy,
				publicationAppId,
				overridePublicationAppId);
		}

		internal MetricsPublication CreateMetricsPublication(
			MetricsStore store,
			MetricsRuntimeConfig config,
			MetricsPolicy policy,
			string publicationAppId = null,
			bool overridePublicationAppId = false)
		{
			Dictionary<string, string> labels = policy?.labels == null
				? new Dictionary<string, string>()
				: new Dictionary<string, string>(policy.labels);
			string compatibilityAppId = overridePublicationAppId
				? publicationAppId
				: appId;

			if (string.IsNullOrEmpty(compatibilityAppId) && user?.Length > 0)
			{
				compatibilityAppId =
					ByteUtil.Utf8ToString(user, 0, user.Length);
			}
			string metricsAppId = compatibilityAppId;
			if (string.IsNullOrEmpty(metricsAppId))
			{
				metricsAppId = "not-set";
			}

			return new MetricsPublication(
				store,
				config,
				new ReadOnlyDictionary<string, string>(labels),
				metricsAppId,
				compatibilityAppId ?? string.Empty);
		}

		private void RejectMetricsLifecycleFromExporter()
		{
			metricsManager.RejectLifecycleFromExporter();
		}

		protected virtual AerospikeClient GetAerospikeClient()
		{
			return client;
		}

		protected virtual void UpdateClientPolicy()
		{
			var client = GetAerospikeClient();
			ClientPolicy policy = client.clientPolicyBase ?? client.GetClientPolicy();
			client.clientPolicy = new ClientPolicy(policy, client.configProvider);
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
			ClientPolicy clientPolicy = client.GetClientPolicy();
			ValidateClusterConfig(clientPolicy, init);
			ApplyClusterConfig(clientPolicy, init);
		}

		private void ValidateClusterConfig(
			ClientPolicy clientPolicy,
			bool validateConfigInterval)
		{
			if (clientPolicy.tendInterval < TEND_INTERVAL_MIN_MS)
			{
				throw new AerospikeException("Invalid tendInterval: " + clientPolicy.tendInterval + ". min: " + TEND_INTERVAL_MIN_MS);
			}

			if (validateConfigInterval &&
				configInterval < clientPolicy.tendInterval)
			{
				throw new AerospikeException("Dynamic config interval " + configInterval +
					" ms must be greater than or equal to tend interval " + clientPolicy.tendInterval);
			}

			if (clientPolicy.maxSocketIdle < 0)
			{
				throw new AerospikeException("Invalid maxSocketIdle: " + clientPolicy.maxSocketIdle);
			}
		}

		private void ApplyClusterConfig(ClientPolicy clientPolicy, bool init)
		{
			appId = clientPolicy.AppId;
			connectionTimeout = clientPolicy.timeout;
			errorRateWindow = clientPolicy.errorRateWindow;
			maxErrorRate = clientPolicy.maxErrorRate;
			failIfNotConnected = clientPolicy.failIfNotConnected;
			loginTimeout = clientPolicy.loginTimeout;

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
			if (MetricsOperationalEnabled)
			{
				Interlocked.Increment(ref commandCount);
			}
		}

		internal CommandMetricsHandle BeginMetricsCommand(
			MetricsCallContext context,
			string ns,
			IReadOnlyList<string> namespaces,
			Latency.LatencyType latencyType,
			bool countCommand)
		{
			if (context.IsFor(this))
			{
				return context.BeginCommand(
					ns,
					namespaces,
					latencyType,
					countCommand);
			}

			MetricsStore store = metricsStore;
			return !MetricsOperationalEnabled
				? new CommandMetricsHandle(
					new CommandMetricsState(
						store,
						ns,
						namespaces,
						latencyType,
						false,
						MetricsAttemptMode.Operational))
				: store.BeginCommand(
					ns,
					namespaces,
					latencyType,
					countCommand);
		}

		internal CommandMetricsHandle BeginMetricsCommand(
			MetricsCallContext context,
			string ns,
			Latency.LatencyType latencyType,
			bool countCommand)
		{
			return BeginMetricsCommand(
				context,
				ns,
				null,
				latencyType,
				countCommand);
		}

		internal QueryMetricsHandle BeginQueryMetrics(MetricsCallContext context)
		{
			if (context.IsFor(this))
			{
				return context.BeginQueryMetrics();
			}

			MetricsStore store = metricsStore;
			return store?.BeginQueryMetrics() ?? default;
		}

		internal CommandCountMetricsHandle AddMetricsCommandCount()
		{
			if (apiMetricsContext.IsFor(this))
			{
				return apiMetricsContext.CommandCountMetrics;
			}

			return metricsStore?.BeginCountOnly() ?? default;
		}

		internal void RecordApiUsage(ApiExecutionMode mode)
		{
			if (!apiMetricsContext.IsFor(this))
			{
				metricsStore?.AddUsage(mode);
			}
		}

		internal MetricsCallContext CaptureApiMetricsContext()
		{
			return apiMetricsContext.IsFor(this) ? apiMetricsContext : default;
		}

		internal MetricsRpcAttempt BeginCompatibilityAttempt(
			MetricsCallContext context)
		{
			return context.IsFor(this)
				? context.BeginCompatibilityAttempt()
				: null;
		}

		internal ApiMetricsCall BeginPointApiMetrics(
			ApiMetricsOperation operation,
			string ns,
			ApiExecutionMode mode)
		{
			if (apiMetricsDelegation && apiMetricsContext.IsFor(this))
			{
				return default;
			}

			MetricsCallContext previous = apiMetricsContext;
			bool previousDelegation = apiMetricsDelegation;
			MetricsCallContext owned =
				metricsManager.BeginCall(this, operation, mode);
			apiMetricsContext = owned;
			apiMetricsDelegation = false;
			return new ApiMetricsCall(this, previous, previousDelegation, owned);
		}

		internal ApiMetricsCall BeginPointApiMetrics(
			string ns,
			Latency.LatencyType latencyType,
			ApiExecutionMode mode)
		{
			ApiMetricsOperation operation = latencyType == Latency.LatencyType.WRITE
				? ApiMetricsOperation.Write
				: ApiMetricsOperation.Read;
			return BeginPointApiMetrics(operation, ns, mode);
		}

		internal ApiMetricsCall BeginCoordinatorApiMetrics(
			ApiMetricsOperation operation,
			ApiExecutionMode mode)
		{
			if (apiMetricsDelegation && apiMetricsContext.IsFor(this))
			{
				return default;
			}

			MetricsCallContext previous = apiMetricsContext;
			bool previousDelegation = apiMetricsDelegation;
			MetricsCallContext owned =
				metricsManager.BeginCall(this, operation, mode);
			apiMetricsContext = owned;
			apiMetricsDelegation = false;
			return new ApiMetricsCall(this, previous, previousDelegation, owned);
		}

		internal ApiMetricsCall BeginCoordinatorApiMetrics(ApiExecutionMode mode)
		{
			return BeginCoordinatorApiMetrics(ApiMetricsOperation.Batch, mode);
		}

		internal void RecordEmptyBatchMetrics(ApiExecutionMode mode)
		{
			if (apiMetricsContext.IsFor(this))
			{
				apiMetricsContext.CompleteSuccess();
			}
		}

		internal ApiMetricsCall BeginApiMetricsDelegation()
		{
			MetricsCallContext previous = apiMetricsContext;
			bool previousDelegation = apiMetricsDelegation;
			apiMetricsDelegation = true;
			return new ApiMetricsCall(
				this,
				previous,
				previousDelegation,
				default);
		}

		internal void EndApiMetricsCall(
			MetricsCallContext previous,
			bool previousDelegation,
			MetricsCallContext owned)
		{
			owned.CompleteScope();
			apiMetricsContext = previous;
			apiMetricsDelegation = previousDelegation;
		}

		/// <summary>
		/// Return an immutable metrics snapshot. This method is available before metrics are
		/// enabled, while collection is active, and after collection is disabled.
		/// </summary>
		public MetricsSnapshot GetMetricsSnapshot()
		{
			return metricsManager.BuildSnapshot(this);
		}

		internal MetricsStoreSnapshot ReadMetrics(
			MetricsProcessSampler processSampler = null)
		{
			lock (metricsLock)
			{
				return metricsManager.ReadStoreSnapshot(
					this,
					processSampler);
			}
		}

		internal ConnectionMetricsHandle StartConnectionMetrics(
			Node node = null)
		{
			MetricsStore store = metricsStore;

			if (store != null &&
				store.TryStartConnection(
					out long startTimestamp,
					out MetricsRuntimeConfig runtimeConfig))
			{
				return new ConnectionMetricsHandle(
					store,
					runtimeConfig,
					startTimestamp,
					node == null
						? ConnectionMetricsOrigin.Validation
						: ConnectionMetricsOrigin.NodePool,
					node);
			}
			return default;
		}

		internal void RecordConnectionClosed(Node node)
		{
			MetricsStore store = metricsStore;

			if (store?.AcceptsWrites == true)
			{
				store.AddConnectionClosed(node);
			}
		}

		internal void RecordPoolExhausted(Node node)
		{
			MetricsStore store = metricsStore;

			if (store?.AcceptsWrites == true)
			{
				store.AddPoolExhausted(node);
			}
		}

		internal void RecordConnectionFailure(
			Node node,
			ConnectionFailurePhase phase,
			bool opening)
		{
			MetricsStore store = metricsStore;

			if (store?.AcceptsWrites != true)
			{
				return;
			}

			using MetricsWritePermit permit = store.AcquireWrite();

			if (permit.Acquired)
			{
				if (node == null)
				{
					store.AddConnectionFailureAdmitted(phase, opening);
				}
				else
				{
					store.AddNodeConnectionFailureAdmitted(
						node,
						phase,
						opening);
				}
			}
		}

		internal RecoverMetricsHandle BeginRecoverMetrics()
		{
			MetricsStore store = metricsStore;
			return store?.TryStartRecover() == true
				? new RecoverMetricsHandle(store)
				: default;
		}

		internal void RecordImmediateTimeout()
		{
			metricsStore?.AddImmediateTimeout();
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
			metricsStore?.AddDelayQueueTimeout();
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
			return Volatile.Read(ref recoverCount);
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
			RejectMetricsLifecycleFromExporter();

			if (Log.DebugEnabled())
			{
				Log.Debug(context, "Close cluster " + clusterName);
			}

			tendValid = false;
			cancel.Cancel();

			// Stop admission and take ownership of every queued recovery before
			// aborting outside the gate.
			List<ConnectionRecover> recoveries = new();

			lock (recoverLock)
			{
				recoverAccepting = false;

				while (recoverQueue.TryDequeueLast(out ConnectionRecover recover) &&
					recover != null)
				{
					recoveries.Add(recover);
				}
				recoverCount = 0;
			}

			foreach (ConnectionRecover recover in recoveries)
			{
				recover.Abort();
			}

			if (tendThread != null &&
				tendThread != Thread.CurrentThread &&
				tendThread.IsAlive)
			{
				tendThread.Join();
			}

			lock (metricsLifecycleLock)
			{
				lock (metricsLock)
				{
					try
					{
						Node[] currentNodes = nodes;
						foreach (Node node in currentNodes)
						{
							if (!node.Active)
							{
								TransferNodeDeparture(node);
							}
						}

						if (MetricsEnabled)
						{
							metricsManager.DisableForClose(this);
						}
					}
					catch (Exception e)
					{
						Log.Warn("DisableMetrics failed: " + Util.GetErrorMessage(e));
						metricsManager.FailClosed(this, metricsStore);
					}

					metricsManager.StopDelivery(this);
					metricsListener = null;
					metricsPolicyBase = new MetricsPolicy();
					MetricsPolicy = new MetricsPolicy();
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
