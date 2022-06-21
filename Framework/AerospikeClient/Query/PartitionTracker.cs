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
	public sealed class PartitionTracker
	{
		private readonly PartitionStatus[] partitions;
		private readonly int partitionsCapacity;
		private readonly int partitionBegin;
		private readonly int nodeCapacity;
		private readonly Node nodeFilter;
		private readonly PartitionFilter partitionFilter;
		private List<NodePartitions> nodePartitionsList;
		private List<AerospikeException> exceptions;
		private long maxRecords;
		private int sleepBetweenRetries;
		public int socketTimeout;
		public int totalTimeout;
		public int iteration = 1;
		private DateTime deadline;

		public PartitionTracker(ScanPolicy policy, Node[] nodes)
			: this((Policy)policy, nodes)
		{
			SetMaxRecords(policy.maxRecords);
		}

		public PartitionTracker(QueryPolicy policy, Statement stmt, Node[] nodes)
			: this((Policy)policy, nodes)
		{
			SetMaxRecords(policy, stmt);
		}

		private PartitionTracker(Policy policy, Node[] nodes)
		{
			this.partitionBegin = 0;
			this.nodeCapacity = nodes.Length;
			this.nodeFilter = null;
			this.partitionFilter = null;

			// Create initial partition capacity for each node as average + 25%.
			int ppn = Node.PARTITIONS / nodes.Length;
			ppn += (int)((uint)ppn >> 2);
			this.partitionsCapacity = ppn;
			this.partitions = InitPartitions(Node.PARTITIONS, null);
			InitTimeout(policy);
		}

		public PartitionTracker(ScanPolicy policy, Node nodeFilter)
			: this((Policy)policy, nodeFilter)
		{
			SetMaxRecords(policy.maxRecords);
		}

		public PartitionTracker(QueryPolicy policy, Statement stmt, Node nodeFilter)
			: this((Policy)policy, nodeFilter)
		{
			SetMaxRecords(policy, stmt);
		}

		private PartitionTracker(Policy policy, Node nodeFilter)
		{
			this.partitionBegin = 0;
			this.nodeCapacity = 1;
			this.nodeFilter = nodeFilter;
			this.partitionFilter = null;
			this.partitionsCapacity = Node.PARTITIONS;
			this.partitions = InitPartitions(Node.PARTITIONS, null);
			InitTimeout(policy);
		}

		public PartitionTracker(ScanPolicy policy, Node[] nodes, PartitionFilter filter)
			: this((Policy)policy, nodes, filter, policy.maxRecords)
		{
		}

#pragma warning disable 0618
		public PartitionTracker(QueryPolicy policy, Statement stmt, Node[] nodes, PartitionFilter filter)
			: this((Policy)policy, nodes, filter, (stmt.maxRecords > 0) ? stmt.maxRecords : policy.maxRecords)
		{
		}
#pragma warning restore 0618

		private PartitionTracker(Policy policy, Node[] nodes, PartitionFilter filter, long maxRecords)
		{
			// Validate here instead of initial PartitionFilter constructor because total number of
			// cluster partitions may change on the server and PartitionFilter will never have access
			// to Cluster instance.  Use fixed number of partitions for now.
			if (!(filter.begin >= 0 && filter.begin < Node.PARTITIONS))
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid partition begin " + filter.begin +
					". Valid range: 0-" + (Node.PARTITIONS - 1));
			}

			if (filter.count <= 0)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid partition count " + filter.count);
			}

			if (filter.begin + filter.count > Node.PARTITIONS)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid partition range (" + filter.begin +
					',' + filter.count + ')');
			}

			SetMaxRecords(maxRecords);
			this.partitionBegin = filter.begin;
			this.nodeCapacity = nodes.Length;
			this.nodeFilter = null;
			this.partitionsCapacity = filter.count;

			if (filter.partitions == null)
			{
				filter.partitions = InitPartitions(filter.count, filter.digest);
				filter.retry = true;
			}
			else
			{
				// Retry all partitions when maxRecords not specified.
				if (maxRecords == 0)
				{
					filter.retry = true;
				}
			}
			this.partitions = filter.partitions;
			this.partitionFilter = filter;
			InitTimeout(policy);
		}

		private void SetMaxRecords(QueryPolicy policy, Statement stmt)
		{
#pragma warning disable 0618
			SetMaxRecords((stmt.maxRecords > 0) ? stmt.maxRecords : policy.maxRecords);
#pragma warning restore 0618
		}

		private void SetMaxRecords(long maxRecords)
		{
			if (maxRecords < 0)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid maxRecords: " + maxRecords);
			}
			this.maxRecords = maxRecords;
		}

		private PartitionStatus[] InitPartitions(int partitionCount, byte[] digest)
		{
			PartitionStatus[] partsAll = new PartitionStatus[partitionCount];

			for (int i = 0; i < partitionCount; i++)
			{
				partsAll[i] = new PartitionStatus(partitionBegin + i);
			}

			if (digest != null)
			{
				partsAll[0].digest = digest;
			}
			return partsAll;
		}

		private void InitTimeout(Policy policy)
		{
			sleepBetweenRetries = policy.sleepBetweenRetries;
			socketTimeout = policy.socketTimeout;
			totalTimeout = policy.totalTimeout;

			if (totalTimeout > 0)
			{
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);

				if (socketTimeout == 0 || socketTimeout > totalTimeout)
				{
					socketTimeout = totalTimeout;
				}
			}
		}

		public int SleepBetweenRetries
		{
			set { this.sleepBetweenRetries = value; }
		}

		public List<NodePartitions> AssignPartitionsToNodes(Cluster cluster, string ns)
		{
			//Log.Info("Round " + iteration);
			List<NodePartitions> list = new List<NodePartitions>(nodeCapacity);

			Dictionary<string, Partitions> map = cluster.partitionMap;
			Partitions parts;

			if (!map.TryGetValue(ns, out parts))
			{
				throw new AerospikeException.InvalidNamespace(ns, map.Count);
			}

			Node[] master = parts.replicas[0];
			bool retry = (partitionFilter == null || partitionFilter.retry) && iteration == 1;

			foreach (PartitionStatus part in partitions)
			{
				if (retry || part.retry)
				{
					Node node = Volatile.Read(ref master[part.id]);

					if (node == null)
					{
						throw new AerospikeException.InvalidNode(part.id);
					}

					part.retry = false;

					// Use node name to check for single node equality because
					// partition map may be in transitional state between
					// the old and new node with the same name.
					if (nodeFilter != null && !nodeFilter.Name.Equals(node.Name))
					{
						continue;
					}

					NodePartitions np = FindNode(list, node);

					if (np == null)
					{
						// If the partition map is in a transitional state, multiple
						// NodePartitions instances (each with different partitions)
						// may be created for a single node.
						np = new NodePartitions(node, partitionsCapacity);
						list.Add(np);
					}
					np.AddPartition(part);
				}
			}

			int nodeSize = list.Count;

			if (nodeSize <= 0) {
				throw new AerospikeException.InvalidNode("No nodes were assigned");
			}

			// Set global retry to true because scan/query may terminate early and all partitions
			// will need to be retried if the PartitionFilter instance is reused in a new scan/query.
			// Global retry will be set to false if the scan/query completes normally and maxRecords
			// is specified.
			if (partitionFilter != null)
			{
				partitionFilter.retry = true;
			}

			if (maxRecords > 0)
			{
				// Distribute maxRecords across nodes.
				if (maxRecords < nodeSize)
				{
					// Only include nodes that have at least 1 record requested.
					nodeSize = (int)maxRecords;
					list = list.GetRange(0, nodeSize);
				}

				long max = maxRecords / nodeSize;
				int rem = (int)(maxRecords - (max * nodeSize));

				for (int i = 0; i < nodeSize; i++)
				{
					NodePartitions np = list[i];
					np.recordMax = i < rem ? max + 1 : max;
				}
			}
			nodePartitionsList = list;
			return list;
		}

		private NodePartitions FindNode(List<NodePartitions> list, Node node)
		{
			foreach (NodePartitions nodePartition in list)
			{
				// Use pointer equality for performance.
				if (nodePartition.node == node)
				{
					return nodePartition;
				}
			}
			return null;
		}

		public void PartitionUnavailable(NodePartitions nodePartitions, int partitionId)
		{
			partitions[partitionId - partitionBegin].retry = true;
			nodePartitions.partsUnavailable++;
		}

		public void SetDigest(NodePartitions nodePartitions, Key key)
		{
			uint partitionId = Partition.GetPartitionId(key.digest);
			partitions[partitionId - partitionBegin].digest = key.digest;
			nodePartitions.recordCount++;
		}

		public void SetLast(NodePartitions nodePartitions, Key key, ulong bval)
		{
			uint partitionId = Partition.GetPartitionId(key.digest);
			PartitionStatus ps = partitions[partitionId - partitionBegin];
			ps.digest = key.digest;
			ps.bval = bval;
			nodePartitions.recordCount++;
		}

		public bool IsComplete(Cluster cluster, Policy policy)
		{
			long recordCount = 0;
			int partsUnavailable = 0;

			foreach (NodePartitions np in nodePartitionsList)
			{
				recordCount += np.recordCount;
				partsUnavailable += np.partsUnavailable;

				//Log.Info("Node " + np.node + " partsFull=" + np.partsFull.Count + 
				//  " partsPartial=" + np.partsPartial.Count + " partsUnavailable=" + np.partsUnavailable +
				//  " recordsRequested=" + np.recordMax + " recordsReceived=" + np.recordCount);
			}

			if (partsUnavailable == 0)
			{
				if (maxRecords == 0)
				{
					if (partitionFilter != null)
					{
						partitionFilter.done = true;
					}
				}
				else if (iteration > 1)
				{
					if (partitionFilter != null)
					{
						// If errors occurred on a node, only that node's partitions are retried in the
						// next iteration. If that node finally succeeds, the other original nodes still
						// need to be retried if partition state is reused in the next scan/query command.
						// Force retry on all node partitions.
						partitionFilter.retry = true;
						partitionFilter.done = false;
					}
				}
				else
				{
					if (cluster.hasPartitionQuery)
					{
						// Server version >= 6.0 will return all records for each node up to
						// that node's max. If node's record count reached max, there still
						// may be records available for that node.
						bool done = true;

						foreach (NodePartitions np in nodePartitionsList)
						{
							if (np.recordCount >= np.recordMax)
							{
								MarkRetry(np);
								done = false;
							}
						}

						if (partitionFilter != null)
						{
							// Set global retry to false because only specific node partitions
							// should be retried.
							partitionFilter.retry = false;
							partitionFilter.done = done;
						}
					}
					else
					{
						// Servers version < 6.0 can return less records than max and still
						// have more records for each node, so the node is only done if no
						// records were retrieved for that node.
						foreach (NodePartitions np in nodePartitionsList)
						{
							if (np.recordCount > 0)
							{
								MarkRetry(np);
							}
						}

						if (partitionFilter != null)
						{
							// Set global retry to false because only specific node partitions
							// should be retried.
							partitionFilter.retry = false;
							partitionFilter.done = recordCount == 0;
						}
					}
				}
				return true;
			}

			if (maxRecords > 0 && recordCount >= maxRecords)
			{
				return true;
			}

			// Check if limits have been reached.
			if (iteration > policy.maxRetries)
			{
				StringBuilder sb = new StringBuilder(2048);
				sb.Append("Max retries exceeded: ");
				sb.Append(policy.maxRetries);
				sb.Append(System.Environment.NewLine);

				if (exceptions != null)
				{
					sb.Append("sub-exceptions:");
					sb.Append(System.Environment.NewLine);

					foreach (AerospikeException e in exceptions)
					{
						sb.Append(e.Message);
						sb.Append(System.Environment.NewLine);
					}
				}

				AerospikeException ae = new AerospikeException(ResultCode.MAX_RETRIES_EXCEEDED, sb.ToString());
				ae.Policy = policy;
				ae.Iteration = iteration;
				throw ae;
			}

			if (policy.totalTimeout > 0)
			{
				// Check for total timeout.
				long remaining = (long)deadline.Subtract(DateTime.UtcNow).TotalMilliseconds - sleepBetweenRetries;

				if (remaining <= 0)
				{
					throw new AerospikeException.Timeout(policy, iteration);
				}

				if (remaining < totalTimeout)
				{
					totalTimeout = (int)remaining;

					if (socketTimeout > totalTimeout)
					{
						socketTimeout = totalTimeout;
					}
				}
			}

			// Prepare for next iteration.
			if (maxRecords > 0) 
			{
				maxRecords -= recordCount;
			}
			iteration++;
			return false;
		}

		public bool ShouldRetry(NodePartitions nodePartitions, AerospikeException ae)
		{
			ae.Iteration = iteration;

			switch (ae.Result)
			{
				case ResultCode.SERVER_NOT_AVAILABLE:
				case ResultCode.TIMEOUT:
				case ResultCode.INDEX_NOTFOUND:
				case ResultCode.INDEX_NOTREADABLE:
					// Multiple scan/query threads may call this method, so exception
					// list must be modified under lock.
					lock (this)
					{
						if (exceptions == null)
						{
							exceptions = new List<AerospikeException>();
						}
						exceptions.Add(ae);
					}
					MarkRetry(nodePartitions);
					nodePartitions.partsUnavailable = nodePartitions.partsFull.Count + nodePartitions.partsPartial.Count;
					return true;

				default:
					return false;
			}
		}

		private void MarkRetry(NodePartitions nodePartitions)
		{
			foreach (PartitionStatus ps in nodePartitions.partsFull)
			{
				ps.retry = true;
			}

			foreach (PartitionStatus ps in nodePartitions.partsPartial)
			{
				ps.retry = true;
			}
		}

		public void PartitionError()
		{
			// Mark all partitions for retry on fatal errors.
			if (partitionFilter != null)
			{
				partitionFilter.retry = true;
			}
		}
	}

	public sealed class NodePartitions
	{
		public readonly Node node;
		public readonly List<PartitionStatus> partsFull;
		public readonly List<PartitionStatus> partsPartial;
		public long recordCount;
		public long recordMax;
		public int partsUnavailable;

		public NodePartitions(Node node, int capacity)
		{
			this.node = node;
			this.partsFull = new List<PartitionStatus>(capacity);
			this.partsPartial = new List<PartitionStatus>(capacity);
		}

		public void AddPartition(PartitionStatus part)
		{
			if (part.digest == null)
			{
				partsFull.Add(part);
			}
			else
			{
				partsPartial.Add(part);
			}
		}
	}
}
