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

namespace Aerospike.Client
{
	public sealed class PartitionTracker
	{
		private readonly PartitionStatus[] partitionsAll;
		private readonly int partitionsCapacity;
		private readonly int partitionBegin;
		private readonly int nodeCapacity;
		private readonly Node nodeFilter;
		private List<NodePartitions> nodePartitionsList;
		private long maxRecords;
		private int sleepBetweenRetries;
		public int socketTimeout;
		public int totalTimeout;
		public int iteration = 1;
		private DateTime deadline;

		public PartitionTracker(ScanPolicy policy, Node[] nodes)
			: this((Policy)policy, nodes)
		{
			this.maxRecords = policy.maxRecords;
		}

		public PartitionTracker(QueryPolicy policy, Node[] nodes)
			: this((Policy)policy, nodes)
		{
			this.maxRecords = policy.maxRecords;
		}

		public PartitionTracker(Policy policy, Node[] nodes)
		{
			this.partitionBegin = 0;
			this.nodeCapacity = nodes.Length;
			this.nodeFilter = null;

			// Create initial partition capacity for each node as average + 25%.
			int ppn = Node.PARTITIONS / nodes.Length;
			ppn += (int)((uint)ppn >> 2);
			this.partitionsCapacity = ppn;
			this.partitionsAll = Init(policy, Node.PARTITIONS, null);
		}

		public PartitionTracker(ScanPolicy policy, Node nodeFilter)
			: this((Policy)policy, nodeFilter)
		{
			this.maxRecords = policy.maxRecords;
		}

		public PartitionTracker(QueryPolicy policy, Node nodeFilter)
			: this((Policy)policy, nodeFilter)
		{
			this.maxRecords = policy.maxRecords;
		}

		public PartitionTracker(Policy policy, Node nodeFilter)
		{
			this.partitionBegin = 0;
			this.nodeCapacity = 1;
			this.nodeFilter = nodeFilter;
			this.partitionsCapacity = Node.PARTITIONS;
			this.partitionsAll = Init(policy, Node.PARTITIONS, null);
		}

		public PartitionTracker(ScanPolicy policy, Node[] nodes, PartitionFilter filter)
			: this((Policy)policy, nodes, filter)
		{
			this.maxRecords = policy.maxRecords;
		}

		public PartitionTracker(QueryPolicy policy, Node[] nodes, PartitionFilter filter)
			: this((Policy)policy, nodes, filter)
		{
			this.maxRecords = policy.maxRecords;
		}

		public PartitionTracker(Policy policy, Node[] nodes, PartitionFilter filter)
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

			this.partitionBegin = filter.begin;
			this.nodeCapacity = nodes.Length;
			this.nodeFilter = null;
			this.partitionsCapacity = filter.count;
			this.partitionsAll = Init(policy, filter.count, filter.digest);
		}

		private PartitionStatus[] Init(Policy policy, int partitionCount, byte[] digest)
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
			return partsAll;
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
			Partitions partitions;

			if (!map.TryGetValue(ns, out partitions))
			{
				throw new AerospikeException.InvalidNamespace(ns, map.Count);
			}

			Node[] master = partitions.replicas[0];

			foreach (PartitionStatus part in partitionsAll)
			{
				if (!part.done)
				{
					Node node = master[part.id];

					if (node == null)
					{
						throw new AerospikeException.InvalidNode(part.id);
					}

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

			if (maxRecords > 0)
			{
				// Distribute maxRecords across nodes.
				int nodeSize = list.Count;

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

		public void PartitionDone(NodePartitions nodePartitions, int partitionId)
		{
			partitionsAll[partitionId - partitionBegin].done = true;
			nodePartitions.partsReceived++;
		}

		public void SetDigest(NodePartitions nodePartitions, Key key)
		{
			uint partitionId = Partition.GetPartitionId(key.digest);
			partitionsAll[partitionId - partitionBegin].digest = key.digest;
			nodePartitions.recordCount++;
		}

		public bool IsComplete(Policy policy)
		{
			long recordCount = 0;
			int partsRequested = 0;
			int partsReceived = 0;

			foreach (NodePartitions np in nodePartitionsList)
			{
				recordCount += np.recordCount;
				partsRequested += np.partsRequested;
				partsReceived += np.partsReceived;
				//Log.Info("Node " + np.node + " partsFull=" + np.partsFull.Count + " partsPartial=" + np.partsPartial.Count +
				//	" partsReceived=" + np.partsReceived + " recordsRequested=" + np.recordMax + " recordsReceived=" + np.recordCount);
			}

			if (partsReceived >= partsRequested || (maxRecords > 0 && recordCount >= maxRecords))
			{
				return true;
			}

			// Check if limits have been reached.
			if (iteration > policy.maxRetries)
			{
				AerospikeException ae = new AerospikeException(ResultCode.MAX_RETRIES_EXCEEDED, "Max retries exceeded: " + policy.maxRetries);
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

		public bool ShouldRetry(AerospikeException ae)
		{
			switch (ae.Result)
			{
				case ResultCode.SERVER_NOT_AVAILABLE:
				case ResultCode.PARTITION_UNAVAILABLE:
				case ResultCode.TIMEOUT:
					return true;

				default:
					return false;
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
		public int partsRequested;
		public int partsReceived;

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
			partsRequested++;
		}
	}

	public sealed class PartitionStatus
	{
		public byte[] digest;
		public readonly int id;
		public bool done;

		public PartitionStatus(int id)
		{
			this.id = id;
		}
	}
}
