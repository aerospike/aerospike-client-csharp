/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
	public sealed class BatchNode
	{
		public static List<BatchNode> GenerateList(Cluster cluster, BatchPolicy policy, Key[] keys)
		{
			Node[] nodes = cluster.Nodes;

			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}

			// Create initial key capacity for each node as average + 25%.
			int keysPerNode = keys.Length / nodes.Length;
			keysPerNode += (int)((uint)keysPerNode >> 2);

			// The minimum key capacity is 10.
			if (keysPerNode < 10)
			{
				keysPerNode = 10;
			}

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodes.Length);

			for (int i = 0; i < keys.Length; i++)
			{
				Partition partition = new Partition(keys[i]);
				Node node = GetNode(cluster, partition, policy.replica, 0);
				BatchNode batchNode = FindBatchNode(batchNodes, node);

				if (batchNode == null)
				{
					batchNodes.Add(new BatchNode(node, keysPerNode, i));
				}
				else
				{
					batchNode.AddKey(i);
				}
			}
			return batchNodes;
		}

		public static List<BatchNode> GenerateList(Cluster cluster, BatchPolicy policy, Key[] keys, uint sequence, BatchNode batchSeed)
		{
			Node[] nodes = cluster.Nodes;

			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}

			// Create initial key capacity for each node as average + 25%.
			int keysPerNode = batchSeed.offsetsSize / nodes.Length;
			keysPerNode += (int)((uint)keysPerNode >> 2);

			// The minimum key capacity is 10.
			if (keysPerNode < 10)
			{
				keysPerNode = 10;
			}

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodes.Length);

			for (int i = 0; i < batchSeed.offsetsSize; i++)
			{
				int offset = batchSeed.offsets[i];
				Partition partition = new Partition(keys[offset]);
				Node node = GetNode(cluster, partition, policy.replica, sequence);
				BatchNode batchNode = FindBatchNode(batchNodes, node);

				if (batchNode == null)
				{
					batchNodes.Add(new BatchNode(node, keysPerNode, offset));
				}
				else
				{
					batchNode.AddKey(offset);
				}
			}
			return batchNodes;
		}
		
		public static List<BatchNode> GenerateList(Cluster cluster, BatchPolicy policy, List<BatchRead> records)
		{
			Node[] nodes = cluster.Nodes;

			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}

			// Create initial key capacity for each node as average + 25%.
			int max = records.Count;
			int keysPerNode = max / nodes.Length;
			keysPerNode += (int)((uint)keysPerNode >> 2);

			// The minimum key capacity is 10.
			if (keysPerNode < 10)
			{
				keysPerNode = 10;
			}

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodes.Length);

			for (int i = 0; i < max; i++)
			{
				Partition partition = new Partition(records[i].key);
				Node node = GetNode(cluster, partition, policy.replica, 0);
				BatchNode batchNode = FindBatchNode(batchNodes, node);

				if (batchNode == null)
				{
					batchNodes.Add(new BatchNode(node, keysPerNode, i));
				}
				else
				{
					batchNode.AddKey(i);
				}
			}
			return batchNodes;
		}

		public static List<BatchNode> GenerateList(Cluster cluster, BatchPolicy policy, List<BatchRead> records, uint sequence, BatchNode batchSeed)
		{
			Node[] nodes = cluster.Nodes;

			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}

			// Create initial key capacity for each node as average + 25%.
			int keysPerNode = batchSeed.offsetsSize / nodes.Length;
			keysPerNode += (int)((uint)keysPerNode >> 2);

			// The minimum key capacity is 10.
			if (keysPerNode < 10)
			{
				keysPerNode = 10;
			}

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodes.Length);

			for (int i = 0; i < batchSeed.offsetsSize; i++)
			{
				int offset = batchSeed.offsets[i];
				Partition partition = new Partition(records[offset].key);
				Node node = GetNode(cluster, partition, policy.replica, sequence);
				BatchNode batchNode = FindBatchNode(batchNodes, node);

				if (batchNode == null)
				{
					batchNodes.Add(new BatchNode(node, keysPerNode, offset));
				}
				else
				{
					batchNode.AddKey(offset);
				}
			}
			return batchNodes;
		}
		
		private static Node GetNode(Cluster cluster, Partition partition, Replica replica, uint sequence)
		{
			switch (replica)
			{
				case Replica.SEQUENCE:
					return GetSequenceNode(cluster, partition, sequence);

				case Replica.PREFER_RACK:
					return GetRackNode(cluster, partition, sequence);

				default:
				case Replica.MASTER:
					return cluster.GetMasterNode(partition);

				case Replica.MASTER_PROLES:
					return cluster.GetMasterProlesNode(partition);

				case Replica.RANDOM:
					return cluster.GetRandomNode();
			}
		}

		private static Node GetSequenceNode(Cluster cluster, Partition partition, uint sequence)
		{
			// Must copy hashmap reference for copy on write semantics to work.
			Dictionary<string, Partitions> map = cluster.partitionMap;
			Partitions partitions;

			if (!map.TryGetValue(partition.ns, out partitions))
			{
				throw new AerospikeException.InvalidNamespace(partition.ns, map.Count);
			}

			Node[][] replicas = partitions.replicas;

			for (int i = 0; i < replicas.Length; i++)
			{
				uint index = sequence % (uint)replicas.Length;
				Node node = replicas[index][partition.partitionId];

				if (node != null && node.Active)
				{
					return node;
				}
				sequence++;
			}
			Node[] nodeArray = cluster.Nodes;
			throw new AerospikeException.InvalidNode(nodeArray.Length, partition);
		}

		private static Node GetRackNode(Cluster cluster, Partition partition, uint sequence)
		{
			// Must copy hashmap reference for copy on write semantics to work.
			Dictionary<string, Partitions> map = cluster.partitionMap;
			Partitions partitions;

			if (!map.TryGetValue(partition.ns, out partitions))
			{
				throw new AerospikeException.InvalidNamespace(partition.ns, map.Count);
			}

			Node[][] replicas = partitions.replicas;
			Node fallback = null;

			for (int i = 0; i < replicas.Length; i++)
			{
				uint index = sequence % (uint)replicas.Length;
				Node node = replicas[index][partition.partitionId];

				if (node != null && node.Active)
				{
					if (node.HasRack(partition.ns, cluster.rackId))
					{
						return node;
					}

					if (fallback == null)
					{
						fallback = node;
					}
				}
				sequence++;
			}

			if (fallback != null)
			{
				return fallback;
			}

			Node[] nodeArray = cluster.Nodes;
			throw new AerospikeException.InvalidNode(nodeArray.Length, partition);
		}

		private static BatchNode FindBatchNode(IList<BatchNode> nodes, Node node)
		{
			foreach (BatchNode batchNode in nodes)
			{
				// Note: using pointer equality for performance.
				if (batchNode.node == node)
				{
					return batchNode;
				}
			}
			return null;
		}

		public readonly Node node;
		public int[] offsets;
		public int offsetsSize;

		public BatchNode(Node node, int capacity, int offset)
		{
			this.node = node;
			this.offsets = new int[capacity];
			this.offsets[0] = offset;
			this.offsetsSize = 1;
		}

		public BatchNode(Node node, Key[] keys)
		{
			this.node = node;
			this.offsets = new int[keys.Length];
			this.offsetsSize = keys.Length;

			for (int i = 0; i < offsetsSize; i++)
			{
				offsets[i] = i;
			}
		}

		public void AddKey(int offset)
		{
			if (offsetsSize >= offsets.Length)
			{
				int[] copy = new int[offsetsSize * 2];
				Array.Copy(offsets, 0, copy, 0, offsetsSize);
				offsets = copy;
			}
			offsets[offsetsSize++] = offset;
		}
	}
}
