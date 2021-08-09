/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
			Node[] nodes = cluster.ValidateNodes();

			// Create initial key capacity for each node as average + 25%.
			int keysPerNode = keys.Length / nodes.Length;
			keysPerNode += (int)((uint)keysPerNode >> 2);

			// The minimum key capacity is 10.
			if (keysPerNode < 10)
			{
				keysPerNode = 10;
			}

			Replica replica = policy.replica;
			Replica replicaSC = Partition.GetReplicaSC(policy);

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodes.Length);

			for (int i = 0; i < keys.Length; i++)
			{
				Node node = Partition.GetNodeBatchRead(cluster, keys[i], replica, replicaSC, null, 0, 0);
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

		public static List<BatchNode> GenerateList
		(
			Cluster cluster,
			BatchPolicy policy,
			Key[] keys,
			uint sequenceAP,
			uint sequenceSC,
			BatchNode batchSeed
		)
		{
			Node[] nodes = cluster.ValidateNodes();

			// Create initial key capacity for each node as average + 25%.
			int keysPerNode = batchSeed.offsetsSize / nodes.Length;
			keysPerNode += (int)((uint)keysPerNode >> 2);

			// The minimum key capacity is 10.
			if (keysPerNode < 10)
			{
				keysPerNode = 10;
			}

			Replica replica = policy.replica;
			Replica replicaSC = Partition.GetReplicaSC(policy);

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodes.Length);

			for (int i = 0; i < batchSeed.offsetsSize; i++)
			{
				int offset = batchSeed.offsets[i];

				Node node = Partition.GetNodeBatchRead(cluster, keys[offset], replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);	
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
			Node[] nodes = cluster.ValidateNodes();

			// Create initial key capacity for each node as average + 25%.
			int max = records.Count;
			int keysPerNode = max / nodes.Length;
			keysPerNode += (int)((uint)keysPerNode >> 2);

			// The minimum key capacity is 10.
			if (keysPerNode < 10)
			{
				keysPerNode = 10;
			}

			Replica replica = policy.replica;
			Replica replicaSC = Partition.GetReplicaSC(policy);

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodes.Length);

			for (int i = 0; i < max; i++)
			{
				Node node = Partition.GetNodeBatchRead(cluster, records[i].key, replica, replicaSC, null, 0, 0);
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

		public static List<BatchNode> GenerateList
		(
			Cluster cluster,
			BatchPolicy policy,
			List<BatchRead> records,
			uint sequenceAP,
			uint sequenceSC,
			BatchNode batchSeed
		)
		{
			Node[] nodes = cluster.ValidateNodes();

			// Create initial key capacity for each node as average + 25%.
			int keysPerNode = batchSeed.offsetsSize / nodes.Length;
			keysPerNode += (int)((uint)keysPerNode >> 2);

			// The minimum key capacity is 10.
			if (keysPerNode < 10)
			{
				keysPerNode = 10;
			}

			Replica replica = policy.replica;
			Replica replicaSC = Partition.GetReplicaSC(policy);

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodes.Length);

			for (int i = 0; i < batchSeed.offsetsSize; i++)
			{
				int offset = batchSeed.offsets[i];

				Node node = Partition.GetNodeBatchRead(cluster, records[offset].key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);
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
