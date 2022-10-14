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
using System.Collections;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class BatchNode
	{
		/// <summary>
		/// Assign keys to nodes in initial batch attempt.
		/// </summary>
		public static List<BatchNode> GenerateList
		(
			Cluster cluster,
			BatchPolicy policy,
			Key[] keys,
			BatchRecord[] records,
			bool hasWrite,
			IBatchStatus status
		)
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
			AerospikeException except = null;

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = keys[i];

				try
				{
					Node node = hasWrite ?
						Partition.GetNodeBatchWrite(cluster, key, replica, null, 0) :
						Partition.GetNodeBatchRead(cluster, key, replica, replicaSC, null, 0, 0);

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
				catch (AerospikeException.InvalidNode ain)
				{
					// This method only called on initialization, so inDoubt must be false.
					if (records != null)
					{
						records[i].SetError(ain.Result, false);
					}
					else
					{
						status.SetInvalidNode(cluster, key, i, ain, false, hasWrite);
					}

					if (except == null)
					{
						except = ain;
					}
				}
			}

			if (except != null)
			{
				// Fatal if no key requests were generated on initialization.
				if (batchNodes.Count == 0)
				{
					throw except;
				}
				else
				{
					status.SetInvalidNode(except);
				}
			}
			return batchNodes;
		}

		/// <summary>
		/// Assign keys to nodes in batch node retry.
		/// </summary>
		public static List<BatchNode> GenerateList
		(
			Cluster cluster,
			BatchPolicy policy,
			Key[] keys,
			BatchRecord[] records,
			uint sequenceAP,
			uint sequenceSC,
			BatchNode batchSeed,
			bool hasWrite,
			IBatchStatus status
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
			AerospikeException except = null;

			for (int i = 0; i < batchSeed.offsetsSize; i++)
			{
				int offset = batchSeed.offsets[i];

				if (records[offset].resultCode != ResultCode.NO_RESPONSE)
				{
					// Do not retry keys that already have a response.
					continue;
				}

				Key key = keys[offset];

				try
				{
					Node node = hasWrite ?
						Partition.GetNodeBatchWrite(cluster, key, replica, batchSeed.node, sequenceAP) :
						Partition.GetNodeBatchRead(cluster, key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);

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
				catch (AerospikeException.InvalidNode ain)
				{
					// This method only called on retry, so commandSentCounter(2) will be greater than 1.
					records[offset].SetError(ain.Result, Command.BatchInDoubt(hasWrite, 2));

					if (except == null)
					{
						except = ain;
					}
				}
			}

			if (except != null)
			{
				status.SetInvalidNode(except);
			}
			return batchNodes;
		}

		/// <summary>
		/// Assign keys to nodes in batch node retry for async sequence listeners.
		/// </summary>
		public static List<BatchNode> GenerateList
		(
			Cluster cluster,
			BatchPolicy policy,
			Key[] keys,
			bool[] sent,
			uint sequenceAP,
			uint sequenceSC,
			BatchNode batchSeed,
			bool hasWrite,
			IBatchStatus status
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
			AerospikeException except = null;

			for (int i = 0; i < batchSeed.offsetsSize; i++)
			{
				int offset = batchSeed.offsets[i];

				if (sent[offset])
				{
					// Do not retry keys that already have a response.
					continue;
				}

				Key key = keys[offset];

				try
				{
					Node node = hasWrite ?
						Partition.GetNodeBatchWrite(cluster, key, replica, batchSeed.node, sequenceAP) :
						Partition.GetNodeBatchRead(cluster, key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);

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
				catch (AerospikeException.InvalidNode ain)
				{
					status.SetInvalidNode(cluster, key, offset, ain, Command.BatchInDoubt(hasWrite, 2), hasWrite);

					if (except == null)
					{
						except = ain;
					}
				}
			}

			if (except != null)
			{
				status.SetInvalidNode(except);
			}
			return batchNodes;
		}

		/// <summary>
		/// Assign keys to nodes in batch node retry.
		/// </summary>
		public static List<BatchNode> GenerateList
		(
			Cluster cluster,
			BatchPolicy policy,
			Key[] keys,
			uint sequenceAP,
			uint sequenceSC,
			BatchNode batchSeed,
			bool hasWrite,
			IBatchStatus status
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
			AerospikeException except = null;

			for (int i = 0; i < batchSeed.offsetsSize; i++)
			{
				int offset = batchSeed.offsets[i];

				// This method is only used to retry batch reads and the resultCode is not stored, so
				// retry all keys assigned to this node. Fortunately, it's rare to retry a node after
				// already receiving records and it's harmless to read the same record twice.

				Key key = keys[offset];

				try
				{
					Node node = hasWrite ?
						Partition.GetNodeBatchWrite(cluster, key, replica, batchSeed.node, sequenceAP) :
						Partition.GetNodeBatchRead(cluster, key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);

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
				catch (AerospikeException.InvalidNode ain)
				{
					if (except == null)
					{
						except = ain;
					}
				}
			}

			if (except != null)
			{
				status.SetInvalidNode(except);
			}
			return batchNodes;
		}

		/// <summary>
		/// Assign keys to nodes in initial batch attempt.
		/// </summary>
		public static List<BatchNode> GenerateList
		(
			Cluster cluster,
			BatchPolicy policy,
			IList records,
			IBatchStatus status
		)
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
			AerospikeException except = null;

			for (int i = 0; i < max; i++)
			{
				BatchRecord b = (BatchRecord)records[i];

				try
				{
					b.Prepare();

					Node node = b.hasWrite ?
						Partition.GetNodeBatchWrite(cluster, b.key, replica, null, 0) :
						Partition.GetNodeBatchRead(cluster, b.key, replica, replicaSC, null, 0, 0);

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
				catch (AerospikeException.InvalidNode ain)
				{
					// This method only called on initialization, so inDoubt must be false.
					b.SetError(ain.Result, false);

					if (except == null)
					{
						except = ain;
					}
				}
			}

			if (except != null)
			{
				// Fatal if no key requests were generated on initialization.
				if (batchNodes.Count == 0)
				{
					throw except;
				}
				else
				{
					status.SetInvalidNode(except);
				}
			}
			return batchNodes;
		}

		/// <summary>
		/// Assign keys to nodes in batch node retry.
		/// </summary>
		public static List<BatchNode> GenerateList
		(
			Cluster cluster,
			BatchPolicy policy,
			IList records,
			uint sequenceAP,
			uint sequenceSC,
			BatchNode batchSeed,
			IBatchStatus status
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
			AerospikeException except = null;

			for (int i = 0; i < batchSeed.offsetsSize; i++)
			{
				int offset = batchSeed.offsets[i];
				BatchRecord b = (BatchRecord)records[offset];

				if (b.resultCode != ResultCode.NO_RESPONSE)
				{
					// Do not retry keys that already have a response.
					continue;
				}

				try
				{
					Node node = b.hasWrite ?
						Partition.GetNodeBatchWrite(cluster, b.key, replica, batchSeed.node, sequenceAP) :
						Partition.GetNodeBatchRead(cluster, b.key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);

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
				catch (AerospikeException.InvalidNode ain)
				{
					// This method only called on retry, so commandSentCounter(2) will be greater than 1.
					b.SetError(ain.Result, Command.BatchInDoubt(b.hasWrite, 2));

					if (except == null)
					{
						except = ain;
					}
				}
			}

			if (except != null)
			{
				status.SetInvalidNode(except);
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

	public interface IBatchStatus
	{
		void SetInvalidNode(Cluster cluster, Key key, int index, AerospikeException ae, bool inDoubt, bool hasWrite);
		void SetInvalidNode(AerospikeException ae);
	}
}
