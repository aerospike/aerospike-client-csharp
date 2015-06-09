/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
				Node node = cluster.GetReadNode(partition, policy.replica);
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

		public static List<BatchNode> GenerateList(Cluster cluster, BatchPolicy policy, List<BatchRecord> records)
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
				Node node = cluster.GetReadNode(partition, policy.replica);
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
		public List<BatchNamespace> batchNamespaces; // used by old batch only

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

		public void SplitByNamespace(Key[] keys)
		{
			string first = keys[offsets[0]].ns;

			// Optimize for single namespace.
			if (IsSingleNamespace(keys, first))
			{
				batchNamespaces = new List<BatchNamespace>(1);
				batchNamespaces.Add(new BatchNamespace(first, offsets, offsetsSize));
				return;
			}

			// Process multiple namespaces.
			batchNamespaces = new List<BatchNamespace>(4);

			for (int i = 0; i < offsetsSize; i++)
			{
				int offset = offsets[i];
				string ns = keys[offset].ns;
				BatchNamespace batchNamespace = FindNamespace(batchNamespaces, ns);

				if (batchNamespace == null)
				{
					batchNamespaces.Add(new BatchNamespace(ns, offsetsSize, offset));
				}
				else
				{
					batchNamespace.Add(offset);
				}
			}
		}

		private bool IsSingleNamespace(Key[] keys, string first)
		{
			for (int i = 1; i < offsetsSize; i++)
			{
				string ns = keys[offsets[i]].ns;

				if (!(ns == first || ns.Equals(first)))
				{
					return false;
				}
			}
			return true;
		}

		private BatchNamespace FindNamespace(List<BatchNamespace> batchNamespaces, string ns)
		{
			foreach (BatchNamespace batchNamespace in batchNamespaces)
			{
				// Note: use both pointer equality and equals.
				if (batchNamespace.ns == ns || batchNamespace.ns.Equals(ns))
				{
					return batchNamespace;
				}
			}
			return null;
		}

		public sealed class BatchNamespace
		{
			public readonly string ns;
			public int[] offsets;
			public int offsetsSize;

			public BatchNamespace(string ns, int capacity, int offset)
			{
				this.ns = ns;
				this.offsets = new int[capacity];
				this.offsets[0] = offset;
				this.offsetsSize = 1;
			}

			public BatchNamespace(string ns, int[] offsets, int offsetsSize)
			{
				this.ns = ns;
				this.offsets = offsets;
				this.offsetsSize = offsetsSize;
			}

			public void Add(int offset)
			{
				offsets[offsetsSize++] = offset;
			}
		}
	}
}
