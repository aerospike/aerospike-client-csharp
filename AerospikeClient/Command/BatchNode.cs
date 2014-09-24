/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
		public static List<BatchNode> GenerateList(Cluster cluster, Key[] keys)
		{
			Node[] nodes = cluster.Nodes;

			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}

			int nodeCount = nodes.Length;
			int keysPerNode = keys.Length / nodeCount + 10;

			// Split keys by server node.
			List<BatchNode> batchNodes = new List<BatchNode>(nodeCount + 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = keys[i];
				Partition partition = new Partition(key);
				BatchNode batchNode;

				Node node = cluster.GetNode(partition);
				batchNode = FindBatchNode(batchNodes, node);

				if (batchNode == null)
				{
					batchNodes.Add(new BatchNode(node, keysPerNode, key.ns, i));
				}
				else
				{
					batchNode.AddKey(key.ns, i);
				}
			}
			return batchNodes;
		}

		public readonly Node node;
		public readonly List<BatchNamespace> batchNamespaces;
		public readonly int keyCapacity;

		public BatchNode(Node node, int keyCapacity, string ns, int offset)
		{
			this.node = node;
			this.keyCapacity = keyCapacity;
			batchNamespaces = new List<BatchNamespace>(4);
			batchNamespaces.Add(new BatchNamespace(ns, keyCapacity, offset));
		}

		public void AddKey(string ns, int offset)
		{
			BatchNamespace batchNamespace = FindNamespace(ns);

			if (batchNamespace == null)
			{
				batchNamespaces.Add(new BatchNamespace(ns, keyCapacity, offset));
			}
			else
			{
				batchNamespace.Add(offset);
			}
		}

		private BatchNamespace FindNamespace(string ns)
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

		private static BatchNode FindBatchNode(List<BatchNode> nodes, Node node)
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

			public void Add(int offset)
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
}
