/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class BatchNode
	{
		public static List<BatchNode> GenerateList(Cluster cluster, Key[] keys)
		{

			int nodeCount = cluster.Nodes.Length;
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
					batchNodes.Add(new BatchNode(node, keysPerNode, key));
				}
				else
				{
					batchNode.AddKey(key);
				}
			}
			return batchNodes;
		}

		public readonly Node node;
		public readonly List<BatchNamespace> batchNamespaces;
		public readonly int keyCapacity;

		public BatchNode(Node node, int keyCapacity, Key key)
		{
			this.node = node;
			this.keyCapacity = keyCapacity;
			batchNamespaces = new List<BatchNamespace>(4);
			batchNamespaces.Add(new BatchNamespace(key.ns, keyCapacity, key));
		}

		public void AddKey(Key key)
		{
			BatchNamespace batchNamespace = FindNamespace(key.ns);

			if (batchNamespace == null)
			{
				batchNamespaces.Add(new BatchNamespace(key.ns, keyCapacity, key));
			}
			else
			{
				batchNamespace.keys.Add(key);
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
			public readonly List<Key> keys;

			public BatchNamespace(string ns, int capacity, Key key)
			{
				this.ns = ns;
				keys = new List<Key>(capacity);
				keys.Add(key);
			}
		}
	}
}
