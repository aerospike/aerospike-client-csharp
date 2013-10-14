/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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