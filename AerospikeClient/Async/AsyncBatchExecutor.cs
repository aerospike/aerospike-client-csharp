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
	public abstract class AsyncBatchExecutor : AsyncMultiExecutor
	{
		protected internal readonly Key[] keys;
		protected internal readonly List<BatchNode> batchNodes;

		public AsyncBatchExecutor(Cluster cluster, Key[] keys)
		{
			this.keys = keys;
			this.batchNodes = BatchNode.GenerateList(cluster, keys);

			// Count number of asynchronous commands needed.
			int size = 0;
			foreach (BatchNode batchNode in batchNodes)
			{
				size += batchNode.batchNamespaces.Count;
			}
			completedSize = size;
		}
	}
}