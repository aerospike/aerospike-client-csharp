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
	public sealed class AsyncBatchGetSequenceExecutor : AsyncBatchExecutor
	{
		private readonly RecordSequenceListener listener;

		public AsyncBatchGetSequenceExecutor(AsyncCluster cluster, Policy policy, RecordSequenceListener listener, Key[] keys, HashSet<string> binNames, int readAttr) 
			: base(cluster, keys)
		{
			this.listener = listener;

			if (policy == null)
			{
				policy = new Policy();
			}

			// Dispatch asynchronous commands to nodes.
			foreach (BatchNode batchNode in batchNodes)
			{
				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					AsyncBatchGetSequence async = new AsyncBatchGetSequence(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, binNames, listener, readAttr);
					async.Execute();
				}
			}
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess();
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}
}