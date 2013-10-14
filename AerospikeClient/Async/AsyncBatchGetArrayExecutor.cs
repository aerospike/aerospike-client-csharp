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
	public sealed class AsyncBatchGetArrayExecutor : AsyncBatchExecutor
	{
		private readonly RecordArrayListener listener;
		private readonly Record[] recordArray;

		public AsyncBatchGetArrayExecutor(AsyncCluster cluster, Policy policy, RecordArrayListener listener, Key[] keys, HashSet<string> binNames, int readAttr) 
			: base(cluster, keys)
		{
			this.recordArray = new Record[keys.Length];
			this.listener = listener;

			Dictionary<Key, BatchItem> keyMap = BatchItem.GenerateMap(keys);

			// Dispatch asynchronous commands to nodes.
			foreach (BatchNode batchNode in batchNodes)
			{
				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					Command command = new Command();
					command.SetBatchGet(batchNamespace, binNames, readAttr);

					AsyncBatchGetArray async = new AsyncBatchGetArray(this, cluster, (AsyncNode)batchNode.node, keyMap, binNames, recordArray);
					async.Execute(policy, command);
				}
			}
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(keys, recordArray);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}
}