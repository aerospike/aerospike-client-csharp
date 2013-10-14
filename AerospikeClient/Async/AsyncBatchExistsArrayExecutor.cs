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
	public sealed class AsyncBatchExistsArrayExecutor : AsyncBatchExecutor
	{
		private readonly ExistsArrayListener listener;
		private readonly bool[] existsArray;

		public AsyncBatchExistsArrayExecutor(AsyncCluster cluster, Policy policy, Key[] keys, ExistsArrayListener listener) 
			: base(cluster, keys)
		{
			this.existsArray = new bool[keys.Length];
			this.listener = listener;

			Dictionary<Key, BatchItem> keyMap = BatchItem.GenerateMap(keys);

			// Dispatch asynchronous commands to nodes.
			foreach (BatchNode batchNode in batchNodes)
			{
				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					Command command = new Command();
					command.SetBatchExists(batchNamespace);

					AsyncBatchExistsArray async = new AsyncBatchExistsArray(this, cluster, (AsyncNode)batchNode.node, keyMap, existsArray);
					async.Execute(policy, command);
				}
			}
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(keys, existsArray);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}
}