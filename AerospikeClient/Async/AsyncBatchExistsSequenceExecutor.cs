/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
namespace Aerospike.Client
{
	public sealed class AsyncBatchExistsSequenceExecutor : AsyncBatchExecutor
	{
		private readonly ExistsSequenceListener listener;

		public AsyncBatchExistsSequenceExecutor(AsyncCluster cluster, Policy policy, Key[] keys, ExistsSequenceListener listener) 
			: base(cluster, keys)
		{
			this.listener = listener;

			// Dispatch asynchronous commands to nodes.
			foreach (BatchNode batchNode in batchNodes)
			{
				foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
				{
					Command command = new Command();
					command.SetBatchExists(batchNamespace);

					AsyncBatchExistsSequence async = new AsyncBatchExistsSequence(this, cluster, (AsyncNode)batchNode.node, listener);
					async.Execute(policy, command);
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