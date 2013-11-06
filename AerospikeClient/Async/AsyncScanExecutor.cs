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
	public sealed class AsyncScanExecutor : AsyncMultiExecutor
	{
		private readonly RecordSequenceListener listener;

		public AsyncScanExecutor
		(
			AsyncCluster cluster,
			ScanPolicy policy,
			RecordSequenceListener listener,
			string ns,
			string setName,
			string[] binNames
		)
		{
			this.listener = listener;

			Node[] nodes = cluster.Nodes;
			completedSize = nodes.Length;

			foreach (Node node in nodes)
			{
				AsyncScan async = new AsyncScan(this, cluster, (AsyncNode)node, policy, listener, ns, setName, binNames);
				async.Execute();
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