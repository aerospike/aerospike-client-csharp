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
	public sealed class AsyncBatchExistsSequence : AsyncMultiCommand
	{
		private readonly ExistsSequenceListener listener;

		public AsyncBatchExistsSequence(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, ExistsSequenceListener listener) 
			: base(parent, cluster, node, false)
		{
			this.listener = listener;
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}
			listener.OnExists(key, resultCode == 0);
		}
	}
}