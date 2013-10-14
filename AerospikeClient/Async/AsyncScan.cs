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
	public sealed class AsyncScan : AsyncMultiCommand
	{
		private readonly RecordSequenceListener listener;

		public AsyncScan(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, RecordSequenceListener listener) 
			: base(parent, cluster, node, true)
		{
			this.listener = listener;
		}

		protected internal override void ParseRow(Key key)
		{
			Record record = ParseRecord();
			listener.OnRecord(key, record);
		}
	}
}