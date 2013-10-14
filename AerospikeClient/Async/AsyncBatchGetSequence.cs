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
	public sealed class AsyncBatchGetSequence : AsyncMultiCommand
	{
		private readonly RecordSequenceListener listener;

		public AsyncBatchGetSequence(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, HashSet<string> binNames, RecordSequenceListener listener) 
			: base(parent, cluster, node, false, binNames)
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