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
	public sealed class QueryRecordExecutor : QueryExecutor
	{
		private readonly RecordSet recordSet;

		public QueryRecordExecutor(QueryPolicy policy, Statement statement, Node[] nodes) : base(policy, statement)
		{
			this.recordSet = new RecordSet(this, policy.recordQueueSize);
			StartThreads(nodes);
		}

		protected internal override QueryCommand CreateCommand(Node node)
		{
			return new QueryRecordCommand(node, recordSet);
		}

		protected internal override void SendCompleted()
		{
			recordSet.Put(RecordSet.END);
		}

		public RecordSet RecordSet
		{
			get
			{
				return recordSet;
			}
		}
	}
}