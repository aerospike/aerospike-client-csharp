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
	public sealed class WriteCommand : SingleCommand
	{
		private readonly WritePolicy policy;
		private readonly Bin[] bins;
		private readonly Operation.Type operation;

		public WriteCommand(Cluster cluster, WritePolicy policy, Key key, Bin[] bins, Operation.Type operation) 
			: base(cluster, key)
		{
			this.policy = (policy == null) ? new WritePolicy() : policy;
			this.bins = bins;
			this.operation = operation;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetWrite(policy, operation, key, bins);
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.		
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

			int resultCode = dataBuffer[13];

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}
			EmptySocket(conn);
		}
	}
}