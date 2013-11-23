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
	public sealed class TouchCommand : SingleCommand
	{
		private readonly WritePolicy policy;

		public TouchCommand(Cluster cluster, WritePolicy policy, Key key) 
			: base(cluster, key)
		{
			this.policy = (policy == null) ? new WritePolicy() : policy;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetTouch(policy, key);
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