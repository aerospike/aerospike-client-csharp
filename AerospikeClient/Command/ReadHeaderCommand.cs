/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class ReadHeaderCommand : SingleCommand
	{
		private readonly Policy policy;
		private Record record;

		public ReadHeaderCommand(Cluster cluster, Policy policy, Key key) 
			: base(cluster, key)
		{
			this.policy = (policy == null) ? new Policy() : policy;
		}

		protected internal sealed override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetReadHeader(key);
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.		
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

			int resultCode = dataBuffer[13];

			if (resultCode == 0)
			{
				int generation = ByteUtil.BytesToInt(dataBuffer, 14);
				int expiration = ByteUtil.BytesToInt(dataBuffer, 18);
				record = new Record(null, generation, expiration);
			}
			else
			{
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					record = null;
				}
				else
				{
					throw new AerospikeException(resultCode);
				}
			}
			EmptySocket(conn);
		}

		public Record Record
		{
			get
			{
				return record;
			}
		}
	}
}