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
	public sealed class ExistsCommand : SingleCommand
	{
		private readonly Policy policy;
		private bool exists;

		public ExistsCommand(Cluster cluster, Policy policy, Key key)
			: base(cluster, key)
		{
			this.policy = (policy == null) ? new Policy() : policy;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetExists(key);
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

			int resultCode = dataBuffer[13];

			if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR)
			{
				throw new AerospikeException(resultCode);
			}
			exists = resultCode == 0;
			EmptySocket(conn);
		}

		public bool Exists()
		{
			return exists;
		}
	}
}