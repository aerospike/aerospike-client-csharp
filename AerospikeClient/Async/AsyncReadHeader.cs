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
	public sealed class AsyncReadHeader : AsyncSingleCommand
	{
		private readonly Policy policy;
		private readonly RecordListener listener;
		private Record record;

		public AsyncReadHeader(AsyncCluster cluster, Policy policy, RecordListener listener, Key key) 
			: base(cluster, key)
		{
			this.policy = (policy == null) ? new Policy() : policy;
			this.listener = listener;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetReadHeader(key);
		}

		protected internal override void ParseResult()
		{
			int resultCode = dataBuffer[5];

			if (resultCode == 0)
			{
				int generation = ByteUtil.BytesToInt(dataBuffer, 6);
				int expiration = ByteUtil.BytesToInt(dataBuffer, 10);

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
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(key, record);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}