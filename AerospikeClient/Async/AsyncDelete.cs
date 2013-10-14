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
	public sealed class AsyncDelete : AsyncSingleCommand
	{
		private readonly DeleteListener listener;
		private bool existed;

		public AsyncDelete(AsyncCluster cluster, Key key, DeleteListener listener) 
			: base(cluster, key)
		{
			this.listener = listener;
		}

		protected internal override void ParseResult()
		{
			int resultCode = byteBuffer[5];

			if (resultCode == 0)
			{
				existed = true;
			}
			else
			{
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					existed = false;
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
				listener.OnSuccess(key, existed);
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