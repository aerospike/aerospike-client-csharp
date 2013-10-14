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
	public sealed class AsyncExists : AsyncSingleCommand
	{
		private readonly ExistsListener listener;
		private bool exists;

		public AsyncExists(AsyncCluster cluster, Key key, ExistsListener listener) 
			: base(cluster, key)
		{
			this.listener = listener;
		}

		protected internal override void ParseResult()
		{
			int resultCode = byteBuffer[5];

			if (resultCode == 0)
			{
				exists = true;
			}
			else
			{
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					exists = false;
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
				listener.OnSuccess(key, exists);
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