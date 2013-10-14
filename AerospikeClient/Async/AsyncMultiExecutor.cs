/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System.Threading;

namespace Aerospike.Client
{
	public abstract class AsyncMultiExecutor
	{
		private int completedCount;
		protected internal int completedSize;
		private bool failed;

		protected internal void ChildSuccess()
		{
			int count = Interlocked.Increment(ref completedCount);

			if (!failed && count >= completedSize)
			{
				OnSuccess();
			}
		}

		protected internal void ChildFailure(AerospikeException ae)
		{
			failed = true;
			Interlocked.Increment(ref completedCount);
			OnFailure(ae);
		}

		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}