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
	public sealed class ThreadLocalData2
	{
		//private static final int MAX_BUFFER_SIZE = 1024 * 1024;  // 1 MB
		private const int THREAD_LOCAL_CUTOFF = 1024 * 128; // 128 KB

		private static readonly ThreadLocal<byte[]> BufferThreadLocal2 = new ThreadLocal<byte[]>(() =>
		{
			return new byte[8192];
		});

		public static byte[] GetBuffer()
		{
			return BufferThreadLocal2.Value;
		}

		public static byte[] ResizeBuffer(int size)
		{
			// Do not store extremely large buffers in thread local storage.
			if (size > THREAD_LOCAL_CUTOFF)
			{
				/*
				if (size > MAX_BUFFER_SIZE) {
					throw new IllegalArgumentException("Thread " + Thread.currentThread().getId() + " invalid buffer2 size: " + size);
				}*/

				if (Log.DebugEnabled())
				{
					Log.Debug("Thread " + Thread.CurrentThread.ManagedThreadId + " allocate buffer2 on heap " + size);
				}
				return new byte[size];
			}

			if (Log.DebugEnabled())
			{
				Log.Debug("Thread " + Thread.CurrentThread.ManagedThreadId + " resize buffer2 to " + size);
			}
			BufferThreadLocal2.Value = new byte[size];
			return BufferThreadLocal2.Value;
		}
	}
}