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
using System.Threading;

namespace Aerospike.Client
{
	public sealed class ThreadLocalData
	{
		//private static final int MAX_BUFFER_SIZE = 1024 * 1024;  // 1 MB
		private const int THREAD_LOCAL_CUTOFF = 1024 * 128; // 128 KB

		[ThreadStatic]
		private static byte[] BufferThreadLocal;

		public static byte[] GetBuffer()
		{
			if (BufferThreadLocal == null)
			{
				BufferThreadLocal = new byte[8192];
			}
			return BufferThreadLocal;
		}

		public static byte[] ResizeBuffer(int size)
		{
			// Do not store extremely large buffers in thread local storage.
			if (size > THREAD_LOCAL_CUTOFF)
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Thread " + Thread.CurrentThread.ManagedThreadId + " allocate buffer on heap " + size);
				}
				return new byte[size];
			}

			if (Log.DebugEnabled())
			{
				Log.Debug("Thread " + Thread.CurrentThread.ManagedThreadId + " resize buffer to " + size);
			}
			BufferThreadLocal = new byte[size];
			return BufferThreadLocal;
		}
	}
}