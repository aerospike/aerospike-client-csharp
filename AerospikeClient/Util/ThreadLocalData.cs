/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
