/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
