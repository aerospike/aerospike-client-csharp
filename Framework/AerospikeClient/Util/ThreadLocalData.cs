/* 
 * Copyright 2012-2021 Aerospike, Inc.
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

#if (IIS)
using System.Web;
#endif

namespace Aerospike.Client
{
	public sealed class ThreadLocalData
	{
		public static int THREAD_LOCAL_CUTOFF = 1024 * 128; // 128 KB

		[ThreadStatic]
		private static byte[] BufferThreadLocal;

		#if (! IIS)
		//--------------------------------------------------------------------------------
		// Regular client applications always use thread static to store reusable buffers.
		//--------------------------------------------------------------------------------

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

		#else
		//----------------------------------------------------------------------
		// IIS web server applications use HttpContext to store reusable buffers
		// when a http context is defined in the current thread.
		//----------------------------------------------------------------------
		
		private static bool IsWebContext
		{
			get
			{
				try
				{
					return HttpContext.Current != null && HttpContext.Current.Items != null;
				}
				catch
				{
					return false;
				}
			}
		}

		private static byte[] Buffer
		{
			get
			{
				if (IsWebContext)
				{
					var val = HttpContext.Current.Items["AeroSpike.Client.ThreadLocalData"];
					return val != null ? (byte[])val : null;

				}
				else
				{
					return BufferThreadLocal;
				}
			}
			set
			{
				if (IsWebContext)
				{
					HttpContext.Current.Items["AeroSpike.Client.ThreadLocalData"] = value;
				}
				else
				{
					BufferThreadLocal = value;
				}
			}
		}

		private static bool BufferExists
		{
			get
			{
				if (IsWebContext)
				{
					return HttpContext.Current.Items["AeroSpike.Client.ThreadLocalData"] != null;
				}
				else
				{
					return BufferThreadLocal != null;
				}
			}
		}

		public static byte[] GetBuffer()
		{
			if (!BufferExists)
			{
				Buffer = new byte[8192];
			}
			return Buffer;
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
			Buffer = new byte[size];
			return Buffer;
		}
		#endif
	}
}
