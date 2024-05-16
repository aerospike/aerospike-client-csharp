/* 
 * Copyright 2012-2024 Aerospike, Inc.
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

using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace Aerospike.Client
{
	/// <summary>
	/// An awaiter for asynchronous socket operations
	/// </summary>
	// adapted from Stephen Toub's code at
	// https://blogs.msdn.microsoft.com/pfxteam/2011/12/15/awaiting-socket-operations/
	public sealed class SocketAwaitable : ICriticalNotifyCompletion
	{
		// placeholder for when we don't have an actual continuation. does nothing
		readonly static Action _sentinel = () => { };
		// the continuation to use
		Action _continuation;

		/// <summary>
		/// Creates a new instance of the class for the specified <paramref name="eventArgs"/>
		/// </summary>
		/// <param name="eventArgs">The socket event args to use</param>
		public SocketAwaitable(SocketAsyncEventArgs eventArgs)
		{
			if (null == eventArgs) throw new ArgumentNullException("eventArgs");
			EventArgs = eventArgs;
			eventArgs.Completed += delegate
			{
				var prev = _continuation ?? Interlocked.CompareExchange(
					ref _continuation, _sentinel, null);
				if (prev != null) prev();
			};
		}
		/// <summary>
		/// Indicates the event args used by the awaiter
		/// </summary>
		public SocketAsyncEventArgs EventArgs { get; internal set; }
		/// <summary>
		/// Indicates whether or not the operation is completed
		/// </summary>
		public bool IsCompleted { get; internal set; }

		internal void Reset()
		{
            IsCompleted = false;
            _continuation = null;
		}
		/// <summary>
		/// This method supports the async/await framework
		/// </summary>
		/// <returns>Itself</returns>
		public SocketAwaitable GetAwaiter() { return this; }

		/// <summary>
		/// Checks the result of the socket operation, throwing if unsuccessful
		/// </summary>
		/// <remarks>This is used by the async/await framework</remarks>
		public int GetResult()
		{
			this._continuation = null;

			if (EventArgs.SocketError != SocketError.Success)
				throw new SocketException((int)EventArgs.SocketError);

			return this.EventArgs.BytesTransferred;
		}


		// for INotifyCompletion
		public void OnCompleted(Action continuation)
		{
			if (ReferenceEquals(_continuation, _sentinel) ||
				ReferenceEquals(Interlocked.CompareExchange(ref _continuation,
															continuation,
															null),
								_sentinel))
			{
				Task.Run(continuation);
			}
		}

		public void UnsafeOnCompleted(Action continuation)
		{
			this.OnCompleted(continuation);
		}
	}

	public static class AsyncHelpers
	{
		/// <summary>
		/// Receive data using the specified awaitable class
		/// </summary>
		/// <param name="socket">The socket</param>
		/// <param name="awaitable">An instance of <see cref="SocketAwaitable"/></param>
		/// <returns><paramref name="awaitable"/></returns>
		public static SocketAwaitable ReceiveAsync(this Socket socket,
													SocketAwaitable awaitable)
		{
			awaitable.Reset();
			if (!socket.ReceiveAsync(awaitable.EventArgs))
				awaitable.IsCompleted = true;
			return awaitable;
		}

        /// <summary>
        /// Receive all data into <paramref name="buffer"/> based on <paramref name="offset"/> and <paramref name="length"/> using the specified awaitable class		
        /// </summary>
        /// <param name="socket">The socket</param>
        /// <param name="awaitable">An instance of <see cref="SocketAwaitable"/></param>
        /// <param name="args">The <see cref="SocketAsyncEventArgs"/> instance</param>
        /// <param name="buffer">
        /// The buffer where the returned data is placed.
        /// If null, the buffer provided to <paramref name="args"/> is used and the data will be placed into that buffer based on <paramref name="offset"/> and <paramref name="length"/>.
        /// </param>
        /// <param name="offset">Offset into <paramref name="buffer"/></param>
        /// <param name="length">Length of the expected data</param>
        /// <returns><paramref name="awaitable"/></returns>
        public static SocketAwaitable ReceiveAsync(this Socket socket,
                                                    SocketAwaitable awaitable,
                                                    byte[] buffer, int offset, int length)
        {
			if (buffer is null)
				awaitable.EventArgs.SetBuffer(offset, length);
			else
			{
				awaitable.EventArgs.SetBuffer(buffer, offset, length);
				awaitable.Reset();
			}
            if (!socket.ReceiveAsync(awaitable.EventArgs))
            {
                if (awaitable.EventArgs.SocketError == SocketError.Success)
                {
                    int received = awaitable.EventArgs.BytesTransferred;

                    if (received <= 0)
                    {
                        throw new SocketException((int)SocketError.Shutdown);
                    }

                    if (received < awaitable.EventArgs.Count)
                    {
                        return ReceiveAsync(socket,
                                            awaitable,
                                            null,
                                            awaitable.EventArgs.Offset + received,
                                            awaitable.EventArgs.Count - received);
                    }

                    awaitable.IsCompleted = true;
                }
            }
            return awaitable;
        }


        /// <summary>
        /// Sends data using the specified awaitable class
        /// </summary>
        /// <param name="socket">The socket</param>
        /// <param name="awaitable">An instance of <see cref="SocketAwaitable"/></param>
        /// <returns><paramref name="awaitable"/></returns>
        public static SocketAwaitable SendAsync(this Socket socket,
			SocketAwaitable awaitable)
		{
			awaitable.Reset();
			if (!socket.SendAsync(awaitable.EventArgs))
				awaitable.IsCompleted = true;
			return awaitable;
		}
	}
}
