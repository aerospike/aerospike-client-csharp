/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Net.Sockets;

namespace Aerospike.Client
{
	public sealed class AsyncCluster : Cluster
	{
		// EventArgs Pool used in asynchronous SocketChannel communications.
		private readonly ArgsQueue argsQueue;

		// Contiguous pool of byte buffers.
		private BufferPool bufferPool;

		// Maximum number of concurrent asynchronous commands.
		private readonly int maxCommands;

		public AsyncCluster(AsyncClientPolicy policy, Host[] hosts) : base(policy, hosts)
		{
			maxCommands = policy.asyncMaxCommands;

			// How to handle cases when the asynchronous maximum number of concurrent database commands 
			// have been exceeded.  
			if (policy.asyncMaxCommandAction == MaxCommandAction.BLOCK)
			{
				argsQueue = new BlockArgsQueue(maxCommands);
			}
			else
			{
				argsQueue = new RejectArgsQueue(maxCommands);
			}

			bufferPool = new BufferPool();
			InitTendThread(policy.failIfNotConnected);
		}

		protected internal override Node CreateNode(NodeValidator nv)
		{
			return new AsyncNode(this, nv);
		}

		public SocketAsyncEventArgs GetEventArgs()
		{
			return argsQueue.GetEventArgs();
		}

		public void PutEventArgs(SocketAsyncEventArgs args)
		{
			argsQueue.PutEventArgs(args);
		}

		public void GetNextBuffer(int size, BufferSegment segment)
		{
			lock (this)
			{
				if (size > bufferPool.bufferSize)
				{
					bufferPool = new BufferPool(maxCommands, size);
				}
				bufferPool.GetNextBuffer(segment);
			}
		}

		public bool HasBufferChanged(BufferSegment segment)
		{
			return bufferPool.bufferSize != segment.size;
		}

		private interface ArgsQueue
		{
			SocketAsyncEventArgs GetEventArgs();
			void PutEventArgs(SocketAsyncEventArgs args);
		}

		private class BlockArgsQueue : ArgsQueue
		{
			// Block until EventArgs becomes available.
			private readonly BlockingCollection<SocketAsyncEventArgs> argsQueue;

			public BlockArgsQueue(int maxCommands)
			{
				argsQueue = new BlockingCollection<SocketAsyncEventArgs>(maxCommands);

				for (int i = 0; i < maxCommands; i++)
				{
					SocketAsyncEventArgs eventArgs = new SocketAsyncEventArgs();
					eventArgs.UserToken = new BufferSegment();
					eventArgs.Completed += AsyncCommand.SocketListener;
					argsQueue.Add(eventArgs);
				}
			}

			public SocketAsyncEventArgs GetEventArgs()
			{
				// Use blocking retrieve from queue.
				// If queue is empty, wait till an item is available.
				return argsQueue.Take();
			}

			public void PutEventArgs(SocketAsyncEventArgs args)
			{
				argsQueue.Add(args);
			}
		}

		private class RejectArgsQueue : ArgsQueue
		{
			// Reject command if EventArgs not available.
			private readonly ConcurrentQueue<SocketAsyncEventArgs> argsQueue;

			public RejectArgsQueue(int maxCommands)
			{
				argsQueue = new ConcurrentQueue<SocketAsyncEventArgs>();

				for (int i = 0; i < maxCommands; i++)
				{
					SocketAsyncEventArgs eventArgs = new SocketAsyncEventArgs();
					eventArgs.UserToken = new BufferSegment();
					eventArgs.Completed += AsyncCommand.SocketListener;
					argsQueue.Enqueue(eventArgs);
				}
			}

			public SocketAsyncEventArgs GetEventArgs()
			{
				// Use non-blocking retrieve from queue.
				SocketAsyncEventArgs args;
				if (argsQueue.TryDequeue(out args))
				{
					return args;
				}
				// Queue is empty. Reject command.
				throw new AerospikeException.CommandRejected();
			}

			public void PutEventArgs(SocketAsyncEventArgs args)
			{
				argsQueue.Enqueue(args);
			}
		}
	}
}
