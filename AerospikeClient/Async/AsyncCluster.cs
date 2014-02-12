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
using System.Collections.Concurrent;
using System.Threading;
using System.Net.Sockets;

namespace Aerospike.Client
{
	public sealed class AsyncCluster : Cluster
	{
		// Pool used in asynchronous SocketChannel communications.
		private readonly BlockingCollection<SocketAsyncEventArgs> argsQueue;

		// Contiguous pool of byte buffers.
		private BufferPool bufferPool;

		// Maximum number of concurrent asynchronous commands.
		private readonly int maxCommands;

		// How to handle cases when the asynchronous maximum number of concurrent database commands 
		// have been exceeded.  
		private readonly bool block;

		public AsyncCluster(AsyncClientPolicy policy, Host[] hosts) : base(policy, hosts)
		{
			maxCommands = policy.asyncMaxCommands;
			block = policy.asyncMaxCommandAction == MaxCommandAction.BLOCK;
			argsQueue = new BlockingCollection<SocketAsyncEventArgs>(maxCommands);

			for (int i = 0; i < maxCommands; i++)
			{
				SocketAsyncEventArgs eventArgs = new SocketAsyncEventArgs();
				eventArgs.Completed += AsyncCommand.SocketListener;
				argsQueue.Add(eventArgs);
			}

			bufferPool = new BufferPool();
			InitTendThread();
		}

		protected internal override Node CreateNode(NodeValidator nv)
		{
			return new AsyncNode(this, nv);
		}

		public SocketAsyncEventArgs GetEventArgs()
		{
			if (block)
			{
				// Use blocking retrieve from queue.
				// If queue is empty, wait till an item is available.
				return argsQueue.Take();
			}

			// Use non-blocking retrieve from queue.
			SocketAsyncEventArgs args;
			if (argsQueue.TryTake(out args))
			{
				return args;
			}
			// Queue is empty. Reject command.
			throw new AerospikeException.CommandRejected();
		}

		public void PutEventArgs(SocketAsyncEventArgs args)
		{
			argsQueue.Add(args);
		}

		public byte[] GetNextBuffer(int size)
		{
			lock (bufferPool)
			{
				if (size > bufferPool.bufferSize)
				{
					bufferPool = new BufferPool(maxCommands, size);
				}
				return bufferPool.GetNextBuffer();
			}
		}

		public bool HasBufferChanged(byte[] dataBuffer)
		{
			// Make reference copy because lock is not applied.
			BufferPool temp = bufferPool;
			return dataBuffer.Length != temp.bufferSize;
		}

		public int MaxCommands
		{
			get
			{
				return maxCommands;
			}
		}
	}
}
