/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
			argsQueue = new BlockingCollection<SocketAsyncEventArgs>(policy.asyncMaxCommands);

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