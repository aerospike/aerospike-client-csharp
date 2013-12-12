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

		// How to handle cases when the asynchronous maximum number of concurrent database commands 
		// have been exceeded.  
		private readonly MaxCommandAction maxCommandAction;

		// Commands currently used.
		private int commandsUsed;

		// Maximum number of concurrent asynchronous commands.
		private readonly int maxCommands;

		public AsyncCluster(AsyncClientPolicy policy, Host[] hosts) : base(policy, hosts)
		{
			maxCommandAction = policy.asyncMaxCommandAction;
			maxCommands = policy.asyncMaxCommands;
			argsQueue = new BlockingCollection<SocketAsyncEventArgs>(policy.asyncMaxCommands);
			bufferPool = new BufferPool();
			InitTendThread();
		}

		protected internal override Node CreateNode(NodeValidator nv)
		{
			return new AsyncNode(this, nv);
		}

		public SocketAsyncEventArgs GetEventArgs()
		{
			// If buffers available or always accept command, use standard non-blocking poll().
			if (Interlocked.Increment(ref commandsUsed) <= maxCommands || maxCommandAction == MaxCommandAction.ACCEPT)
			{
				SocketAsyncEventArgs args = null;
				argsQueue.TryTake(out args);
				return args;
			}
    
			// Max buffers exceeded.  Reject command if specified.
			if (maxCommandAction == MaxCommandAction.REJECT)
			{
				Interlocked.Decrement(ref commandsUsed);
				throw new AerospikeException.CommandRejected();
			}
    
			// Block until buffer becomes available.
			try
			{
				return argsQueue.Take();
			}
			catch (ThreadInterruptedException)
			{
				Interlocked.Decrement(ref commandsUsed);
				throw new AerospikeException("Buffer pool take interrupted.");
			}
		}

		public void PutEventArgs(SocketAsyncEventArgs args)
		{
			if (!argsQueue.TryAdd(args))
			{
				// Add failed.  Must free resources.
				args.Dispose();
			}

			// Free up slot after add completed.
			Interlocked.Decrement(ref commandsUsed);
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