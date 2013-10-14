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

namespace Aerospike.Client
{
	public sealed class AsyncCluster : Cluster
	{
		// ByteBuffer pool used in asynchronous SocketChannel communications.
		private readonly BlockingCollection<byte[]> bufferQueue;

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
			bufferQueue = new BlockingCollection<byte[]>(policy.asyncMaxCommands);
			InitTendThread();
		}

		protected internal override Node CreateNode(NodeValidator nv)
		{
			return new AsyncNode(this, nv);
		}

		public byte[] GetByteBuffer()
		{
			// If buffers available or always accept command, use standard non-blocking poll().
			if (Interlocked.Increment(ref commandsUsed) <= maxCommands || maxCommandAction == MaxCommandAction.ACCEPT)
			{
				byte[] byteBuffer;
				bufferQueue.TryTake(out byteBuffer);
    
				if (byteBuffer != null)
				{
					return byteBuffer;
				}
				return new byte[8192];
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
				return bufferQueue.Take();
			}
			catch (ThreadInterruptedException)
			{
				Interlocked.Decrement(ref commandsUsed);
				throw new AerospikeException("Buffer pool take interrupted.");
			}
		}

		public void PutByteBuffer(byte[] byteBuffer)
		{
			Interlocked.Decrement(ref commandsUsed);
			bufferQueue.TryAdd(byteBuffer);
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