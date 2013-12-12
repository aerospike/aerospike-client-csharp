/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
namespace Aerospike.Client
{
	public sealed class BufferPool
	{
		public const int BUFFER_CUTOFF = 1024 * 128; // 128 KB

		public readonly byte[][] buffers;
		public readonly int bufferSize;
		public int bufferOffset;

		/// <summary>
		/// Construct near contiguous cached buffers that will be pinned 
		/// (like by asynchronous socket commands).
		/// Since the buffers are closely located and long lived,
		/// memory fragmentation will be greatly reduced.
		/// </summary>
		public BufferPool(int maxCommands, int size)
		{
			// Round up buffer size in 8K increments.
			int rem = size % 8192;

			if (rem > 0)
			{
				size += 8192 - rem;
			}
			this.bufferSize = size;
			this.buffers = new byte[maxCommands][];

			for (int i = 0; i < maxCommands; i++)
			{
				this.buffers[i] = new byte[bufferSize];
			}
		}

		public BufferPool()
		{
		}

		public byte[] GetNextBuffer()
		{
			return buffers[bufferOffset++];
		}
	}
}
