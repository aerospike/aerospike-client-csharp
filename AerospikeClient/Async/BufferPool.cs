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
