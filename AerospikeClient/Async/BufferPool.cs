/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
