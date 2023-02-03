/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
		public readonly byte[] buffer;
		public readonly int bufferSize;

		/// <summary>
		/// Construct one large contiguous cached buffer for use in asynchronous socket commands.
		/// Each command will use a segment of this large buffer.
		/// </summary>
		public BufferPool(int maxCommands, int size)
		{
			// Round up buffer size in 8K increments.
			int rem = size % 8192;

			if (rem > 0)
			{
				size += 8192 - rem;
			}
			bufferSize = size;

			// Allocate one large buffer which will likely be placed on LOH (large object heap).
			// This heap is not usually compacted, so pinning and fragmentation becomes less of 
			// an issue.
			buffer = new byte[maxCommands * bufferSize];
		}
	}

	public sealed class BufferSegment
	{
		public readonly byte[] buffer;
		public readonly int index;
		public readonly int offset;
		public readonly int size;

		/// <summary>
		/// Allocate buffer segment from pool.
		/// </summary>
		public BufferSegment(BufferPool pool, int index)
		{
			this.buffer = pool.buffer;
			this.index = index;
			this.offset = pool.bufferSize * index;
			this.size = pool.bufferSize;
		}

		/// <summary>
		/// Allocate buffer segment from heap.
		/// </summary>
		public BufferSegment(int index, int size)
		{
			this.buffer = new byte[size];
			this.index = index;
			this.offset = 0;
			this.size = size;
		}
	}
}
