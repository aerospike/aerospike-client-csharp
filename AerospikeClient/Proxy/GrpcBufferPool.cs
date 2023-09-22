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
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// 
	/// </summary>
	public sealed class GrpcBufferPool
	{
		private readonly ConcurrentQueue<BufferSegment> bufferQueue = new ConcurrentQueue<BufferSegment>();

		public GrpcBufferPool(AsyncClientPolicy policy, BufferPool pool)
		{
			for (int i = 0; i < policy.asyncMaxCommands; i++)
			{
				bufferQueue.Enqueue(new BufferSegment(pool, i));
			}
		}

		/// <summary>
		/// Schedule command for execution.
		/// </summary>
		public BufferSegment Get()
		{
			if (bufferQueue.TryDequeue(out var buffer))
			{
				return buffer;
			}
			return null;
		}

		/// <summary>
		/// Release command slot.
		/// </summary>
		public void Release(BufferSegment segment)
		{
			bufferQueue.Enqueue(segment);
		}
	}
}
