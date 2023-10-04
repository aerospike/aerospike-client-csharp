/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
	/// <summary>
	/// Class representing a buffer as a byte array and an offset
	/// </summary>
	public sealed class Buffer
	{
		private const int MAX_BUFFER_SIZE = 1024 * 1024 * 128;  // 128 MB

		public byte[] DataBuffer { get; set; }
		public int Offset;

		public Buffer()
		{
			DataBuffer = null;
			Offset = 0;
		}

		public Buffer(int length)
		{
			DataBuffer = new byte[length];
			Offset = 0;
		}

		public void Resize(int length)
		{
			if (length > MAX_BUFFER_SIZE)
			{
				throw new AerospikeException("Invalid buffer size: " + length);
			}
			DataBuffer = new byte[length];

		}
	}
}
