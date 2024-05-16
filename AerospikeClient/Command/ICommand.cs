/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Buffers;
using System.Collections;

namespace Aerospike.Client
{
	public interface ICommand
	{
		public ArrayPool<byte> BufferPool { get; }

		public Task Execute(CancellationToken token);

		public void WriteBuffer();
		public Task ParseResult(IConnection conn, CancellationToken token);
		public bool PrepareRetry(bool timeout);

		public int SizeBuffer(ref byte[] dataBuffer, ref int dataOffset);
		public void SizeBuffer(int size);
		public void End(byte[] dataBuffer, ref int dataOffset);
		public void SetLength(byte[] dataBuffer, ref int dataOffset, int length);
	}
}
