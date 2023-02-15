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
	/// Long value.
	/// </summary>
	public sealed class LongValue : Value<long>
	{
		public LongValue(long value)
		: base(value, ParticleType.INTEGER)
		{
		}

		public override int EstimateSize() => 8;

		public override int Write(byte[] buffer, int offset) => ByteUtil.LongToBytes((ulong)value, buffer, offset);

		public override void Pack(Packer packer) => packer.PackNumber(value);

		public override int GetHashCode() => (int)((ulong)value ^ (ulong)value >> 32);

		public override int ToInteger() => (int)value;

		public override uint ToUnsignedInteger() => (uint)value;

		public override long ToLong() => value;

		public override ulong ToUnsignedLong() => (ulong)value;
	}
}
