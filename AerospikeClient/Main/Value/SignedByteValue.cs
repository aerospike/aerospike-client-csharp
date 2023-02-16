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
	partial class Value
	{
		/// <summary>
		/// Byte value.
		/// </summary>
		public sealed class SignedByteValue : Value<sbyte>
		{
			public SignedByteValue(sbyte value)
			: base(value, ParticleType.INTEGER)
			{
			}

			public override int EstimateSize() => 8;

			public override int Write(byte[] buffer, int offset) => ByteUtil.LongToBytes((ulong)value, buffer, offset);

			public override void Pack(Packer packer) => packer.PackNumber(value);

			public override int GetHashCode() => value;

			public override int ToInteger() => value;

			public override uint ToUnsignedInteger() => (uint)value;

			public override long ToLong() => value;

			public override ulong ToUnsignedLong() => (ulong)value;
		}
	}
}
