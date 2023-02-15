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
	/// Float value.
	/// </summary>
	public sealed class FloatValue : Value<float>
	{
		public FloatValue(float value)
		: base(value, ParticleType.DOUBLE)
		{
		}

		public override int EstimateSize() => 8;

		public override int Write(byte[] buffer, int offset) => ByteUtil.DoubleToBytes(value, buffer, offset);

		public override void Pack(Packer packer) => packer.PackFloat(value);

		public override int GetHashCode()
		{
			ulong bits = (ulong)BitConverter.DoubleToInt64Bits(value);
			return (int)(bits ^ bits >> 32);
		}

		public override int ToInteger() => (int)value;

		public override uint ToUnsignedInteger() => (uint)value;

		public override long ToLong() => (long)value;

		public override ulong ToUnsignedLong() => (ulong)value;
	}
}
