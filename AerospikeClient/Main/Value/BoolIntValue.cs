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
	/// Boolean value that converts to integer when sending a bin to the server.
	/// This class will be deleted once full conversion to boolean particle type
	/// is complete.
	/// </summary>
	public sealed class BoolIntValue : Value<bool>, IEquatable<BoolIntValue>
	{
		public BoolIntValue(bool value)
			: base(value, ParticleType.INTEGER)
		{
		}

		public override int EstimateSize() => 8;

		public override int Write(byte[] buffer, int offset) => ByteUtil.LongToBytes(value ? 1UL : 0UL, buffer, offset);

		public override void Pack(Packer packer) => packer.PackBoolean(value);

		public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: BoolIntValue");

		public override bool Equals(object obj)
		{
			if (obj is BoolIntValue bObj) return this.Equals(bObj);

			return false;
		}

		public bool Equals(BoolIntValue other) => this.value.Equals(other);

		public override int GetHashCode() => value ? 1231 : 1237;

		public override int ToInteger() => value ? 1 : 0;

		public override uint ToUnsignedInteger() => value ? 1 : (uint)0;

		public override long ToLong() => value ? 1 : 0;

		public override ulong ToUnsignedLong() => value ? 1 : (ulong)0;

		public static bool operator ==(BoolIntValue o1, BoolIntValue o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(BoolIntValue o1, BoolIntValue o2) => o1 == o2 ? false : true;

		public static bool operator ==(BoolIntValue o1, bool o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(BoolIntValue o1, bool o2) => o1 == o2 ? false : true;
	}
}
