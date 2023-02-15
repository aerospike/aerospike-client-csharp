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
	/// HyperLogLog value.
	/// </summary>
	public sealed class HLLValue : Value, IEquatable<HLLValue>, IEquatable<byte[]>
	{
		public byte[] Bytes { get; }

		public override ParticleType Type { get => ParticleType.HLL; }

		public override object Object { get => Bytes; }

		public HLLValue(byte[] bytes)
		{
			this.Bytes = bytes;
		}

		public override int EstimateSize() => Bytes.Length;

		public override int Write(byte[] buffer, int offset)
		{
			Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
			return Bytes.Length;
		}

		public override void Pack(Packer packer) => packer.PackParticleBytes(Bytes);

		public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: HLL");

		public override string ToString() => ByteUtil.BytesToHexString(Bytes);

		public override bool Equals(object obj)
		{
			if (obj is byte[] bValue) return Equals(bValue);
			if (obj is HLLValue hValue) return Equals(hValue);

			return false;
		}

		public bool Equals(HLLValue other) => other is null || other.Bytes is null ? false : Util.ByteArrayEquals(Bytes, other.Bytes);

		public bool Equals(byte[] other) => other is null ? false : Util.ByteArrayEquals(Bytes, other);

		public override int GetHashCode()
		{
			int result = 1;
			foreach (byte b in Bytes)
			{
				result = 31 * result + b;
			}
			return result;
		}

		public static bool operator ==(HLLValue o1, HLLValue o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(HLLValue o1, HLLValue o2) => o1 == o2 ? false : true;

		public static bool operator ==(HLLValue o1, byte[] o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(HLLValue o1, byte[] o2) => o1 == o2 ? false : true;
	}
}
