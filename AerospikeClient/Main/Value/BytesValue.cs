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
	/// Byte array value.
	/// </summary>
	public sealed class BytesValue : Value, IEquatable<BytesValue>, IEquatable<byte[]>
	{
		public byte[] Bytes { get; }

		public override ParticleType Type { get => ParticleType.BLOB; }

		public override object Object { get => Bytes; }

		public BytesValue(byte[] bytes)
		{
			Bytes = bytes;
		}

		public override int EstimateSize() => Bytes.Length;

		public override int Write(byte[] buffer, int offset)
		{
			Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
			return Bytes.Length;
		}

		public override void Pack(Packer packer) => packer.PackParticleBytes(Bytes);

		public override string ToString() => ByteUtil.BytesToHexString(Bytes);

		public override int GetHashCode()
		{
			int result = 1;
			foreach (byte b in Bytes)
			{
				result = 31 * result + b;
			}
			return result;
		}

		public override bool Equals(object obj)
		{
			if (obj is byte[] bValue) return Equals(bValue);
			if (obj is BytesValue bytesValue) return Equals(bytesValue);

			return false;
		}

		public bool Equals(BytesValue other) => other is null || other.Bytes is null ? false : Util.ByteArrayEquals(Bytes, other.Bytes);

		public bool Equals(byte[] other) => other is null ? false : Util.ByteArrayEquals(Bytes, other);

		public static bool operator ==(BytesValue o1, BytesValue o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(BytesValue o1, BytesValue o2) => o1 == o2 ? false : true;

		public static bool operator ==(BytesValue o1, string o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(BytesValue o1, string o2) => o1 == o2 ? false : true;
	}
}
