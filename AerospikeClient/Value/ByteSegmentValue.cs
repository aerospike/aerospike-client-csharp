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
		/// Byte segment value.
		/// </summary>
		public sealed class ByteSegmentValue : Value, IEquatable<ByteSegmentValue>
		{
			public byte[] Bytes { get; }
			public int Offset { get; }
			public int Length { get; }

			public override int Type { get => ParticleType.BLOB; }

			public override object Object { get => this; }

			public ByteSegmentValue(byte[] bytes, int offset, int length)
			{
				Bytes = bytes;
				Offset = offset;
				Length = length;
			}

			public override int EstimateSize() => Length;

			public override int Write(byte[] buffer, int targetOffset)
			{
				Array.Copy(Bytes, Offset, buffer, targetOffset, Length);
				return Length;
			}

			public override void Pack(Packer packer)
			{
				packer.PackParticleBytes(Bytes, Offset, Length);
			}

			public override string ToString() => ByteUtil.BytesToHexString(Bytes, Offset, Length);

			public override bool Equals(object obj)
			{
				if (obj is ByteSegmentValue bObj) return Equals(bObj);

				return false;
			}

			public bool Equals(ByteSegmentValue other)
			{
				if (other is null) return false;

				if (Length != other.Length) return false;

				for (int i = 0; i < Length; i++)
				{
					if (Bytes[Offset + i] != other.Bytes[other.Offset + i]) return false;
				}

				return true;
			}

			public override int GetHashCode()
			{
				int result = 1;
				for (int i = 0; i < Length; i++)
				{
					result = 31 * result + Bytes[Offset + i];
				}
				return result;
			}

			public static bool operator ==(ByteSegmentValue o1, ByteSegmentValue o2) => o1?.Equals(o2) ?? false;
			public static bool operator !=(ByteSegmentValue o1, ByteSegmentValue o2) => o1 == o2 ? false : true;
		}
	}
}
