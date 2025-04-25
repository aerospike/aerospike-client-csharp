/*
 * Copyright 2012-2025 Aerospike, Inc.
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

namespace Aerospike.Client;

partial class Value
{
	/// <summary>
	/// <see cref="ReadOnlyMemory{T}"/> value.
	/// </summary>
	internal sealed class ReadOnlyMemoryBytesValue : Value, IEquatable<ReadOnlyMemoryBytesValue>, IEquatable<ReadOnlyMemory<byte>>
	{
		public ReadOnlyMemory<byte> Bytes { get; }

		public override ParticleType Type => ParticleType.BLOB;

		public override object Object => Bytes;

		public ReadOnlyMemoryBytesValue(ReadOnlyMemory<byte> bytes)
		{
			Bytes = bytes;
		}

		public override int EstimateSize() => Bytes.Length;

		public override int Write(byte[] buffer, int offset)
		{
			Bytes.CopyTo(buffer.AsMemory(offset));
			return Bytes.Length;
		}

		public override void Pack(Packer packer) => packer.PackParticleBytes(Bytes);

		public override string ToString() => Convert.ToHexString(Bytes.Span);

		public override int GetHashCode()
		{
			int result = 1;
			foreach (byte b in Bytes.Span)
			{
				result = 31 * result + b;
			}
			return result;
		}

		public override bool Equals(object obj)
		{
			return obj switch
			{
				ReadOnlyMemory<byte> mem => Equals(mem),
				ReadOnlyMemoryBytesValue memValue => Equals(memValue),
				_ => false
			};
		}

		public bool Equals(ReadOnlyMemoryBytesValue other) => other is not null && Equals(other.Bytes);

		public bool Equals(ReadOnlyMemory<byte> other) => Bytes.Span.SequenceEqual(other.Span);

		public static bool operator ==(ReadOnlyMemoryBytesValue o1, ReadOnlyMemoryBytesValue o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(ReadOnlyMemoryBytesValue o1, ReadOnlyMemoryBytesValue o2) => !(o1 == o2);
	}
}