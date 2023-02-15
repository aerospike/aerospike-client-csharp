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
	/// Blob value.
	/// </summary>
	public sealed class BlobValue : Value, IEquatable<BlobValue>, IEquatable<byte[]>
	{
		public override object Object { get; }

		public byte[] Bytes { get; set; }

		public override ParticleType Type { get => ParticleType.CSHARP_BLOB; }

		public BlobValue(object obj)
		{
			Object = obj;
			Bytes = default;
		}

		public override int EstimateSize()
		{
			Bytes = Serialize(Object);
			return Bytes.Length;
		}

		public static byte[] Serialize(object val)
		{
#if BINARY_FORMATTER
			if (DisableSerializer) throw new AerospikeException("Object serializer has been disabled");

			using (MemoryStream ms = new MemoryStream())
			{
				BinaryFormatter formatter = new BinaryFormatter();
				formatter.Serialize(ms, val);
				return ms.ToArray();
			}
#else
			throw new AerospikeException("Object serializer has been disabled");
#endif
		}

		public override int Write(byte[] buffer, int offset)
		{
			Array.Copy(Bytes, 0, buffer, offset, Bytes.Length);
			return Bytes.Length;
		}

		// Do not try to pack bytes field because it will be null
		// when packing objects in a collection (ie. EstimateSize() not called).
		public override void Pack(Packer packer) => packer.PackBlob(Object);

		public override void ValidateKeyType() => throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: csblob");

		public override string ToString() => ByteUtil.BytesToHexString(Bytes);

		public override bool Equals(object obj)
		{
			if (obj is byte[] bValue) return Equals(bValue);
			if (obj is BlobValue blobValue) return Equals(blobValue);

			return false;
		}

		public bool Equals(BlobValue other) => other is null || other.Bytes is null ? false : Util.ByteArrayEquals(Bytes, other.Bytes);

		public bool Equals(byte[] other) => other is null ? false : Util.ByteArrayEquals(Bytes, other);

		public override int GetHashCode() => Object.GetHashCode();

		public static bool operator ==(BlobValue o1, BlobValue o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(BlobValue o1, BlobValue o2) => o1 == o2 ? false : true;

		public static bool operator ==(BlobValue o1, byte[] o2) => o1?.Equals(o2) ?? false;
		public static bool operator !=(BlobValue o1, byte[] o2) => o1 == o2 ? false : true;
	}
}
