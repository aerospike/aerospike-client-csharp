/* 
 * Copyright 2012-2026 Aerospike, Inc.
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
using System.Collections;

namespace Aerospike.Client
{
	/// <summary>
	/// Serialize collection objects using MessagePack format specification:
	/// 
	/// https://github.com/msgpack/msgpack/blob/master/spec.md
	/// </summary>
	public sealed class Packer
	{
		public static byte[] Pack(Value[] val)
		{
			Packer packer = new Packer();
			packer.PackValueArray(val);
			return packer.ToByteArray();
		}

		public static byte[] Pack(IList val)
		{
			Packer packer = new Packer();
			packer.PackList(val);
			return packer.ToByteArray();
		}

		public static byte[] Pack(IDictionary val, MapOrder order)
		{
			Packer packer = new Packer();
			packer.PackMap(val, order);
			return packer.ToByteArray();
		}

		private byte[] buffer;
		private int offset;
		private List<BufferItem> bufferList;
		private bool sortMaps;

		public Packer()
		{
			this.buffer = ThreadLocalData.GetBuffer();
		}

		private Packer(int initialSize)
		{
			this.buffer = new byte[initialSize];
		}

		/// <summary>
		/// Pack unordered maps at any depth with entries sorted by key in the server's
		/// canonical msgpack order, without adding an order flag ext header. Servers
		/// that include AER-6930 (8.1.2.3+) require map value literals in expressions
		/// to be in canonical form. Default is false.
		/// </summary>
		public void SortMaps(bool sortMaps)
		{
			this.sortMaps = sortMaps;
		}

		public void PackValueArray(Value[] values)
		{
			PackArrayBegin(values.Length);
			foreach (Value value in values)
			{
				value.Pack(this);
			}
		}

		public void PackList(IList list)
		{
			PackArrayBegin(list.Count);
			foreach (object obj in list)
			{
				PackObject(obj);
			}
		}

		public void PackArrayBegin(int size)
		{
			if (size < 16)
			{
				PackByte((byte)(0x90 | size));
			}
			else if (size < 65536)
			{
				PackShort(0xdc, (ushort)size);
			}
			else
			{
				PackInt(0xdd, (uint)size);
			}
		}

		public void PackMap(IDictionary map)
		{
			PackMap(map, MapOrder.UNORDERED);
		}

		public void PackMap(IDictionary map, MapOrder order)
		{
			if (sortMaps && order == MapOrder.UNORDERED && map.Count > 1)
			{
				PackMapCanonical(map);
				return;
			}

			PackMapBegin(map.Count, order);
			foreach (DictionaryEntry entry in map)
			{
				PackObject(entry.Key);
				PackObject(entry.Value);
			}
		}

		private void PackMapCanonical(IDictionary map)
		{
			byte[][] keys = new byte[map.Count][];
			object[] values = new object[map.Count];
			int[] ranks = new int[map.Count];
			int i = 0;

			foreach (DictionaryEntry entry in map)
			{
				// Use a dedicated heap buffer, not new Packer(), which would grab the
				// shared thread-local buffer already in use by this (outer) packer and
				// corrupt its in-progress serialization.
				Packer packer = new(256);
				packer.sortMaps = true;
				packer.PackObject(entry.Key);
				keys[i] = packer.ToByteArray();
				values[i] = entry.Value;
				ranks[i] = i;
				i++;
			}

			CanonicalCompare c0 = new();
			CanonicalCompare c1 = new();

			Array.Sort(ranks, (a, b) =>
			{
				c0.Reset(keys[a]);
				c1.Reset(keys[b]);
				return CanonicalCompare.CompareElement(c0, c1);
			});

			for (i = 1; i < ranks.Length; i++)
			{
				c0.Reset(keys[ranks[i - 1]]);
				c1.Reset(keys[ranks[i]]);

				if (CanonicalCompare.CompareElement(c0, c1) == 0)
				{
					throw new AerospikeException(ResultCode.PARAMETER_ERROR,
						"Map keys pack to duplicate msgpack keys in expression map literal");
				}
			}

			PackMapBegin(map.Count);

			for (i = 0; i < ranks.Length; i++)
			{
				byte[] key = keys[ranks[i]];
				PackByteArray(key, 0, key.Length);
				PackObject(values[ranks[i]]);
			}
		}

		/// <summary>
		/// Compare packed msgpack elements using the same ordering as the server's msgpack_cmp.
		/// </summary>
		private sealed class CanonicalCompare
		{
			private const int TYPE_NIL = 1;
			private const int TYPE_FALSE = 2;
			private const int TYPE_TRUE = 3;
			private const int TYPE_NEGINT = 4;
			private const int TYPE_INT = 5;
			private const int TYPE_STRING = 6;
			private const int TYPE_LIST = 7;
			private const int TYPE_MAP = 8;
			private const int TYPE_BYTES = 9;
			private const int TYPE_DOUBLE = 10;
			private const int TYPE_GEOJSON = 11;
			private const int TYPE_EXT = 12;
			private const int TYPE_WILDCARD = 13;
			private const int TYPE_INF = 14;

			private byte[] buf;
			private int offset;
			private int type;
			private ulong iNum;
			private double dNum;
			private int dataOffset;
			private int dataLen;
			private int count;

			internal void Reset(byte[] buf)
			{
				this.buf = buf;
				this.offset = 0;
			}

			internal static int CompareElement(CanonicalCompare c0, CanonicalCompare c1)
			{
				c0.Parse();
				c1.Parse();

				if (c0.type == TYPE_WILDCARD || c1.type == TYPE_WILDCARD)
				{
					c0.SkipParsed();
					c1.SkipParsed();
					return 0;
				}

				if (c0.type != c1.type)
				{
					return c0.type.CompareTo(c1.type);
				}

				switch (c0.type)
				{
					case TYPE_NEGINT:
					case TYPE_INT:
						return c0.iNum.CompareTo(c1.iNum);

					case TYPE_STRING:
					case TYPE_BYTES:
					case TYPE_GEOJSON:
					case TYPE_EXT:
						return CompareBytes(c0, c1);

					case TYPE_LIST:
						return CompareList(c0, c1);

					case TYPE_MAP:
						return CompareMap(c0, c1);

					case TYPE_DOUBLE:
						if (c0.dNum > c1.dNum)
						{
							return 1;
						}
						if (c0.dNum < c1.dNum)
						{
							return -1;
						}
						return 0;

					default:
						return 0;
				}
			}

			private static int CompareBytes(CanonicalCompare c0, CanonicalCompare c1)
			{
				int len = Math.Min(c0.dataLen, c1.dataLen);

				for (int i = 0; i < len; i++)
				{
					int cmp = c0.buf[c0.dataOffset + i] - c1.buf[c1.dataOffset + i];

					if (cmp != 0)
					{
						return cmp;
					}
				}
				return c0.dataLen.CompareTo(c1.dataLen);
			}

			private static int CompareList(CanonicalCompare c0, CanonicalCompare c1)
			{
				int n0 = c0.count;
				int n1 = c1.count;
				int n = Math.Min(n0, n1);

				for (int i = 0; i < n; i++)
				{
					int cmp = CompareElement(c0, c1);

					if (cmp != 0)
					{
						return cmp;
					}
				}
				return n0.CompareTo(n1);
			}

			private static int CompareMap(CanonicalCompare c0, CanonicalCompare c1)
			{
				if (c0.count != c1.count)
				{
					return c0.count.CompareTo(c1.count);
				}

				int n = c0.count * 2;

				for (int i = 0; i < n; i++)
				{
					int cmp = CompareElement(c0, c1);

					if (cmp != 0)
					{
						return cmp;
					}
				}
				return 0;
			}

			private void SkipParsed()
			{
				int n;

				switch (type)
				{
					case TYPE_LIST:
						n = count;
						break;

					case TYPE_MAP:
						n = count * 2;
						break;

					default:
						return;
				}

				for (int i = 0; i < n; i++)
				{
					Parse();
					SkipParsed();
				}
			}

			private void Parse()
			{
				int b = buf[offset++] & 0xff;

				switch (b)
				{
					case 0xc0:
						type = TYPE_NIL;
						return;
					case 0xc2:
						type = TYPE_FALSE;
						return;
					case 0xc3:
						type = TYPE_TRUE;
						return;

					case 0xcc:
						iNum = buf[offset++];
						type = TYPE_INT;
						return;
					case 0xcd:
						iNum = ReadUint(2);
						type = TYPE_INT;
						return;
					case 0xce:
						iNum = ReadUint(4);
						type = TYPE_INT;
						return;
					case 0xcf:
						iNum = ReadUint(8);
						type = TYPE_INT;
						return;

					case 0xd0:
						SetSigned(ReadSint(1));
						return;
					case 0xd1:
						SetSigned(ReadSint(2));
						return;
					case 0xd2:
						SetSigned(ReadSint(4));
						return;
					case 0xd3:
						SetSigned(ReadSint(8));
						return;

					case 0xca:
						dNum = BitConverter.Int32BitsToSingle((int)ReadUint(4));
						type = TYPE_DOUBLE;
						return;
					case 0xcb:
						dNum = BitConverter.Int64BitsToDouble((long)ReadUint(8));
						type = TYPE_DOUBLE;
						return;

					case 0xc4:
					case 0xd9:
						SetRaw(buf[offset++] & 0xff);
						return;
					case 0xc5:
					case 0xda:
						SetRaw((int)ReadUint(2));
						return;
					case 0xc6:
					case 0xdb:
						SetRaw((int)ReadUint(4));
						return;

					case 0xdc:
						count = (int)ReadUint(2);
						type = TYPE_LIST;
						return;
					case 0xdd:
						count = (int)ReadUint(4);
						type = TYPE_LIST;
						return;
					case 0xde:
						count = (int)ReadUint(2);
						type = TYPE_MAP;
						return;
					case 0xdf:
						count = (int)ReadUint(4);
						type = TYPE_MAP;
						return;

					case 0xd4:
						SetFixExt(1);
						return;
					case 0xd5:
						SetExt(2);
						return;
					case 0xd6:
						SetExt(4);
						return;
					case 0xd7:
						SetExt(8);
						return;
					case 0xd8:
						SetExt(16);
						return;
					case 0xc7:
						SetExt(buf[offset++] & 0xff);
						return;
					case 0xc8:
						SetExt((int)ReadUint(2));
						return;
					case 0xc9:
						SetExt((int)ReadUint(4));
						return;

					default:
						if (b < 0x80)
						{
							iNum = (ulong)b;
							type = TYPE_INT;
							return;
						}

						if (b >= 0xe0)
						{
							SetSigned(unchecked((sbyte)b));
							return;
						}

						if ((b & 0xe0) == 0xa0)
						{
							SetRaw(b & 0x1f);
							return;
						}

						if ((b & 0xf0) == 0x80)
						{
							count = b & 0x0f;
							type = TYPE_MAP;
							return;
						}

						if ((b & 0xf0) == 0x90)
						{
							count = b & 0x0f;
							type = TYPE_LIST;
							return;
						}

						throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Unexpected msgpack header: " + b);
				}
			}

			private void SetSigned(long val)
			{
				iNum = unchecked((ulong)val);
				type = (val < 0) ? TYPE_NEGINT : TYPE_INT;
			}

			private void SetRaw(int len)
			{
				dataOffset = offset;
				dataLen = len;
				offset += len;

				if (len == 0)
				{
					type = TYPE_BYTES;
					return;
				}

				switch ((ParticleType)buf[dataOffset])
				{
					case ParticleType.STRING:
						type = TYPE_STRING;
						return;

					case ParticleType.GEOJSON:
						type = TYPE_GEOJSON;
						return;

					default:
						type = TYPE_BYTES;
						return;
				}
			}

			private void SetFixExt(int len)
			{
				int extType = buf[offset++] & 0xff;

				if (extType == 0xff && len == 1)
				{
					int val = buf[offset] & 0xff;

					if (val == 0x00)
					{
						offset++;
						type = TYPE_WILDCARD;
						return;
					}

					if (val == 0x01)
					{
						offset++;
						type = TYPE_INF;
						return;
					}
				}

				dataOffset = offset;
				dataLen = len;
				offset += len;
				type = TYPE_EXT;
			}

			private void SetExt(int len)
			{
				int extType = buf[offset++] & 0xff;

				if (extType == 0xff && len == 1)
				{
					int val = buf[offset] & 0xff;

					if (val == 0x00)
					{
						offset++;
						type = TYPE_WILDCARD;
						return;
					}

					if (val == 0x01)
					{
						offset++;
						type = TYPE_INF;
						return;
					}
				}

				dataOffset = offset;
				dataLen = len;
				offset += len;
				type = TYPE_EXT;
			}

			private ulong ReadUint(int size)
			{
				ulong val = 0;

				for (int i = 0; i < size; i++)
				{
					val = (val << 8) | buf[offset++];
				}
				return val;
			}

			private long ReadSint(int size)
			{
				long val = unchecked((sbyte)buf[offset++]);

				for (int i = 1; i < size; i++)
				{
					val = (val << 8) | buf[offset++];
				}
				return val;
			}
		}

		private void PackMapBegin(int size, MapOrder order)
		{
			if (order == MapOrder.UNORDERED)
			{
				PackMapBegin(size);
			}
			else
			{
				// Map is sorted.
				PackMapBegin(size + 1);
				PackByte(0xc7);
				PackByte(0);
				PackByte((byte)order);
				PackByte(0xc0);
			}
		}

		private void PackMapBegin(int size)
		{
			if (size < 16)
			{
				PackByte((byte)(0x80 | size));
			}
			else if (size < 65536)
			{
				PackShort(0xde, (ushort)size);
			}
			else
			{
				PackInt(0xdf, (uint)size);
			}
		}

		public void PackBytes(byte[] b)
		{
			PackByteArrayBegin(b.Length);
			PackByteArray(b, 0, b.Length);
		}

		public void PackParticleBytes(byte[] b)
		{
			PackParticleBytes(b.AsMemory());
		}

		public void PackParticleBytes(byte[] b, int offset, int length)
		{
			PackParticleBytes(b.AsMemory(offset, length));
		}

		public void PackParticleBytes(ReadOnlyMemory<byte> b)
		{
			PackParticleBytes(b, ParticleType.BLOB);
		}

		public void PackParticleBytes(byte[] b, ParticleType type)
		{
			PackParticleBytes(b.AsMemory(), type);
		}

		public void PackParticleBytes(ReadOnlyMemory<byte> b, ParticleType type)
		{
			PackByteArrayBegin(b.Length + 1);
			PackByte((byte)type);
			PackByteArray(b);
		}

		public void PackBlob(object val)
		{
			byte[] bytes = Value.BlobValue.Serialize(val);
			PackByteArrayBegin(bytes.Length + 1);
			PackByte((int)ParticleType.CSHARP_BLOB);
			PackByteArray(bytes, 0, bytes.Length);
		}

		public void PackGeoJSON(string val)
		{
			byte[] buffer = ByteUtil.StringToUtf8(val);
			PackByteArrayBegin(buffer.Length + 1);
			PackByte((int)ParticleType.GEOJSON);
			PackByteArray(buffer, 0, buffer.Length);
		}

		private void PackByteArrayBegin(int size)
		{
			// Use string header codes for byte arrays.
			PackStringBegin(size);
			/*
			if (size < 256)
			{
				PackByte(0xc4, (byte)size);
			}
			else if (size < 65536)
			{
				PackShort(0xc5, (ushort)size);
			}
			else
			{
				PackInt(0xc6, (uint)size);
			}
			*/
		}

		internal void PackObject(object obj)
		{
			if (obj == null)
			{
				PackNil();
				return;
			}

			if (obj is byte[])
			{
				PackParticleBytes((byte[])obj);
				return;
			}

			if (obj is Value)
			{
				Value value = (Value)obj;
				value.Pack(this);
				return;
			}

			if (obj is IList)
			{
				PackList((IList)obj);
				return;
			}

			if (obj is IDictionary)
			{
				PackMap((IDictionary)obj);
				return;
			}

			TypeCode code = System.Type.GetTypeCode(obj.GetType());

			switch (code)
			{
				case TypeCode.Empty:
					PackNil();
					break;

				case TypeCode.String:
					PackParticleString((string)obj);
					break;

				case TypeCode.Double:
					PackDouble((double)obj);
					break;

				case TypeCode.Single:
					PackFloat((float)obj);
					break;

				case TypeCode.Int64:
					PackNumber((long)obj);
					break;

				case TypeCode.Int32:
					PackNumber((int)obj);
					break;

				case TypeCode.Int16:
					PackNumber((short)obj);
					break;

				case TypeCode.UInt64:
					PackNumber((ulong)obj);
					break;

				case TypeCode.UInt32:
					PackNumber((uint)obj);
					break;

				case TypeCode.UInt16:
					PackNumber((ushort)obj);
					break;

				case TypeCode.Boolean:
					PackBoolean((bool)obj);
					break;

				case TypeCode.Byte:
					PackNumber((byte)obj);
					break;

				case TypeCode.SByte:
					PackNumber((sbyte)obj);
					break;

				case TypeCode.Char:
				case TypeCode.DateTime:
				case TypeCode.Decimal:
				case TypeCode.Object:
				default:
					PackBlob(obj);
					break;
			}
		}

		public void PackNumber(long val)
		{
			if (val >= 0L)
			{
				if (val < 128L)
				{
					PackByte((byte)val);
					return;
				}

				if (val < 256L)
				{
					PackByte(0xcc, (byte)val);
					return;
				}

				if (val < 65536L)
				{
					PackShort(0xcd, (ushort)val);
					return;
				}

				if (val < 4294967296L)
				{
					PackInt(0xce, (uint)val);
					return;
				}
				PackLong(0xcf, (ulong)val);
			}
			else
			{
				if (val >= -32)
				{
					PackByte((byte)(0xe0 | ((int)val + 32)));
					return;
				}

				if (val >= sbyte.MinValue)
				{
					PackByte(0xd0, (byte)val);
					return;
				}

				if (val >= short.MinValue)
				{
					PackShort(0xd1, (ushort)val);
					return;
				}

				if (val >= int.MinValue)
				{
					PackInt(0xd2, (uint)val);
					return;
				}
				PackLong(0xd3, (ulong)val);
			}
		}

		public void PackNumber(ulong val)
		{
			if (val < 128L)
			{
				PackByte((byte)val);
				return;
			}

			if (val < 256L)
			{
				PackByte(0xcc, (byte)val);
				return;
			}

			if (val < 65536L)
			{
				PackShort(0xcd, (ushort)val);
				return;
			}

			if (val < 4294967296L)
			{
				PackInt(0xce, (uint)val);
				return;
			}
			PackLong(0xcf, val);
		}

		public void PackBoolean(bool val)
		{
			if (val)
			{
				PackByte(0xc3);
			}
			else
			{
				PackByte(0xc2);
			}
		}

		public void PackString(string val)
		{
			int size = ByteUtil.EstimateSizeUtf8(val);
			PackStringBegin(size);

			if (offset + size > buffer.Length)
			{
				Resize(size);
			}
			offset += ByteUtil.StringToUtf8(val, buffer, offset);
		}

		public void PackParticleString(string val)
		{
			int size = ByteUtil.EstimateSizeUtf8(val) + 1;
			PackStringBegin(size);

			if (offset + size > buffer.Length)
			{
				Resize(size);
			}
			buffer[offset++] = (byte)ParticleType.STRING;
			offset += ByteUtil.StringToUtf8(val, buffer, offset);
		}

		private void PackStringBegin(int size)
		{
			if (size < 32)
			{
				PackByte((byte)(0xa0 | size));
			}
			else if (size < 256)
			{
				PackByte(0xd9, (byte)size);
			}
			else if (size < 65536)
			{
				PackShort(0xda, (ushort)size);
			}
			else
			{
				PackInt(0xdb, (uint)size);
			}
		}

		public void PackByteArray(byte[] src, int srcOffset, int srcLength)
		{
			PackByteArray(src.AsMemory(srcOffset, srcLength));
		}

		public void PackByteArray(ReadOnlyMemory<byte> src)
		{
			if (offset + src.Length > buffer.Length)
			{
				Resize(src.Length);
			}

			src.CopyTo(buffer.AsMemory(offset));
			offset += src.Length;
		}

		public void PackDouble(double val)
		{
			if (offset + 9 > buffer.Length)
			{
				Resize(9);
			}
			buffer[offset++] = (byte)0xcb;
			offset += ByteUtil.DoubleToBytes(val, buffer, offset);
		}

		public void PackFloat(float val)
		{
			if (offset + 5 > buffer.Length)
			{
				Resize(5);
			}
			buffer[offset++] = (byte)0xca;
			offset += ByteUtil.FloatToBytes(val, buffer, offset);
		}

		private void PackLong(int type, ulong val)
		{
			if (offset + 9 > buffer.Length)
			{
				Resize(9);
			}
			buffer[offset++] = (byte)type;
			ByteUtil.LongToBytes(val, buffer, offset);
			offset += 8;
		}

		public void PackInt(int type, uint val)
		{
			if (offset + 5 > buffer.Length)
			{
				Resize(5);
			}
			buffer[offset++] = (byte)type;
			ByteUtil.IntToBytes(val, buffer, offset);
			offset += 4;
		}

		private void PackShort(int type, ushort val)
		{
			if (offset + 3 > buffer.Length)
			{
				Resize(3);
			}
			buffer[offset++] = (byte)type;
			ByteUtil.ShortToBytes(val, buffer, offset);
			offset += 2;
		}

		public void PackRawShort(int val)
		{
			// WARNING. This method is not compatible with message pack standard.
			if (offset + 2 > buffer.Length)
			{
				Resize(2);
			}
			ByteUtil.ShortToBytes((ushort)val, buffer, offset);
			offset += 2;
		}

		private void PackByte(int type, byte val)
		{
			if (offset + 2 > buffer.Length)
			{
				Resize(2);
			}
			buffer[offset++] = (byte)type;
			buffer[offset++] = val;
		}

		public void PackNil()
		{
			if (offset >= buffer.Length)
			{
				Resize(1);
			}
			buffer[offset++] = unchecked((byte)0xc0);
		}

		public void PackInfinity()
		{
			if (offset + 3 > buffer.Length)
			{
				Resize(3);
			}
			buffer[offset++] = (byte)0xd4;
			buffer[offset++] = (byte)0xff;
			buffer[offset++] = (byte)0x01;
		}

		public void PackWildcard()
		{
			if (offset + 3 > buffer.Length)
			{
				Resize(3);
			}
			buffer[offset++] = (byte)0xd4;
			buffer[offset++] = (byte)0xff;
			buffer[offset++] = (byte)0x00;
		}

		private void PackByte(byte val)
		{
			if (offset >= buffer.Length)
			{
				Resize(1);
			}
			buffer[offset++] = val;
		}

		private void Resize(int size)
		{
			if (bufferList == null)
			{
				bufferList = new List<BufferItem>();
			}
			bufferList.Add(new BufferItem(buffer, offset));

			if (size < buffer.Length)
			{
				size = buffer.Length;
			}
			buffer = new byte[size];
			offset = 0;
		}

		public byte[] ToByteArray()
		{
			if (bufferList != null)
			{
				int size = offset;
				foreach (BufferItem item in bufferList)
				{
					size += item.length;
				}

				byte[] target = new byte[size];
				size = 0;
				foreach (BufferItem item in bufferList)
				{
					Array.Copy(item.buffer, 0, target, size, item.length);
					size += item.length;
				}

				Array.Copy(buffer, 0, target, size, offset);
				return target;
			}
			else
			{
				byte[] target = new byte[offset];
				Array.Copy(buffer, 0, target, 0, offset);
				return target;
			}
		}

		private sealed class BufferItem
		{
			internal byte[] buffer;
			internal int length;

			internal BufferItem(byte[] buffer, int length)
			{
				this.buffer = buffer;
				this.length = length;
			}
		}
	}
}
