/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

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

		public static byte[] Pack(IDictionary val)
		{
			Packer packer = new Packer();
			packer.PackMap(val);
			return packer.ToByteArray();
		}

		private byte[] buffer;
		private int offset;
		private List<BufferItem> bufferList;

		public Packer()
		{
			this.buffer = ThreadLocalData.GetBuffer();
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
			PackMapBegin(map.Count);
			foreach (DictionaryEntry entry in map)
			{
				PackObject(entry.Key);
				PackObject(entry.Value);
			}
		}

		private void PackMapBegin(int size)
		{
			if (size < 16)
			{
				PackByte((byte) (0x80 | size));
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
			PackByteArrayBegin(b.Length + 1);
			PackByte(ParticleType.BLOB);
			PackByteArray(b, 0, b.Length);
		}

		public void PackBytes(byte[] b, int offset, int length)
		{
			PackByteArrayBegin(length + 1);
			PackByte(ParticleType.BLOB);
			PackByteArray(b, offset, length);
		}

		public void PackBlob(object val)
		{
#if NETFRAMEWORK
			using (MemoryStream ms = new MemoryStream())
			{
				Formatter.Default.Serialize(ms, val);
				byte[] bytes = ms.ToArray();
				PackByteArrayBegin(bytes.Length + 1);
				PackByte(ParticleType.CSHARP_BLOB);
				PackByteArray(bytes, 0, bytes.Length);
			}
#else
			throw new AerospikeException.Serialize("Binary serialize not supported in .NET core");
#endif
		}

		public void PackGeoJSON(string val)
		{
			byte[] buffer = ByteUtil.StringToUtf8(val);
			PackByteArrayBegin(buffer.Length + 1);
			PackByte(ParticleType.GEOJSON);
			PackByteArray(buffer, 0, buffer.Length);
		}
		
		private void PackByteArrayBegin(int size)
		{
			// Continue to pack byte arrays as strings until all servers/clients
			// have been upgraded to handle new message pack binary type.
			if (size < 32)
			{
				PackByte((byte)(0xa0 | size));
			}
			else if (size < 65536)
			{
				PackShort(0xda, (ushort)size);
			}
			else
			{
				PackInt(0xdb, (uint)size);
			}

			// TODO: Replace with this code after all servers/clients
			// have been upgraded to handle new message pack binary type.
			/*
			if (size < 32)
			{
				PackByte((byte)(0xa0 | size));
			}
			else if (size < 256)
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

		private void PackObject(object obj)
		{
			if (obj == null)
			{
				PackNil();
				return;
			}

			if (obj is Value)
			{
				Value value = (Value) obj;
				value.Pack(this);
				return;
			}

			if (obj is byte[])
			{
				PackBytes((byte[]) obj);
				return;
			}

			if (obj is string)
			{
				PackString((string) obj);
				return;
			}

			if (obj is int)
			{
				PackNumber((int) obj);
				return;
			}

			if (obj is long)
			{
				PackNumber((long) obj);
				return;
			}

			if (obj is double)
			{
				PackDouble((double)obj);
				return;
			}

			if (obj is float)
			{
				PackFloat((float)obj);
				return;
			}

			if (obj is IList)
			{
				PackList((IList) obj);
				return;
			}

			if (obj is IDictionary)
			{
				PackMap((IDictionary) obj);
				return;
			}

			PackBlob(obj);
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

				if (val >= byte.MinValue)
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
			int size = ByteUtil.EstimateSizeUtf8(val) + 1;

			if (size < 32)
			{
				PackByte((byte)(0xa0 | size));
			}
			// TODO: Enable this code after all servers/clients
			// have been upgraded to handle 8 bit string length format.
			/*
			else if (size < 256)
			{
				PackByte(0xd9, (byte)size);
			}
			*/
			else if (size < 65536)
			{
				PackShort(0xda, (ushort)size);
			}
			else
			{
				PackInt(0xdb, (uint)size);
			} 

			if (offset + size > buffer.Length)
			{
				Resize(size);
			}
			buffer[offset++] = (byte)ParticleType.STRING;
			offset += ByteUtil.StringToUtf8(val, buffer, offset);
		}

		private void PackByteArray(byte[] src, int srcOffset, int srcLength)
		{
			if (offset + srcLength > buffer.Length)
			{
				Resize(srcLength);
			}
			Array.Copy(src, srcOffset, buffer, offset, srcLength);
			offset += srcLength;
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
