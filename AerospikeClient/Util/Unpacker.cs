/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System;
using System.Collections.Generic;
using System.IO;

namespace Aerospike.Client
{
	/// <summary>
	/// De-serialize collection objects using MessagePack format specification:
	/// 
	/// http://wiki.msgpack.org/display/MSGPACK/Format+specification#Formatspecification-int32
	/// </summary>
	public sealed class Unpacker
	{
		private readonly byte[] buffer;
		private int offset;
		private readonly int length;
		private readonly bool lua;

		public Unpacker(byte[] buffer, int offset, int length, bool lua)
		{
			this.buffer = buffer;
			this.offset = offset;
			this.length = length;
			this.lua = lua;
		}

		public object UnpackList()
		{
			if (length <= 0)
			{
				return new List<object>(0);
			}

			int type = buffer[offset++] & 0xff;
			int count;

			if ((type & 0xf0) == 0x90)
			{
				count = type & 0x0f;
			}
			else if (type == 0xdc)
			{
				count = ByteUtil.BytesToShort(buffer, offset);
				offset += 2;
			}
			else if (type == 0xdd)
			{
				count = ByteUtil.BytesToInt(buffer, offset);
				offset += 4;
			}
			else
			{
				return new List<object>(0);
			}
			return UnpackList(count);
		}

		private object UnpackList(int count)
		{
			List<object> list = new List<object>();

			for (int i = 0; i < count; i++)
			{
				list.Add(UnpackObject());
			}

			#if (! LITE)
			if (lua)
			{
				return new LuaList(list);
			}
			#endif
			return list;
		}

		public object UnpackMap()
		{
			if (length <= 0)
			{
				return new Dictionary<object, object>(0);
			}

			int type = buffer[offset++] & 0xff;
			int count;

			if ((type & 0xf0) == 0x80)
			{
				count = type & 0x0f;
			}
			else if (type == 0xde)
			{
				count = ByteUtil.BytesToShort(buffer, offset);
				offset += 2;
			}
			else if (type == 0xdf)
			{
				count = ByteUtil.BytesToInt(buffer, offset);
				offset += 4;
			}
			else
			{
				return new Dictionary<object, object>(0);
			}
			return UnpackMap(count);
		}

		private object UnpackMap(int count)
		{
			Dictionary<object, object> dict = new Dictionary<object, object>();

			for (int i = 0; i < count; i++)
			{
				object key = UnpackObject();
				object val = UnpackObject();
				dict[key] = val;
			}

			#if (! LITE)
			if (lua)
			{
				return new LuaMap(dict);
			}
			#endif
			return dict;
		}

		private object UnpackBlob(int count)
		{
			int type = buffer[offset++] & 0xff;
			count--;
			object val;

			switch (type)
			{
				case ParticleType.STRING:
					val = ByteUtil.Utf8ToString(buffer, offset, count);
					break;

				case ParticleType.CSHARP_BLOB:
					val = ByteUtil.BytesToObject(buffer, offset, count);
					break;

				default:
					byte[] dest = new byte[count];
					Array.Copy(buffer, offset, dest, 0, count);

					#if (! LITE)
					if (lua)
					{
						val = new LuaBytes(dest);
					}
					else
					{
						val = dest;
					}
					#else
						val = dest;
					#endif
					break;
			}
			offset += count;
			return val;
		}

		private object UnpackObject()
		{
			int type = buffer[offset++] & 0xff;

			switch (type)
			{
				case 0xc0:
				{
					return null;
				}

				case 0xc3:
				{
					return true;
				}

				case 0xc2:
				{
					return false;
				}

				case 0xca:
				{
					float val = ByteUtil.BytesToFloat(buffer, offset);
					offset += 4;
					return val;
				}

				case 0xcb:
				{
					double val = ByteUtil.BytesToDouble(buffer, offset);
					offset += 8;
					return val;
				}

				case 0xcc:
				{
					return (long)(buffer[offset++] & 0xff);
				}

				case 0xcd:
				{
					int val = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return (long)val;
				}

				case 0xce:
				{
					int val = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return (long)val;
				}

				case 0xcf:
				{
					long val = ByteUtil.BytesToLong(buffer, offset);
					offset += 8;
					return val;
				}

				case 0xd0:
				{
					return (long)(buffer[offset++]);
				}

				case 0xd1:
				{
					int val = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return (long)val;
				}

				case 0xd2:
				{
					int val = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return (long)val;
				}

				case 0xd3:
				{
					long val = ByteUtil.BytesToLong(buffer, offset);
					offset += 8;
					return val;
				}

				case 0xda:
				{
					int count = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return UnpackBlob(count);
				}

				case 0xdb:
				{
					int count = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return UnpackBlob(count);
				}

				case 0xdc:
				{
					int count = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return UnpackList(count);
				}

				case 0xdd:
				{
					int count = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return UnpackList(count);
				}

				case 0xde:
				{
					int count = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return UnpackMap(count);
				}

				case 0xdf:
				{
					int count = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return UnpackMap(count);
				}

				default:
				{
					if ((type & 0xe0) == 0xa0)
					{
						return UnpackBlob(type & 0x1f);
					}

					if ((type & 0xf0) == 0x80)
					{
						return UnpackMap(type & 0x0f);
					}

					if ((type & 0xf0) == 0x90)
					{
						return UnpackList(type & 0x0f);
					}

					if (type < 0x80)
					{
						return (long)type;
					}

					if (type >= 0xe0)
					{
						return (long)(type - 0xe0 - 32);
					}
					throw new IOException("Unknown unpack type: " + type);
				}
			}
		}
	}
}
