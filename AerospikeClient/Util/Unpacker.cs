/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
using System.Collections.Generic;
using System.IO;

namespace Aerospike.Client
{
	/// <summary>
	/// De-serialize collection objects using MessagePack format specification:
	/// 
	/// https://github.com/msgpack/msgpack/blob/master/spec.md
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

			int type = buffer[offset++];
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

			if (lua)
			{
				return new LuaList(list);
			}
			return list;
		}

		public object UnpackMap()
		{
			if (length <= 0)
			{
				return new Dictionary<object, object>(0);
			}

			int type = buffer[offset++];
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

				if (key != null)
				{
					dict[key] = val;
				}
			}

			if (lua)
			{
				return new LuaMap(dict);
			}
			return dict;
		}

		private object UnpackBlob(int count)
		{
			int type = buffer[offset++];
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

					if (lua)
					{
						val = new LuaBytes(dest);
					}
					else
					{
						val = dest;
					}
					break;
			}
			offset += count;
			return val;
		}

		private object UnpackObject()
		{
			int type = buffer[offset++];

			switch (type)
			{
				case 0xc0: // nil
				{
					return null;
				}

				case 0xc3: // boolean true
				{
					return true;
				}

				case 0xc2: // boolean false
				{
					return false;
				}

				case 0xca: // float
				{
					float val = ByteUtil.BytesToFloat(buffer, offset);
					offset += 4;
					return val;
				}

				case 0xcb: // double
				{
					double val = ByteUtil.BytesToDouble(buffer, offset);
					offset += 8;
					return val;
				}

				case 0xd0: // signed 8 bit integer
				{
					return (long)(sbyte)(buffer[offset++]);
				}
				
				case 0xcc: // unsigned 8 bit integer
				{
					return (long)(buffer[offset++]);
				}

				case 0xd1: // signed 16 bit integer
				{
					int val = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return (long)(short)val;
				}

				case 0xcd: // unsigned 16 bit integer
				{
					int val = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return (long)val;
				}

				case 0xd2: // signed 32 bit integer
				{
					int val = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return (long)val;
				}

				case 0xce: // unsigned 32 bit integer
				{
					uint val = ByteUtil.BytesToUInt(buffer, offset);
					offset += 4;
					return (long)val;
				}

				case 0xd3: // signed 64 bit integer
				{
					long val = ByteUtil.BytesToLong(buffer, offset);
					offset += 8;
					return val;
				}

				case 0xcf: // unsigned 64 bit integer
				{
					// The contract is to always return long.  
					// The caller can always cast back to ulong.
					long val = ByteUtil.BytesToLong(buffer, offset);
					offset += 8;
					return val;
				}

				case 0xda: // raw bytes with 16 bit header
				{
					int count = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return UnpackBlob(count);
				}

				case 0xdb: // raw bytes with 32 bit header
				{
					// Array length is restricted to positive int values (0 - int.MAX_VALUE).
					int count = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return UnpackBlob(count);
				}

				case 0xdc: // list with 16 bit header
				{
					int count = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return UnpackList(count);
				}

				case 0xdd: // list with 32 bit header
				{
					// List size is restricted to positive int values (0 - int.MAX_VALUE).
					int count = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return UnpackList(count);
				}

				case 0xde: // map with 16 bit header
				{
					int count = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return UnpackMap(count);
				}

				case 0xdf: // map with 32 bit header
				{
					// Map size is restricted to positive int values (0 - int.MAX_VALUE).
					int count = ByteUtil.BytesToInt(buffer, offset);
					offset += 4;
					return UnpackMap(count);
				}

				default:
				{
					if ((type & 0xe0) == 0xa0) // raw bytes with 8 bit combined header
					{
						return UnpackBlob(type & 0x1f);
					}

					if ((type & 0xf0) == 0x80) // map with 8 bit combined header
					{
						return UnpackMap(type & 0x0f);
					}

					if ((type & 0xf0) == 0x90) // list with 8 bit combined header
					{
						return UnpackList(type & 0x0f);
					}

					if (type < 0x80) // 8 bit combined unsigned integer
					{
						return (long)type;
					}

					if (type >= 0xe0) // 8 bit combined signed integer
					{
						return (long)(type - 0xe0 - 32);
					}
					throw new IOException("Unknown unpack type: " + type);
				}
			}
		}
	}
}
