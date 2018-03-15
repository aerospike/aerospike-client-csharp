/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
				return GetList(new List<object>(0));
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
				return GetList(new List<object>(0));
			}
			return UnpackList(count);
		}

		private object UnpackList(int count)
		{
			if (count <= 0)
			{
				return GetList(new List<object>(0));
			}

			// Extract first object.
			int mark = offset;
			int size = count;
			object val = UnpackObject();
		
			if (val == null)
			{
				// Determine if null value is because of an extension type.
				int type = buffer[mark];

				if (type != 0xc0) // not nil type
				{ 
					// Ignore extension type.
					size--;
				}
			}
		
			List<object> list = new List<object>(size);

			if (size == count)
			{
				list.Add(val);
			}
			
			for (int i = 1; i < count; i++)
			{
				list.Add(UnpackObject());
			}
			return GetList(list);
		}

		private object GetList(List<object> list)
		{
			if (lua)
			{
#if NETFRAMEWORK
				return new LuaList(list);
#else
				throw new AerospikeException("Lua not supported in .NET core");
#endif
			}
			return list;
		}

		public object UnpackMap()
		{
			if (length <= 0)
			{
				return GetMap(new Dictionary<object, object>(0));
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
				return GetMap(new Dictionary<object, object>(0));
			}
			return UnpackMap(count);
		}

		private object UnpackMap(int count)
		{
			if (count <= 0)
			{
				return GetMap(new Dictionary<object, object>(0));
			}

			IDictionary<object,object> map = CreateMap(count);

			if (map != null)
			{
				// Dictionary or SortedDictionary			
				for (int i = 0; i < count; i++)
				{
					object key = UnpackObject();
					object val = UnpackObject();

					if (key != null)
					{
						map[key] = val;
					}
				}
				return GetMap(map);
			}
			else
			{
				// Store in list to preserve order.
				List<object> list = new List<object>(count - 1);

				for (int i = 0; i < count; i++)
				{
					object key = UnpackObject();
					object val = UnpackObject();

					if (key != null)
					{
						list.Add(new KeyValuePair<object, object>(key, val));
					}
				}
				return GetList(list);
			}
		}

		private IDictionary<object, object> CreateMap(int count)
		{
			// Peek at buffer to determine map type, but do not advance.
			int type = buffer[offset];

			// Check for extension that the server uses.
			if (type == 0xc7)
			{
				int extensionType = buffer[offset + 1];

				if (extensionType == 0)
				{
					int mapBits = buffer[offset + 2];

					// Extension is a map type.  Determine which one.
					if ((mapBits & (0x04 | 0x08)) != 0)
					{
						// Index/rank range result where order needs to be preserved.
						return null;
					}
					else if ((mapBits & 0x01) != 0)
					{
						// Sorted map
						return new SortedDictionary<object, object>();
					}
				}
			}
			return new Dictionary<object, object>(count);
		}

		private object GetMap(IDictionary<object, object> map)
		{
			if (lua)
			{
#if NETFRAMEWORK
				return new LuaMap(map);
#else
				throw new AerospikeException("Lua not supported in .NET core");
#endif
			}
			return map;
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

				case ParticleType.GEOJSON:
					val = new Value.GeoJSONValue(ByteUtil.Utf8ToString(buffer, offset, count));
					break;
				
				default:
					byte[] dest = new byte[count];
					Array.Copy(buffer, offset, dest, 0, count);

					if (lua)
					{
#if NETFRAMEWORK
						val = new LuaBytes(dest);
#else
						throw new AerospikeException("Lua not supported in .NET core");
#endif
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

		public object UnpackObject()
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

				case 0xc4:
				case 0xd9: // string raw bytes with 8 bit header
				{
					int count = buffer[offset++];
					return UnpackBlob(count);
				}

				case 0xc5:
				case 0xda: // raw bytes with 16 bit header
				{
					int count = ByteUtil.BytesToShort(buffer, offset);
					offset += 2;
					return UnpackBlob(count);
				}

				case 0xc6:
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

				case 0xd4: // Skip over type extension with 1 byte
				{
					offset += 1 + 1;
					return null;
				}

				case 0xd5: // Skip over type extension with 2 bytes
				{
					offset += 1 + 2;
					return null;
				}

				case 0xd6: // Skip over type extension with 4 bytes
				{
					offset += 1 + 4;
					return null;
				}

				case 0xd7: // Skip over type extension with 8 bytes
				{
					offset += 1 + 8;
					return null;
				}

				case 0xd8: // Skip over type extension with 16 bytes
				{
					offset += 1 + 16;
					return null;
				}

				case 0xc7: // Skip over type extension with 8 bit header and bytes
				{
					int count = buffer[offset];
					offset += count + 1 + 1;
					return null;
				}

				case 0xc8: // Skip over type extension with 16 bit header and bytes
				{
					int count = ByteUtil.BytesToShort(buffer, offset);
					offset += count + 1 + 2;
					return null;
				}

				case 0xc9: // Skip over type extension with 32 bit header and bytes
				{
					int count = ByteUtil.BytesToInt(buffer, offset);
					offset += count + 1 + 4;
					return null;
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
