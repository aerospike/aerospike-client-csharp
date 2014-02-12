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
using System.IO;
using System.Collections.Generic;
using System.Runtime.Serialization;
using MsgPack;

namespace Aerospike.Client
{
	public sealed class MsgUnpacker
	{
		private bool lua;

		public MsgUnpacker(bool lua)
		{
			this.lua = lua;
		}

		public object ParseList(byte[] buf, int offset, int len)
		{
			if (len <= 0)
			{
				return new List<object>(0);
			}

			using (MemoryStream ms = new MemoryStream(buf, offset, len))
			{
				Unpacker unpacker = Unpacker.Create(ms);
				unpacker.Read();

				if (!unpacker.IsArrayHeader)
				{
					throw new AerospikeException.Serialize("Failed to deserialize list.");
				}
				return UnpackList(unpacker);
			}
		}

		public object ParseMap(byte[] buf, int offset, int len)
		{
			if (len <= 0)
			{
				return new Dictionary<object, object>(0);
			}

			using (MemoryStream ms = new MemoryStream(buf, offset, len))
			{
				Unpacker unpacker = Unpacker.Create(ms);
				unpacker.Read();

				if (!unpacker.IsMapHeader)
				{
					throw new AerospikeException.Serialize("Failed to deserialize map.");
				}
				return UnpackMap(unpacker);
			}
		}

		private object UnpackList(Unpacker unpacker)
		{
			List<object> trgList = new List<object>();
			uint length = (uint)unpacker.Data.Value;

			for (int i = 0; i < length; i++)
			{
				trgList.Add(UnpackObject(unpacker));
			}

			if (lua)
			{
				return new LuaList(trgList);
			}
			return trgList;
		}

		private object UnpackMap(Unpacker unpacker)
		{
			Dictionary<object, object> map = new Dictionary<object, object>();
			uint length = (uint)unpacker.Data.Value;

			for (int i = 0; i < length; i++)
			{
				object key = UnpackObject(unpacker);
				object val = UnpackObject(unpacker);
				map[key] = val;
			}

			if (lua)
			{
				return new LuaMap(map);
			}
			return map;
		}

		private object UnpackObject(Unpacker unpacker)
		{
			unpacker.Read();

			if (unpacker.IsArrayHeader)
			{
				return UnpackList(unpacker);
			}

			if (unpacker.IsMapHeader)
			{
				return UnpackMap(unpacker);
			}

			MessagePackObject mpo = unpacker.Data.Value;

			if (mpo.IsRaw)
			{
				return UnpackBlob(mpo);
			}
			return mpo.ToObject();
		}

		private object UnpackBlob(MessagePackObject mpo)
		{
			byte[] bytes = mpo.AsBinary();

			switch (bytes[0])
			{
				case ParticleType.STRING:
					return ByteUtil.Utf8ToString(bytes, 1, bytes.Length - 1);

				case ParticleType.CSHARP_BLOB:
					return ByteUtil.BytesToObject(bytes, 1, bytes.Length - 1);

				case ParticleType.BLOB:
				default:
					byte[] trg = new byte[bytes.Length - 1];
					Array.Copy(bytes, 1, trg, 0, bytes.Length - 1);

					if (lua)
					{
						return new LuaBytes(trg);
					}
					return trg;
			}
		}
	}
}
