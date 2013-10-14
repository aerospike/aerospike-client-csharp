/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
