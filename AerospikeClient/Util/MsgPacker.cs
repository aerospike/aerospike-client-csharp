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
	public sealed class MsgPacker
	{
		//-------------------------------------------------------
		// Pack methods
		//-------------------------------------------------------

		public static byte[] Pack(Aerospike.Client.Value[] val)
		{
	        using (MemoryStream ms = new MemoryStream())
			{
				Packer packer = Packer.Create(ms);
				PackValueArray(packer, val);
				return ms.ToArray();
			}
		}

		public static byte[] Pack(List<object> val)
		{
	        using (MemoryStream ms = new MemoryStream())
			{
				Packer packer = Packer.Create(ms);
				PackList(packer, val);
				return ms.ToArray();
			}
		}

		public static byte[] Pack(Dictionary<object,object> val)
		{
	        using (MemoryStream ms = new MemoryStream())
			{
				Packer packer = Packer.Create(ms);
				PackMap(packer, val);
				return ms.ToArray();
			}
		}

		public static void PackBytes(Packer packer, byte[] val)
		{
			byte[] buf = new byte[val.Length + 1];
			buf[0] = ParticleType.BLOB;
			Array.Copy(val, 0, buf, 1, val.Length);
			packer.PackRaw(buf);
		}

		public static void PackString(Packer packer, string val)
		{
			int size = ByteUtil.EstimateSizeUtf8(val);
			byte[] buf = new byte[size + 1];
			buf[0] = ParticleType.STRING;
			ByteUtil.StringToUtf8(val, buf, 1);
			packer.PackRaw(buf);
		}

		public static void PackValueArray(Packer packer, Aerospike.Client.Value[] values)
		{
			packer.PackArrayHeader(values.Length);
			foreach (Aerospike.Client.Value value in values)
			{
				value.Pack(packer);
			}
		}

		public static void PackList(Packer packer, List<object> list)
		{
			packer.PackArrayHeader(list.Count);
			foreach (object obj in list)
			{
				PackObject(packer, obj);
			}
		}

		public static void PackMap(Packer packer, Dictionary<object,object> map)
		{
			packer.PackMapHeader(map.Count);
			foreach (KeyValuePair<object,object> entry in map)
			{
				PackObject(packer, entry.Key);
				PackObject(packer, entry.Value);
			}
		}

		public static void PackBlob(Packer packer, object val)
		{
            using (MemoryStream ms = new MemoryStream())
            {
                Formatter.Default.Serialize(ms, val);
				byte[] src = ms.ToArray();
				byte[] trg = new byte[src.Length + 1];
				trg[0] = ParticleType.CSHARP_BLOB;
				Array.Copy(src, 0, trg, 1, src.Length);
				packer.PackRaw(trg);
            }
		}

		private static void PackObject(Packer packer, object obj)
		{
			if (obj == null)
			{
				packer.PackNull();
				return;
			}

			if (obj is Aerospike.Client.Value)
			{
				Aerospike.Client.Value value = (Aerospike.Client.Value)obj;
				value.Pack(packer);
				return;
			}

			if (obj is string)
			{
				PackString(packer, (string) obj);
				return;
			}

			if (obj is byte[])
			{
				PackBytes(packer, (byte[]) obj);
				return;
			}

			if (obj is int)
			{
				packer.Pack((int) obj);
				return;
			}

			if (obj is long)
			{
				packer.Pack((long) obj);
				return;
			}

			if (obj is List<object>)
			{
				PackList(packer, (List<object>) obj);
				return;
			}

			if (obj is Dictionary<object,object>)
			{
				PackMap(packer, (Dictionary<object,object>) obj);
				return;
			}

			PackBlob(packer, obj);
		}
	}
}