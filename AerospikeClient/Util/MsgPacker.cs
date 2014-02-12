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

		public static void PackBytes(Packer packer, byte[] val, int offset, int length)
		{
			byte[] buf = new byte[length + 1];
			buf[0] = ParticleType.BLOB;
			Array.Copy(val, offset, buf, 1, length);
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
