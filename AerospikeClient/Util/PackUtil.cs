/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
	public sealed class PackUtil
	{
		public static byte[] Pack(int command, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(1);
			packer.PackNumber(command);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(2);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, int v2, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(3);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, int v2, int v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, int v2, int v3, int v4, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(5);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			packer.PackNumber(v4);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, int v2, long v3, int v4, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(5);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			packer.PackNumber(v4);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, int v2, bool v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackBoolean(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, int v2, byte[] v3, int v4, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(5);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackParticleBytes(v3);
			packer.PackNumber(v4);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, byte[] v2, int v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackParticleBytes(v2);
			packer.PackNumber(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, Value v2, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(3);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			v2.Pack(packer);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, Value v2, int v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			v2.Pack(packer);
			packer.PackNumber(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, Value v2, int v3, int v4, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(5);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			v2.Pack(packer);
			packer.PackNumber(v3);
			packer.PackNumber(v4);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, IList list, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(3);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackList(list);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, IList v2, int v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			packer.PackList(v2);
			packer.PackNumber(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Value value, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(2);
			packer.PackNumber(command);
			value.Pack(packer);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Value value, int v1, int v2, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			value.Pack(packer);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Value v1, Value v2, int v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			v1.Pack(packer);
			v2.Pack(packer);
			packer.PackNumber(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, IList list, int v1, int v2, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			packer.PackList(list);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, IList list, int v1, int v2, int v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(5);
			packer.PackNumber(command);
			packer.PackList(list);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, Exp v2, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(3);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			v2.Pack(packer);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, Exp v2, Exp v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			v2.Pack(packer);
			v3.Pack(packer);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, int v1, Exp v2, Exp v3, Exp v4, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(5);
			packer.PackNumber(command);
			packer.PackNumber(v1);
			v2.Pack(packer);
			v3.Pack(packer);
			v4.Pack(packer);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Exp v1)
		{
			Packer packer = new Packer();
			packer.PackArrayBegin(2);
			packer.PackNumber(command);
			v1.Pack(packer);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Exp v1, int v2, int v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			v1.Pack(packer);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Exp v1, Exp v2, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(3);
			packer.PackNumber(command);
			v1.Pack(packer);
			v2.Pack(packer);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Exp v1, Exp v2, int v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			v1.Pack(packer);
			v2.Pack(packer);
			packer.PackNumber(v3);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Exp v1, Exp v2, int v3, int v4, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(5);
			packer.PackNumber(command);
			v1.Pack(packer);
			v2.Pack(packer);
			packer.PackNumber(v3);
			packer.PackNumber(v4);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Exp v1, Exp v2, Exp v3, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(4);
			packer.PackNumber(command);
			v1.Pack(packer);
			v2.Pack(packer);
			v3.Pack(packer);
			return packer.ToByteArray();
		}

		public static byte[] Pack(int command, Exp v1, Exp v2, Exp v3, int v4, params CTX[] ctx)
		{
			Packer packer = new Packer();
			Init(packer, ctx);
			packer.PackArrayBegin(5);
			packer.PackNumber(command);
			v1.Pack(packer);
			v2.Pack(packer);
			v3.Pack(packer);
			packer.PackNumber(v4);
			return packer.ToByteArray();
		}

		public static void Init(Packer packer, CTX[] ctx)
		{
			if (ctx != null && ctx.Length > 0)
			{
				packer.PackArrayBegin(3);
				packer.PackNumber(0xff);
				packer.PackArrayBegin(ctx.Length * 2);

				foreach (CTX c in ctx)
				{
					packer.PackNumber(c.id);
					c.value.Pack(packer);
				}
			}
		}

		public static byte[] Pack(CTX[] ctx)
		{
			Packer packer = new Packer();
			packer.PackArrayBegin(ctx.Length * 2);

			foreach (CTX c in ctx)
			{
				packer.PackNumber(c.id);
				c.value.Pack(packer);
			}
			return packer.ToByteArray();
		}
	}
}
