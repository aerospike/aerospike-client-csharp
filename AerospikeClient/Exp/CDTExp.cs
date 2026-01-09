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

namespace Aerospike.Client
{
	public abstract class CDTExp
	{
		/// <summary>
		/// The module identifier for CDT expressions.
		/// </summary>
		private const int MODULE = 0;

		/// <summary>
		/// The modify flag for CDT expressions.
		/// </summary>
		public const int MODIFY = 0x40;

		/// <summary>
		/// The type of CDT expression.
		/// </summary>
		private enum Type
		{
			/// <summary>
			/// The identifier for SELECT CDT expressions.
			/// </summary>
			SELECT = 0xfe,
		}

		/// <summary>
		/// Create a CDT select expression.
		/// </summary>
		/// <param name="returnType"></param>
		/// <param name="flags"></param>
		/// <param name="bin"></param>
		/// <param name="ctx"></param>
		public static Exp SelectByPath(Exp.Type returnType, SelectFlag flags, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackCDTSelect(Type.SELECT, flags, ctx);

			return new Exp.Module(bin, bytes, (int)returnType, MODULE);
		}

		/// <summary>
		/// Create a CDT modify expression.
		/// </summary>
		/// <param name="returnType"></param>
		/// <param name="modifyFlag"></param>
		/// <param name="modifyExp"></param>
		/// <param name="bin"></param>
		/// <param name="ctx"></param>
		public static Exp ModifyByPath(Exp.Type returnType, ModifyFlag modifyFlag, Exp modifyExp, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackCDTModify(Type.SELECT, modifyFlag, modifyExp, ctx);

			return new Exp.Module(bin, bytes, (int)returnType, MODULE | MODIFY);
		}

		private static byte[] PackCDTModify(Type type, ModifyFlag modifyFlags, Exp modifyExp, params CTX[] ctx)
		{
			Packer packer = new Packer();

			packer.PackArrayBegin(4);
			packer.PackNumber((int)type);
			packer.PackArrayBegin(ctx.Length * 2);

			foreach (CTX c in ctx)
			{
				packer.PackNumber(c.id);
				if (c.value != null)
				{
					c.value.Pack(packer);
				}
				else
				{
					packer.PackByteArray(c.exp.Bytes, 0, c.exp.Bytes.Length);
				}
			}

			packer.PackNumber((int)modifyFlags | 4);
			modifyExp.Pack(packer);

			return packer.ToByteArray();
		}

		private static byte[] PackCDTSelect(Type type, SelectFlag selectFlag, params CTX[] ctx)
		{
			Packer packer = new Packer();

			packer.PackArrayBegin(3);
			packer.PackNumber((int)type);
			packer.PackArrayBegin(ctx.Length * 2);

			foreach (CTX c in ctx)
			{
				packer.PackNumber(c.id);
				if (c.value != null)
				{
					c.value.Pack(packer);
				}
				else
				{
					packer.PackByteArray(c.exp.Bytes, 0, c.exp.Bytes.Length);
				}
			}

			packer.PackNumber((int)selectFlag);

			return packer.ToByteArray();
		}
	}
}
