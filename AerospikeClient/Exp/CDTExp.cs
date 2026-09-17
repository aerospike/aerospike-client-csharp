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
		private const int MODIFY = 0x40;

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
		/// Create a CDT select expression that traverses a nested CDT structure and returns
		/// matching values.
		/// </summary>
		/// <remarks>
		/// <para>
		/// <paramref name="returnType"/> should match the expected result shape. Use
		/// <see cref="Exp.Type.LIST"/> with <see cref="SelectFlag.VALUE"/>,
		/// <see cref="SelectFlag.MAP_KEY"/>, or <see cref="SelectFlag.MAP_KEY_VALUE"/>.
		/// Use <see cref="Exp.Type.MAP"/> with <see cref="SelectFlag.MATCHING_TREE"/> on a map bin.
		/// </para>
		/// <para>
		/// Flags may be combined with the bitwise OR operator. For example,
		/// <c>SelectFlag.VALUE | SelectFlag.NO_FAIL</c>.
		/// </para>
		/// <para>
		/// The context path may use methods such as <see cref="CTX.MapKey(Value)"/>,
		/// <see cref="CTX.AllChildren()"/>, and <see cref="CTX.AllChildrenWithFilter(Exp)"/>.
		/// </para>
		/// </remarks>
		/// <param name="returnType">Expected result type.</param>
		/// <param name="flags">Flags that control the selected data and result shape.</param>
		/// <param name="bin">Source bin expression, such as <see cref="Exp.MapBin(string)"/>.</param>
		/// <param name="ctx">Path to the nested CDT.</param>
		/// <returns>An expression that evaluates to <paramref name="returnType"/>.</returns>
		/// <exception cref="AerospikeException"><paramref name="flags"/> is invalid.</exception>
		public static Exp SelectByPath(Exp.Type returnType, SelectFlag flags, Exp bin, params CTX[] ctx)
		{
			ValidateFlags((int)flags, "select");
			byte[] bytes = PackCDTSelect(Type.SELECT, flags, ctx);

			return new Exp.Module(bin, bytes, (int)returnType, MODULE);
		}

		/// <summary>
		/// Create a CDT modify expression that traverses a nested CDT structure and applies
		/// a modification expression at each matching location.
		/// </summary>
		/// <remarks>
		/// <para>
		/// <paramref name="returnType"/> should match the top-level type of the modified
		/// bin, typically <see cref="Exp.Type.MAP"/> or <see cref="Exp.Type.LIST"/>.
		/// </para>
		/// <para>
		/// <paramref name="modifyExp"/> can reference the current value through loop
		/// variable expressions such as <see cref="Exp.FloatLoopVar(LoopVarPart)"/>.
		/// The context path may use methods such as <see cref="CTX.MapKey(Value)"/>,
		/// <see cref="CTX.AllChildren()"/>, and <see cref="CTX.AllChildrenWithFilter(Exp)"/>.
		/// </para>
		/// <para>
		/// To remove elements selected by the path context, use <see cref="Exp.RemoveResult()"/> as
		/// <paramref name="modifyExp"/>. If the path matches one element, one element is removed;
		/// if it matches multiple elements, all matches are removed.
		/// </para>
		/// </remarks>
		/// <param name="returnType">Expected result type.</param>
		/// <param name="modifyFlag">Flags that control modification behavior.</param>
		/// <param name="modifyExp">Expression to apply at each matching location.</param>
		/// <param name="bin">Source bin expression, such as <see cref="Exp.MapBin(string)"/>.</param>
		/// <param name="ctx">Path to the nested CDT.</param>
		/// <returns>An expression containing the entire modified CDT structure.</returns>
		/// <exception cref="AerospikeException"><paramref name="modifyFlag"/> is invalid.</exception>
		public static Exp ModifyByPath(Exp.Type returnType, ModifyFlag modifyFlag, Exp modifyExp, Exp bin, params CTX[] ctx)
		{
			ValidateFlags((int)modifyFlag, "modify");
			byte[] bytes = PackCDTModify(Type.SELECT, modifyFlag, modifyExp, ctx);

			return new Exp.Module(bin, bytes, (int)returnType, MODULE | MODIFY);
		}

		// Bit 2 is reserved for the internal apply flag.
		private static void ValidateFlags(int flags, string name)
		{
			if (flags < 0 || (flags & 4) != 0)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, $"Invalid {name} flag: {flags}");
			}
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
