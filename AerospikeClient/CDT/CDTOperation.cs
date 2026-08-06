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
	/// <summary>
	/// Nested CDT context.  Identifies the location of nested list/map to apply the operation.
	/// for the current level.  An array of CTX identifies location of the list/map on multiple
	/// levels on nesting.
	/// </summary>
	public sealed class CDTOperation
	{
		/// <summary>
		/// Create a CDT select operation that traverses a nested CDT structure and returns
		/// matching values.
		/// </summary>
		/// <remarks>
		/// <para>The result type depends on <paramref name="flags"/>:</para>
		/// <list type="bullet">
		/// <item><see cref="SelectFlag.VALUE"/> returns a list of leaf values.</item>
		/// <item><see cref="SelectFlag.MATCHING_TREE"/> returns the matching nested structure.</item>
		/// <item><see cref="SelectFlag.MAP_KEY"/> returns a list of map keys.</item>
		/// <item><see cref="SelectFlag.MAP_KEY_VALUE"/> returns a list of map key-value pairs.</item>
		/// </list>
		/// <para>
		/// Flags may be combined with the bitwise OR operator. For example,
		/// <c>SelectFlag.VALUE | SelectFlag.NO_FAIL</c>.
		/// </para>
		/// <para>
		/// The context path may use methods such as <see cref="CTX.MapKey(Value)"/>,
		/// <see cref="CTX.AllChildren()"/>, and <see cref="CTX.AllChildrenWithFilter(Exp)"/>.
		/// A null or empty path operates on the top-level bin value.
		/// </para>
		/// </remarks>
		/// <param name="binName">Bin name.</param>
		/// <param name="flags">Flags that control the selected data and result shape.</param>
		/// <param name="ctx">Optional path to the nested CDT.</param>
		/// <returns>A CDT read operation.</returns>
		/// <exception cref="AerospikeException">
		/// <paramref name="binName"/> or <paramref name="flags"/> is invalid.
		/// </exception>
		public static Operation SelectByPath(string binName, SelectFlag flags, params CTX[] ctx)
		{
			if (string.IsNullOrEmpty(binName) || binName.Length > Bin.MaxBinNameLength)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					$"binName cannot be null, empty, or exceed {Bin.MaxBinNameLength} characters");
			}
			ValidateFlags((int)flags, "select");

			byte[] packedBytes;
			if (ctx == null || ctx.Length == 0)
			{
				packedBytes = PackUtil.Pack((int)CDT.Type.SELECT, (int)flags);
			}
			else
			{
				packedBytes = PackCDTSelect(CDT.Type.SELECT, flags, ctx);
			}

			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(packedBytes));
		}

		/// <summary>
		/// Create a CDT modify operation that traverses a nested CDT structure and applies
		/// a modification expression at each matching location. The operation writes the
		/// modified CDT structure back to the bin.
		/// </summary>
		/// <remarks>
		/// <para>
		/// <paramref name="modifyExp"/> is a compiled expression created by
		/// <see cref="Exp.Build(Exp)"/>. It can reference the current value through loop
		/// variable expressions such as <see cref="Exp.FloatLoopVar(LoopVarPart)"/>.
		/// </para>
		/// <para>
		/// The context path may use methods such as <see cref="CTX.MapKey(Value)"/>,
		/// <see cref="CTX.AllChildren()"/>, and <see cref="CTX.AllChildrenWithFilter(Exp)"/>.
		/// A null or empty path operates on the top-level bin value.
		/// </para>
		/// </remarks>
		/// <param name="binName">Bin name.</param>
		/// <param name="flags">Flags that control modification behavior.</param>
		/// <param name="modifyExp">Compiled expression to apply at each matching location.</param>
		/// <param name="ctx">Optional path to the nested CDT.</param>
		/// <returns>A CDT modify operation.</returns>
		/// <exception cref="AerospikeException">
		/// <paramref name="binName"/> or <paramref name="flags"/> is invalid.
		/// </exception>
		public static Operation ModifyByPath(string binName, ModifyFlag flags, Expression modifyExp, params CTX[] ctx)
		{
			if (string.IsNullOrEmpty(binName) || binName.Length > Bin.MaxBinNameLength)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					$"binName cannot be null, empty, or exceed {Bin.MaxBinNameLength} characters");
			}
			ValidateFlags((int)flags, "modify");

			byte[] packedBytes;
			if (ctx == null || ctx.Length == 0)
			{
				packedBytes = PackUtil.Pack((int)CDT.Type.SELECT, (int)flags, modifyExp);
			}
			else
			{
				packedBytes = PackCDTModify(CDT.Type.SELECT, flags, modifyExp, ctx);
			}

			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packedBytes));
		}

		// Bit 2 is reserved for the internal apply flag.
		private static void ValidateFlags(int flags, string name)
		{
			if (flags < 0 || (flags & 4) != 0)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, $"Invalid {name} flag: {flags}");
			}
		}

		private static byte[] PackCDTSelect(CDT.Type typeSelect, SelectFlag flags, params CTX[] ctx)
		{
			Packer packer = new Packer();

			packer.PackArrayBegin(3);
			packer.PackNumber((int)typeSelect);
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

			packer.PackNumber((int)flags);

			return packer.ToByteArray();
		}

		private static byte[] PackCDTModify(CDT.Type type, ModifyFlag flags, Expression modifyExp, params CTX[] ctx)
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

			packer.PackNumber((int)flags | 4);
			packer.PackByteArray(modifyExp.Bytes, 0, modifyExp.Bytes.Length);

			return packer.ToByteArray();
		}
	}
}
