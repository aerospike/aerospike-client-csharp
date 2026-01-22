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
		/// Create CDT select operation with context.
		/// Equivalent to as_operations_cdt_select in C client.
		/// </summary>
		/// <param name="binName">bin name</param>
		/// <param name="flags">select flags</param>
		/// <param name="ctx">optional path to nested CDT.</param>
		public static Operation SelectByPath(string binName, SelectFlag flags, params CTX[] ctx)
		{
			byte[] packedBytes;
			if (ctx == null || ctx.Length == 0)
			{
				packedBytes = PackUtil.Pack((int)CDT.Type.SELECT, (int)flags);
			}
			else
			{
				packedBytes = PackCDTSelect(flags, CDT.Type.SELECT, ctx);
			}

			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(packedBytes));
		}

		/// <summary>
		/// Create CDT apply operation with context and modify expression.
		/// Equivalent to as_operations_cdt_apply in C client.
		/// </summary>
		/// <param name="binName">bin name</param>
		/// <param name="flags">apply flags</param>
		/// <param name="modifyExp">modify expression</param>
		/// <param name="ctx">optional path to nested CDT.</param>
		public static Operation ModifyByPath(string binName, ModifyFlag flags, Expression modifyExp, params CTX[] ctx)
		{
			byte[] packedBytes;
			if (ctx == null || ctx.Length == 0)
			{
				packedBytes = PackUtil.Pack((int)CDT.Type.SELECT, (int)flags, modifyExp);
			}
			else
			{
				packedBytes = PackCDTModify(flags, CDT.Type.SELECT, modifyExp, ctx);
			}

			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packedBytes));
		}

		private static byte[] PackCDTSelect(SelectFlag flags, CDT.Type typeSelect, params CTX[] ctx)
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

			// Ensure the apply flag is cleared, since no expression is provided.
			// This avoids problems if the caller accidentally sets bit 2 in the flags field.
			packer.PackNumber((int)flags & ~4);

			return packer.ToByteArray();
		}

		private static byte[] PackCDTModify(ModifyFlag flags, CDT.Type type, Expression modifyExp, params CTX[] ctx)
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
