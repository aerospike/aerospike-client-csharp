/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	public sealed class CDT
	{
		[Flags]
		public enum Type
		{
			/// <summary>
			/// Opcode used encode a CDT select and apply operation.
			/// If flag bit 2 is clear, the operation will be interpreted as a select
			/// operation; otherwise, as an apply operation.
			/// </summary>
			SELECT = 0xfe,

			/// <summary>
			/// Opcode used to encode the calling of a virtual operation.
			/// </summary>
			CONTEXT_EVAL = 0xff
		}

		internal static byte[] PackRangeOperation(int command, int returnType, Value begin, Value end, CTX[] ctx)
		{
			Packer packer = new Packer();
			PackUtil.Init(packer, ctx);
			packer.PackArrayBegin((end != null) ? 4 : 3);
			packer.PackNumber(command);
			packer.PackNumber(returnType);

			if (begin != null)
			{
				begin.Pack(packer);
			}
			else
			{
				packer.PackNil();
			}

			if (end != null)
			{
				end.Pack(packer);
			}
			return packer.ToByteArray();
		}

		internal static void Init(Packer packer, CTX[] ctx, int command, int count, int flag)
		{
			packer.PackArrayBegin(3);
			packer.PackNumber((int)Type.CONTEXT_EVAL);
			packer.PackArrayBegin(ctx.Length * 2);

			CTX c;
			int last = ctx.Length - 1;

			for (int i = 0; i < last; i++)
			{
				c = ctx[i];
				packer.PackNumber(c.id);
				c.value.Pack(packer);
			}

			c = ctx[last];
			packer.PackNumber(c.id | flag);
			c.value.Pack(packer);

			packer.PackArrayBegin(count + 1);
			packer.PackNumber(command);
		}
	}
}
