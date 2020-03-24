/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using System;
using System.Collections;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public class CDT
	{
		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 0);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx, int v1)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 1);
			packer.PackNumber(v1);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx, int v1, int v2)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 2);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx, int v1, int v2, int v3)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 3);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx, int v1, Value v2)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 2);
			packer.PackNumber(v1);
			v2.Pack(packer);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx, int v1, Value v2, int v3)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 3);
			packer.PackNumber(v1);
			v2.Pack(packer);
			packer.PackNumber(v3);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx, int v1, Value v2, int v3, int v4)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 4);
			packer.PackNumber(v1);
			v2.Pack(packer);
			packer.PackNumber(v3);
			packer.PackNumber(v4);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx, int v1, IList v2)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 2);
			packer.PackNumber(v1);
			packer.PackList(v2);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, CTX[] ctx, Value v1, Value v2, int v3)
		{
			Packer packer = new Packer();
			CDT.Init(packer, ctx, command, 3);
			v1.Pack(packer);
			v2.Pack(packer);
			packer.PackNumber(v3);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateRangeOperation(int command, Operation.Type type, string binName, CTX[] ctx, Value begin, Value end, int returnType)
		{
			Packer packer = new Packer();

			if (begin == null)
			{
				begin = Value.AsNull;
			}

			if (end == null)
			{
				CDT.Init(packer, ctx, command, 2);
				packer.PackNumber(returnType);
				begin.Pack(packer);
			}
			else
			{
				CDT.Init(packer, ctx, command, 3);
				packer.PackNumber(returnType);
				begin.Pack(packer);
				end.Pack(packer);
			}
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		internal static void Init(Packer packer, CTX[] ctx, int command, int count)
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
				packer.PackArrayBegin(count + 1);
				packer.PackNumber(command);
			}
			else
			{
				packer.PackRawShort(command);

				if (count > 0)
				{
					packer.PackArrayBegin(count);
				}
			}
		}

		internal static void Init(Packer packer, CTX[] ctx, int command, int count, int flag)
		{
			packer.PackArrayBegin(3);
			packer.PackNumber(0xff);
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
