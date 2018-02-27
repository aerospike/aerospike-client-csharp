/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int v1)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(1);
			packer.PackNumber(v1);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int v1, int v2)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(2);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int v1, int v2, int v3)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(3);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int v1, Value v2)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(2);
			packer.PackNumber(v1);
			v2.Pack(packer);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int v1, Value v2, int v3)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(3);
			packer.PackNumber(v1);
			v2.Pack(packer);
			packer.PackNumber(v3);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int v1, Value v2, int v3, int v4)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(4);
			packer.PackNumber(v1);
			v2.Pack(packer);
			packer.PackNumber(v3);
			packer.PackNumber(v4);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int v1, IList v2)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(2);
			packer.PackNumber(v1);
			packer.PackList(v2);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, Value v1, Value v2, int v3)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(3);
			v1.Pack(packer);
			v2.Pack(packer);
			packer.PackNumber(v3);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateRangeOperation(int command, Operation.Type type, string binName, Value begin, Value end, int returnType)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);

			if (begin == null)
			{
				begin = Value.AsNull;
			}

			if (end == null)
			{
				packer.PackArrayBegin(2);
				packer.PackNumber(returnType);
				begin.Pack(packer);
			}
			else
			{
				packer.PackArrayBegin(3);
				packer.PackNumber(returnType);
				begin.Pack(packer);
				end.Pack(packer);
			}
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}
	}
}
