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
	public abstract class MapBase
	{
		protected internal const int SET_TYPE = 64;
		protected internal const int ADD = 65;
		protected internal const int ADD_ITEMS = 66;
		protected internal const int PUT = 67;
		protected internal const int PUT_ITEMS = 68;
		protected internal const int REPLACE = 69;
		protected internal const int REPLACE_ITEMS = 70;
		protected internal const int INCREMENT = 73;
		protected internal const int DECREMENT = 74;
		protected internal const int CLEAR = 75;
		protected internal const int REMOVE_BY_KEY = 76;
		protected internal const int REMOVE_BY_INDEX = 77;
		protected internal const int REMOVE_BY_RANK = 79;
		protected internal const int REMOVE_BY_KEY_LIST = 81;
		protected internal const int REMOVE_BY_VALUE = 82;
		protected internal const int REMOVE_BY_VALUE_LIST = 83;
		protected internal const int REMOVE_BY_KEY_INTERVAL = 84;
		protected internal const int REMOVE_BY_INDEX_RANGE = 85;
		protected internal const int REMOVE_BY_VALUE_INTERVAL = 86;
		protected internal const int REMOVE_BY_RANK_RANGE = 87;
		protected internal const int SIZE = 96;
		protected internal const int GET_BY_KEY = 97;
		protected internal const int GET_BY_INDEX = 98;
		protected internal const int GET_BY_RANK = 100;
		protected internal const int GET_BY_VALUE = 102;
		protected internal const int GET_BY_KEY_INTERVAL = 103;
		protected internal const int GET_BY_INDEX_RANGE = 104;
		protected internal const int GET_BY_VALUE_INTERVAL = 105;
		protected internal const int GET_BY_RANK_RANGE = 106;

		protected internal static Operation SetMapPolicy(string binName, int attributes)
		{
			Packer packer = new Packer();
			packer.PackRawShort(SET_TYPE);
			packer.PackArrayBegin(1);
			packer.PackNumber(attributes);
			return new Operation(Operation.Type.MAP_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreatePut(int command, int attributes, string binName, Value value1, Value value2)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);

			if (command == MapBase.REPLACE)
			{
				// Replace doesn't allow map attributes because it does not create on non-existing key.
				packer.PackArrayBegin(2);
				value1.Pack(packer);
				value2.Pack(packer);
			}
			else
			{
				packer.PackArrayBegin(3);
				value1.Pack(packer);
				value2.Pack(packer);
				packer.PackNumber(attributes);
			}
			return new Operation(Operation.Type.MAP_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, int attributes, string binName, Value value1, Value value2)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(3);
			value1.Pack(packer);
			value2.Pack(packer);
			packer.PackNumber(attributes);
			return new Operation(Operation.Type.MAP_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, IList list, MapReturnType returnType)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(2);
			packer.PackNumber((int)returnType);
			packer.PackList(list);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, Value value, MapReturnType returnType)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(2);
			packer.PackNumber((int)returnType);
			value.Pack(packer);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int index, MapReturnType returnType)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(2);
			packer.PackNumber((int)returnType);
			packer.PackNumber(index);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateOperation(int command, Operation.Type type, string binName, int index, int count, MapReturnType returnType)
		{
			Packer packer = new Packer();
			packer.PackRawShort(command);
			packer.PackArrayBegin(3);
			packer.PackNumber((int)returnType);
			packer.PackNumber(index);
			packer.PackNumber(count);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		protected internal static Operation CreateRangeOperation(int command, Operation.Type type, string binName, Value begin, Value end, MapReturnType returnType)
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
				packer.PackNumber((int)returnType);
				begin.Pack(packer);
			}
			else
			{
				packer.PackArrayBegin(3);
				packer.PackNumber((int)returnType);
				begin.Pack(packer);
				end.Pack(packer);
			}
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}
	}
}

