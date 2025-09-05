/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
	/// Database operation definition.  The class is used in client's operate() method. 
	/// </summary>
	public sealed class Operation
	{
		/// <summary>
		/// Create read bin database operation.
		/// </summary>
		public static Operation Get(string binName)
		{
			return new Operation(Type.READ, binName);
		}

		/// <summary>
		/// Create read all record bins database operation.
		/// </summary>
		public static Operation Get()
		{
			return new Operation(Type.READ);
		}

		/// <summary>
		/// Create read record header database operation.
		/// </summary>
		public static Operation GetHeader()
		{
			return new Operation(Type.READ_HEADER);
		}

		/// <summary>
		/// Create set database operation.
		/// </summary>
		public static Operation Put(Bin bin)
		{
			return new Operation(Type.WRITE, bin.name, bin.value);
		}

		/// <summary>
		/// Create string append database operation.
		/// </summary>
		public static Operation Append(Bin bin)
		{
			return new Operation(Type.APPEND, bin.name, bin.value);
		}

		/// <summary>
		/// Create string prepend database operation.
		/// </summary>
		public static Operation Prepend(Bin bin)
		{
			return new Operation(Type.PREPEND, bin.name, bin.value);
		}

		/// <summary>
		/// Create integer/double add database operation.
		/// </summary>
		public static Operation Add(Bin bin)
		{
			return new Operation(Type.ADD, bin.name, bin.value);
		}

		/// <summary>
		/// Create touch record database operation.
		/// </summary>
		public static Operation Touch()
		{
			return new Operation(Type.TOUCH);
		}

		/// <summary>
		/// Create delete record database operation.
		/// </summary>
		public static Operation Delete()
		{
			return new Operation(Type.DELETE);
		}

		/// <summary>
		/// Create array of operations from varargs. This method can be useful when
		/// its important to save identical array pointer references. Using varargs
		/// directly always generates new references.
		/// </summary>
		public static Operation[] Array(params Operation[] ops)
		{
			return ops;
		}

		public enum Type
		{
			READ,
			READ_HEADER,
			WRITE,
			CDT_READ,
			CDT_MODIFY,
			MAP_READ,
			MAP_MODIFY,
			ADD,
			EXP_READ,
			EXP_MODIFY,
			APPEND,
			PREPEND,
			TOUCH,
			BIT_READ,
			BIT_MODIFY,
			DELETE,
			HLL_READ,
			HLL_MODIFY
		}

		private static readonly byte[] ProtocolTypes = new byte[] { 1, 1, 2, 3, 4, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

		private static readonly bool[] IsWrites = new bool[]
		{
			false,
			false,
			true,
			false,
			true,
			false,
			true,
			true,
			false,
			true,
			true,
			true,
			true,
			false,
			true,
			true,
			false,
			true
		};

		public static byte GetProtocolType(Type type)
		{
			return ProtocolTypes[(int)type];
		}

		public static bool IsWrite(Type type)
		{
			return IsWrites[(int)type];
		}

		/// <summary>
		/// Type of operation.
		/// </summary>
		public readonly Type type;

		/// <summary>
		/// Optional bin name used in operation.
		/// </summary>
		public readonly string binName;

		/// <summary>
		/// Optional argument to operation.
		/// </summary>
		public readonly Value value;

		public Operation(Type type, string binName, Value value)
		{
			this.type = type;
			this.binName = binName;
			this.value = value;
		}

		private Operation(Type type, string binName)
		{
			this.type = type;
			this.binName = binName;
			this.value = Value.AsNull;
		}

		private Operation(Type type)
		{
			this.type = type;
			this.binName = null;
			this.value = Value.AsNull;
		}
	}
}
