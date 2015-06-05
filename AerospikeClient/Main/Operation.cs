/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
			return new Operation(Type.WRITE, bin);
		}

		/// <summary>
		/// Create string append database operation.
		/// </summary>
		public static Operation Append(Bin bin)
		{
			return new Operation(Type.APPEND, bin);
		}

		/// <summary>
		/// Create string prepend database operation.
		/// </summary>
		public static Operation Prepend(Bin bin)
		{
			return new Operation(Type.PREPEND, bin);
		}

		/// <summary>
		/// Create integer add database operation.
		/// </summary>
		public static Operation Add(Bin bin)
		{
			return new Operation(Type.ADD, bin);
		}

		/// <summary>
		/// Create touch database operation.
		/// </summary>
		public static Operation Touch()
		{
			return new Operation(Type.TOUCH);
		}

		public enum Type
		{
			READ,
			READ_HEADER,
			WRITE,
			ADD,
			APPEND,
			PREPEND,
			TOUCH
		}

		private static byte[] ProtocolTypes = new byte[] { 1, 1, 2, 5, 9, 10, 11 };

		public static byte GetProtocolType(Type type)
		{
			return ProtocolTypes[(int)type];
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
		/// Optional bin value used in operation.
		/// </summary>
		public readonly Value binValue;

		private Operation(Type type, Bin bin)
		{
			this.type = type;
			this.binName = bin.name;
			this.binValue = bin.value;
		}

		private Operation(Type type, string binName)
		{
			this.type = type;
			this.binName = binName;
			this.binValue = Value.AsNull;
		}

		private Operation(Type type)
		{
			this.type = type;
			this.binName = null;
			this.binValue = Value.AsNull;
		}
	}
}
