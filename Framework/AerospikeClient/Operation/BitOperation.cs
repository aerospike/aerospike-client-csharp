/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
	/// Bit operations. Create bit operations used by client operate command.
	/// Offset orientation is left-to-right.  Negative offsets are supported.
	/// If the offset is negative, the offset starts backwards from end of the bitmap.
	/// If an offset is out of bounds, a parameter error will be returned.
	/// <para>
	/// Bit operations on bitmap items nested in lists/maps are not currently
	/// supported by the server.
	/// </para>
	/// </summary>
	public sealed class BitOperation
	{
		private const int RESIZE = 0;
		private const int INSERT = 1;
		private const int REMOVE = 2;
		private const int SET = 3;
		private const int OR = 4;
		private const int XOR = 5;
		private const int AND = 6;
		private const int NOT = 7;
		private const int LSHIFT = 8;
		private const int RSHIFT = 9;
		private const int ADD = 10;
		private const int SUBTRACT = 11;
		private const int SET_INT = 12;
		private const int GET = 50;
		private const int COUNT = 51;
		private const int LSCAN = 52;
		private const int RSCAN = 53;
		private const int GET_INT = 54;

		private const int INT_FLAGS_SIGNED = 1;

		/// <summary>
		/// Resize a bin to byteSize according to resizeFlags (See <seealso cref="BitResizeFlags"/>).
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010]</li>
		/// <li>byteSize = 4</li>
		/// <li>resizeFlags = 0</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000000, 0b00000000]</li>
		/// </ul>
		/// </summary>
		public static Operation Resize(BitPolicy policy, string binName, int byteSize, BitResizeFlags resizeFlags)
		{
			return CreateOperation(RESIZE, Operation.Type.BIT_MODIFY, binName, byteSize, policy.flags, (int)resizeFlags);
		}

		/// <summary>
		/// Create byte "insert" operation.
		/// Server inserts value bytes into byte[] bin at byteOffset.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>byteOffset = 1</li>
		/// <li>value = [0b11111111, 0b11000111]</li>
		/// <li>bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Operation Insert(BitPolicy policy, string binName, int byteOffset, byte[] value)
		{
			Packer packer = new Packer();
			Init(packer, INSERT, 3);
			packer.PackNumber(byteOffset);
			packer.PackBytes(value);
			packer.PackNumber(policy.flags);
			return new Operation(Operation.Type.BIT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create byte "remove" operation.
		/// Server removes bytes from byte[] bin at byteOffset for byteSize.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>byteOffset = 2</li>
		/// <li>byteSize = 3</li>
		/// <li>bin result = [0b00000001, 0b01000010]</li>
		/// </ul>
		/// </summary>
		public static Operation Remove(BitPolicy policy, string binName, int byteOffset, int byteSize)
		{
			return CreateOperation(REMOVE, Operation.Type.BIT_MODIFY, binName, byteOffset, byteSize, policy.flags);
		}

		/// <summary>
		/// Create bit "set" operation.
		/// Server sets value on byte[] bin at bitOffset for bitSize.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 13</li>
		/// <li>bitSize = 3</li>
		/// <li>value = [0b11100000]</li>
		/// <li>bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Operation Set(BitPolicy policy, string binName, int bitOffset, int bitSize, byte[] value)
		{
			return CreateOperation(SET, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, value, policy.flags);
		}

		/// <summary>
		/// Create bit "or" operation.
		/// Server performs bitwise "or" on value and byte[] bin at bitOffset for bitSize.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 17</li>
		/// <li>bitSize = 6</li>
		/// <li>value = [0b10101000]</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Operation Or(BitPolicy policy, string binName, int bitOffset, int bitSize, byte[] value)
		{
			return CreateOperation(OR, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, value, policy.flags);
		}

		/// <summary>
		/// Create bit "exclusive or" operation.
		/// Server performs bitwise "xor" on value and byte[] bin at bitOffset for bitSize.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 17</li>
		/// <li>bitSize = 6</li>
		/// <li>value = [0b10101100]</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Operation Xor(BitPolicy policy, string binName, int bitOffset, int bitSize, byte[] value)
		{
			return CreateOperation(XOR, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, value, policy.flags);
		}

		/// <summary>
		/// Create bit "and" operation.
		/// Server performs bitwise "and" on value and byte[] bin at bitOffset for bitSize.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 23</li>
		/// <li>bitSize = 9</li>
		/// <li>value = [0b00111100, 0b10000000]</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Operation And(BitPolicy policy, string binName, int bitOffset, int bitSize, byte[] value)
		{
			return CreateOperation(AND, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, value, policy.flags);
		}

		/// <summary>
		/// Create bit "not" operation.
		/// Server negates byte[] bin starting at bitOffset for bitSize.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 25</li>
		/// <li>bitSize = 6</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Operation Not(BitPolicy policy, string binName, int bitOffset, int bitSize)
		{
			return CreateOperation(NOT, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, policy.flags);
		}

		/// <summary>
		/// Create bit "left shift" operation.
		/// Server shifts left byte[] bin starting at bitOffset for bitSize.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 32</li>
		/// <li>bitSize = 8</li>
		/// <li>shift = 3</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]</li>
		/// </ul>
		/// </summary>
		public static Operation Lshift(BitPolicy policy, string binName, int bitOffset, int bitSize, int shift)
		{
			return CreateOperation(LSHIFT, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, shift, policy.flags);
		}

		/// <summary>
		/// Create bit "right shift" operation.
		/// Server shifts right byte[] bin starting at bitOffset for bitSize.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 0</li>
		/// <li>bitSize = 9</li>
		/// <li>shift = 1</li>
		/// <li>bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Operation Rshift(BitPolicy policy, string binName, int bitOffset, int bitSize, int shift)
		{
			return CreateOperation(RSHIFT, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, shift, policy.flags);
		}

		/// <summary>
		/// Create bit "add" operation.
		/// Server adds value to byte[] bin starting at bitOffset for bitSize. BitSize must be &lt;= 64.
		/// Signed indicates if bits should be treated as a signed number.
		/// If add overflows/underflows, <seealso cref="BitOverflowAction"/> is used.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 24</li>
		/// <li>bitSize = 16</li>
		/// <li>value = 128</li>
		/// <li>signed = false</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]</li>
		/// </ul>
		/// </summary>
		public static Operation Add
		(
			BitPolicy policy,
			string binName,
			int bitOffset,
			int bitSize,
			long value,
			bool signed,
			BitOverflowAction action
		)
		{
			return CreateMathOperation(ADD, policy, binName, bitOffset, bitSize, value, signed, action);
		}

		/// <summary>
		/// Create bit "subtract" operation.
		/// Server subtracts value from byte[] bin starting at bitOffset for bitSize. BitSize must be &lt;= 64.
		/// Signed indicates if bits should be treated as a signed number.
		/// If add overflows/underflows, <seealso cref="BitOverflowAction"/> is used.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 24</li>
		/// <li>bitSize = 16</li>
		/// <li>value = 128</li>
		/// <li>signed = false</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]</li>
		/// </ul>
		/// </summary>
		public static Operation Subtract
		(
			BitPolicy policy,
			string binName,
			int bitOffset,
			int bitSize,
			long value,
			bool signed,
			BitOverflowAction action
		)
		{
			return CreateMathOperation(SUBTRACT, policy, binName, bitOffset, bitSize, value, signed, action);
		}

		/// <summary>
		/// Create bit "setInt" operation.
		/// Server sets value to byte[] bin starting at bitOffset for bitSize. Size must be &lt;= 64.
		/// Server does not return a value.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 1</li>
		/// <li>bitSize = 8</li>
		/// <li>value = 127</li>
		/// <li>bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Operation SetInt(BitPolicy policy, string binName, int bitOffset, int bitSize, long value)
		{
			Packer packer = new Packer();
			Init(packer, SET_INT, 4);
			packer.PackNumber(bitOffset);
			packer.PackNumber(bitSize);
			packer.PackNumber(value);
			packer.PackNumber(policy.flags);
			return new Operation(Operation.Type.BIT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create bit "get" operation.
		/// Server returns bits from byte[] bin starting at bitOffset for bitSize.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 9</li>
		/// <li>bitSize = 5</li>
		/// <li>returns [0b10000000]</li>
		/// </ul>
		/// </summary>
		public static Operation Get(string binName, int bitOffset, int bitSize)
		{
			return CreateOperation(GET, Operation.Type.BIT_READ, binName, bitOffset, bitSize);
		}

		/// <summary>
		/// Create bit "count" operation.
		/// Server returns integer count of set bits from byte[] bin starting at bitOffset for bitSize.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 20</li>
		/// <li>bitSize = 4</li>
		/// <li>returns 2</li>
		/// </ul>
		/// </summary>
		public static Operation Count(string binName, int bitOffset, int bitSize)
		{
			return CreateOperation(COUNT, Operation.Type.BIT_READ, binName, bitOffset, bitSize);
		}

		/// <summary>
		/// Create bit "left scan" operation.
		/// Server returns integer bit offset of the first specified value bit in byte[] bin
		/// starting at bitOffset for bitSize.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 24</li>
		/// <li>bitSize = 8</li>
		/// <li>value = true</li>
		/// <li>returns 5</li>
		/// </ul>
		/// </summary>
		public static Operation Lscan(string binName, int bitOffset, int bitSize, bool value)
		{
			return CreateOperation(LSCAN, Operation.Type.BIT_READ, binName, bitOffset, bitSize, value);
		}

		/// <summary>
		/// Create bit "right scan" operation.
		/// Server returns integer bit offset of the last specified value bit in byte[] bin
		/// starting at bitOffset for bitSize.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 32</li>
		/// <li>bitSize = 8</li>
		/// <li>value = true</li>
		/// <li>returns 7</li>
		/// </ul>
		/// </summary>
		public static Operation Rscan(string binName, int bitOffset, int bitSize, bool value)
		{
			return CreateOperation(RSCAN, Operation.Type.BIT_READ, binName, bitOffset, bitSize, value);
		}

		/// <summary>
		/// Create bit "get integer" operation.
		/// Server returns integer from byte[] bin starting at bitOffset for bitSize.
		/// Signed indicates if bits should be treated as a signed number.
		/// Example:
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 8</li>
		/// <li>bitSize = 16</li>
		/// <li>signed = false</li>
		/// <li>returns 16899</li>
		/// </ul>
		/// </summary>
		public static Operation GetInt(string binName, int bitOffset, int bitSize, bool signed)
		{
			Packer packer = new Packer();
			Init(packer, GET_INT, signed ? 3 : 2);
			packer.PackNumber(bitOffset);
			packer.PackNumber(bitSize);

			if (signed)
			{
				packer.PackNumber(INT_FLAGS_SIGNED);
			}
			return new Operation(Operation.Type.BIT_READ, binName, Value.Get(packer.ToByteArray()));
		}

		private static Operation CreateOperation
		(
			int command,
			Operation.Type type,
			string binName,
			int v1,
			int v2
		)
		{
			Packer packer = new Packer();
			Init(packer, command, 2);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		private static Operation CreateOperation
		(
			int command,
			Operation.Type type,
			string binName,
			int v1,
			int v2,
			bool v3
		)
		{
			Packer packer = new Packer();
			Init(packer, command, 3);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackBoolean(v3);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		private static Operation CreateOperation
		(
			int command,
			Operation.Type type,
			string binName,
			int v1,
			int v2,
			int v3
		)
		{
			Packer packer = new Packer();
			Init(packer, command, 3);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		private static Operation CreateOperation
		(
			int command,
			Operation.Type type,
			string binName,
			int v1,
			int v2,
			int v3,
			int v4
		)
		{
			Packer packer = new Packer();
			Init(packer, command, 4);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackNumber(v3);
			packer.PackNumber(v4);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		private static Operation CreateOperation
		(
			int command,
			Operation.Type type,
			string binName,
			int v1,
			int v2,
			byte[] v3,
			int v4
		)
		{
			Packer packer = new Packer();
			Init(packer, command, 4);
			packer.PackNumber(v1);
			packer.PackNumber(v2);
			packer.PackBytes(v3);
			packer.PackNumber(v4);
			return new Operation(type, binName, Value.Get(packer.ToByteArray()));
		}

		private static Operation CreateMathOperation
		(
			int command,
			BitPolicy policy,
			string binName,
			int bitOffset,
			int bitSize,
			long value,
			bool signed,
			BitOverflowAction action
		)
		{
			Packer packer = new Packer();
			Init(packer, command, 5);
			packer.PackNumber(bitOffset);
			packer.PackNumber(bitSize);
			packer.PackNumber(value);
			packer.PackNumber(policy.flags);

			int flags = (int)action;

			if (signed)
			{
				flags |= INT_FLAGS_SIGNED;
			}
			packer.PackNumber(flags);
			return new Operation(Operation.Type.BIT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		private static void Init(Packer packer, int command, int count)
		{
			packer.PackArrayBegin(count + 1);
			packer.PackNumber(command);
		}
	}
}
