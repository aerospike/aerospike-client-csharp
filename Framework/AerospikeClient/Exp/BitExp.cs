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
namespace Aerospike.Client
{
	/// <summary>
	/// Bit expression generator. See <see cref="Aerospike.Client.Exp"/>.
	/// <para>
	/// The bin expression argument in these methods can be a reference to a bin or the
	/// result of another expression. Expressions that modify bin values are only used
	/// for temporary expression evaluation and are not permanently applied to the bin.
	/// Bit modify expressions return the blob bin's value.
	/// </para>
	/// <para>
	/// Offset orientation is left-to-right.  Negative offsets are supported.
	/// If the offset is negative, the offset starts backwards from end of the bitmap.
	/// If an offset is out of bounds, a parameter error will be returned.
	/// </para>
	/// </summary>
	public sealed class BitExp
	{
		private const int MODULE = 1;

		/// <summary>
		/// Create expression that resizes byte[] to byteSize according to resizeFlags (See <see cref="BitResizeFlags"/>)
		/// and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010]</li>
		/// <li>byteSize = 4</li>
		/// <li>resizeFlags = 0</li>
		/// <li>returns [0b00000001, 0b01000010, 0b00000000, 0b00000000]</li>
		/// </ul>
		/// </summary>
		/// <example>
		/// <code>
		/// // Resize bin "a" and compare bit count
		/// Exp.EQ(
		///   BitExp.Count(Exp.Val(0), Exp.Val(3),
		///     BitExp.Resize(BitPolicy.Default, Exp.Val(4), BitResizeFlags.DEFAULT, Exp.BlobBin("a"))),
		///   Exp.Val(2))
		/// </code>
		/// </example>
		public static Exp Resize(BitPolicy policy, Exp byteSize, int resizeFlags, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.RESIZE, byteSize, policy.flags, resizeFlags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that inserts value bytes into byte[] bin at byteOffset and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>byteOffset = 1</li>
		/// <li>value = [0b11111111, 0b11000111]</li>
		/// <li>bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		/// <example>
		/// <code>
		/// // Insert bytes into bin "a" and compare bit count
		/// Exp.EQ(
		///   BitExp.Count(Exp.Val(0), Exp.Val(3),
		///     BitExp.Insert(BitPolicy.Default, Exp.Val(1), Exp.Val(bytes), Exp.BlobBin("a"))),
		///   Exp.Val(2))
		/// </code>
		/// </example>
		public static Exp Insert(BitPolicy policy, Exp byteOffset, Exp value, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.INSERT, byteOffset, value, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that removes bytes from byte[] bin at byteOffset for byteSize and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>byteOffset = 2</li>
		/// <li>byteSize = 3</li>
		/// <li>bin result = [0b00000001, 0b01000010]</li>
		/// </ul>
		/// </summary>
		/// <example>
		/// <code>
		/// // Remove bytes from bin "a" and compare bit count
		/// Exp.EQ(
		///   BitExp.Count(Exp.Val(0), Exp.Val(3),
		///     BitExp.Remove(BitPolicy.Default, Exp.Val(2), Exp.Val(3), Exp.BlobBin("a"))),
		///   Exp.Val(2))
		/// </code>
		/// </example>
		public static Exp Remove(BitPolicy policy, Exp byteOffset, Exp byteSize, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.REMOVE, byteOffset, byteSize, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that sets value on byte[] bin at bitOffset for bitSize and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 13</li>
		/// <li>bitSize = 3</li>
		/// <li>value = [0b11100000]</li>
		/// <li>bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		/// <example>
		/// <code>
		/// // Set bytes in bin "a" and compare bit count
		/// Exp.EQ(
		///   BitExp.Count(Exp.Val(0), Exp.Val(3),
		///     BitExp.Set(BitPolicy.Default, Exp.Val(13), Exp.Val(3), Exp.Val(bytes), Exp.BlobBin("a"))),
		///   Exp.Val(2))
		/// </code>
		/// </example>
		public static Exp Set(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.SET, bitOffset, bitSize, value, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that performs bitwise "or" on value and byte[] bin at bitOffset for bitSize
		/// and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 17</li>
		/// <li>bitSize = 6</li>
		/// <li>value = [0b10101000]</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Exp Or(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.OR, bitOffset, bitSize, value, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that performs bitwise "xor" on value and byte[] bin at bitOffset for bitSize
		/// and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 17</li>
		/// <li>bitSize = 6</li>
		/// <li>value = [0b10101100]</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Exp Xor(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.XOR, bitOffset, bitSize, value, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that performs bitwise "and" on value and byte[] bin at bitOffset for bitSize
		/// and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 23</li>
		/// <li>bitSize = 9</li>
		/// <li>value = [0b00111100, 0b10000000]</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Exp And(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.AND, bitOffset, bitSize, value, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that negates byte[] bin starting at bitOffset for bitSize and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 25</li>
		/// <li>bitSize = 6</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Exp Not(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.NOT, bitOffset, bitSize, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that shifts left byte[] bin starting at bitOffset for bitSize and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 32</li>
		/// <li>bitSize = 8</li>
		/// <li>shift = 3</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]</li>
		/// </ul>
		/// </summary>
		public static Exp Lshift(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp shift, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.LSHIFT, bitOffset, bitSize, shift, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that shifts right byte[] bin starting at bitOffset for bitSize and returns byte[].
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 0</li>
		/// <li>bitSize = 9</li>
		/// <li>shift = 1</li>
		/// <li>bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Exp Rshift(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp shift, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.RSHIFT, bitOffset, bitSize, shift, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that adds value to byte[] bin starting at bitOffset for bitSize and returns byte[].
		/// BitSize must be &lt;= 64. Signed indicates if bits should be treated as a signed number.
		/// If add overflows/underflows, <see cref="BitOverflowAction"/> is used.
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 24</li>
		/// <li>bitSize = 16</li>
		/// <li>value = 128</li>
		/// <li>signed = false</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]</li>
		/// </ul>
		/// </summary>
		public static Exp Add(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, bool signed, BitOverflowAction action, Exp bin)
		{
			byte[] bytes = PackMath(BitOperation.ADD, policy, bitOffset, bitSize, value, signed, action);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that subtracts value from byte[] bin starting at bitOffset for bitSize and returns byte[].
		/// BitSize must be &lt;= 64. Signed indicates if bits should be treated as a signed number.
		/// If add overflows/underflows, <see cref="BitOverflowAction"/> is used.
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 24</li>
		/// <li>bitSize = 16</li>
		/// <li>value = 128</li>
		/// <li>signed = false</li>
		/// <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]</li>
		/// </ul>
		/// </summary>
		public static Exp Subtract(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, bool signed, BitOverflowAction action, Exp bin)
		{
			byte[] bytes = PackMath(BitOperation.SUBTRACT, policy, bitOffset, bitSize, value, signed, action);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that sets value to byte[] bin starting at bitOffset for bitSize and returns byte[].
		/// BitSize must be &lt;= 64.
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 1</li>
		/// <li>bitSize = 8</li>
		/// <li>value = 127</li>
		/// <li>bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]</li>
		/// </ul>
		/// </summary>
		public static Exp SetInt(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.SET_INT, bitOffset, bitSize, value, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that returns bits from byte[] bin starting at bitOffset for bitSize.
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 9</li>
		/// <li>bitSize = 5</li>
		/// <li>returns [0b10000000]</li>
		/// </ul>
		/// </summary>
		/// <example>
		/// <code>
		/// // Bin "a" bits = [0b10000000]
		/// Exp.EQ(
		///   BitExp.Get(Exp.Val(9), Exp.Val(5), Exp.BlobBin("a")),
		///   Exp.Val(new byte[] {(byte)0b10000000}))
		/// </code>
		/// </example>
		public static Exp Get(Exp bitOffset, Exp bitSize, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.GET, bitOffset, bitSize);
			return AddRead(bin, bytes, Exp.Type.BLOB);
		}

		/// <summary>
		/// Create expression that returns integer count of set bits from byte[] bin starting at
		/// bitOffset for bitSize.
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 20</li>
		/// <li>bitSize = 4</li>
		/// <li>returns 2</li>
		/// </ul>
		/// </summary>
		/// <example>
		/// <code>
		/// // Bin "a" bit count &lt;= 2
		/// Exp.LE(BitExp.Count(Exp.Val(0), Exp.Val(5), Exp.BlobBin("a")), Exp.Val(2))
		/// </code>
		/// </example>
		public static Exp Count(Exp bitOffset, Exp bitSize, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.COUNT, bitOffset, bitSize);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that returns integer bit offset of the first specified value bit in byte[] bin
		/// starting at bitOffset for bitSize.
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 24</li>
		/// <li>bitSize = 8</li>
		/// <li>value = true</li>
		/// <li>returns 5</li>
		/// </ul>
		/// </summary>
		/// <example>
		/// <code>
		/// // lscan(a) == 5
		/// Exp.EQ(BitExp.Lscan(Exp.Val(24), Exp.Val(8), Exp.Val(true), Exp.BlobBin("a")), Exp.Val(5))
		/// </code>
		/// </example>
		/// <param name="bitOffset">offset int expression</param>
		/// <param name="bitSize">size int expression</param>
		/// <param name="value">boolean expression</param>
		/// <param name="bin">bin or blob value expression</param>
		public static Exp Lscan(Exp bitOffset, Exp bitSize, Exp value, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.LSCAN, bitOffset, bitSize, value);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that returns integer bit offset of the last specified value bit in byte[] bin
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
		/// <example>
		/// <code>
		/// // rscan(a) == 7
		/// Exp.EQ(BitExp.Rscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin("a")), Exp.Val(7))
		/// </code>
		/// </example>
		/// <param name="bitOffset">offset int expression</param>
		/// <param name="bitSize">size int expression</param>
		/// <param name="value">boolean expression</param>
		/// <param name="bin">bin or blob value expression</param>
		public static Exp Rscan(Exp bitOffset, Exp bitSize, Exp value, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(BitOperation.RSCAN, bitOffset, bitSize, value);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that returns integer from byte[] bin starting at bitOffset for bitSize.
		/// Signed indicates if bits should be treated as a signed number.
		/// <ul>
		/// <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
		/// <li>bitOffset = 8</li>
		/// <li>bitSize = 16</li>
		/// <li>signed = false</li>
		/// <li>returns 16899</li>
		/// </ul>
		/// </summary>
		/// <example>
		/// <code>
		/// // getInt(a) == 16899
		/// Exp.EQ(BitExp.GetInt(Exp.Val(8), Exp.Val(16), false, Exp.BlobBin("a")), Exp.Val(16899))
		/// </code>
		/// </example>
		public static Exp GetInt(Exp bitOffset, Exp bitSize, bool signed, Exp bin)
		{
			byte[] bytes = PackGetInt(bitOffset, bitSize, signed);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		private static byte[] PackMath(int command, BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, bool signed, BitOverflowAction action)
		{
			Packer packer = new Packer();
			// Pack.init() only required when CTX is used and server does not support CTX for bit operations.
			// Pack.init(packer, ctx);
			packer.PackArrayBegin(6);
			packer.PackNumber(command);
			bitOffset.Pack(packer);
			bitSize.Pack(packer);
			value.Pack(packer);
			packer.PackNumber(policy.flags);

			int flags = (int)action;

			if (signed)
			{
				flags |= BitOperation.INT_FLAGS_SIGNED;
			}
			packer.PackNumber(flags);
			return packer.ToByteArray();
		}

		private static byte[] PackGetInt(Exp bitOffset, Exp bitSize, bool signed)
		{
			Packer packer = new Packer();
			// Pack.init() only required when CTX is used and server does not support CTX for bit operations.
			// Pack.init(packer, ctx);
			packer.PackArrayBegin(signed ? 4 : 3);
			packer.PackNumber(BitOperation.GET_INT);
			bitOffset.Pack(packer);
			bitSize.Pack(packer);

			if (signed)
			{
				packer.PackNumber(BitOperation.INT_FLAGS_SIGNED);
			}
			return packer.ToByteArray();
		}

		private static Exp AddWrite(Exp bin, byte[] bytes)
		{
			return new Exp.Module(bin, bytes, (int)Exp.Type.BLOB, MODULE | Exp.MODIFY);
		}

		private static Exp AddRead(Exp bin, byte[] bytes, Exp.Type retType)
		{
			return new Exp.Module(bin, bytes, (int)retType, MODULE);
		}
	}
}
