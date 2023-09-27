/*
 * Copyright 2012-2022 Aerospike, Inc.
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
	/// Packed expression byte instructions.
	/// </summary>
	[Serializable]
	public sealed class Expression
	{
		private readonly byte[] bytes;

		internal Expression(Exp exp)
		{
			Packer packer = new Packer();
			exp.Pack(packer);
			bytes = packer.ToByteArray();
		}

		internal Expression(byte[] bytes)
		{
			this.bytes = bytes;
		}

		/// <summary>
		/// Return a new expression from packed expression instructions in bytes.
		/// </summary>
		public static Expression FromBytes(byte[] bytes)
		{
			return new Expression(bytes);
		}

		/// <summary>
		/// Return a new expression from packed expression instructions in base64 encoded chars.
		/// </summary>
		public static Expression FromBase64(char[] chars)
		{
			return Expression.FromBytes(Convert.FromBase64CharArray(chars, 0, chars.Length));
		}

		/// <summary>
		/// Return a new expression from packed expression instructions in base64 encoded string.
		/// </summary>
		public static Expression FromBase64(string s)
		{
			return Expression.FromBytes(Convert.FromBase64String(s));
		}

		/// <summary>
		/// Packed byte instructions.
		/// </summary>
		public byte[] Bytes
		{
			get { return bytes; }
		}

		/// <summary>
		/// Return byte instructions in base64 encoding.
		/// </summary>
		public string GetBase64()
		{
			return Convert.ToBase64String(bytes);
		}

		/// <summary>
		/// Estimate expression size in wire protocol.
		/// For internal use only.
		/// </summary>
		public int Size()
		{
			return bytes.Length + Command.FIELD_HEADER_SIZE;
		}

		/// <summary>
		/// Write expression in wire protocol.
		/// For internal use only.
		/// </summary>
		public void Write(Command cmd)
		{
			cmd.WriteExpHeader(bytes.Length);
			Array.Copy(bytes, 0, cmd.dataBuffer, cmd.dataOffset, bytes.Length);
			cmd.dataOffset += bytes.Length;
		}

		/// <summary>
		/// Write expression in wire protocol.
		/// For internal use only.
		/// </summary>
		public void Write(Command cmd, Buffer buffer)
		{
			cmd.WriteExpHeader(bytes.Length);
			Array.Copy(bytes, 0, buffer.DataBuffer, buffer.Offset, bytes.Length);
			buffer.Offset += bytes.Length;
		}
	}
}
