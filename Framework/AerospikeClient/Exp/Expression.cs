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
	}
}
