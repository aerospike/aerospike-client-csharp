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
	/// Expression operations.
	/// </summary>
	public sealed class ExpOperation
	{
		/// <summary>
		/// Create operation that performs an expression that writes to a record bin.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <param name="binName">name of bin to store expression result</param>
		/// <param name="exp">expression to evaluate</param>
		/// <param name="flags">expression write flags</param>
		public static Operation Write(string binName, Expression exp, ExpWriteFlags flags)
		{
			return CreateOperation(Operation.Type.EXP_MODIFY, binName, exp, (int)flags);
		}

		/// <summary>
		/// Create operation that performs a read expression.
		/// Requires server version 5.6.0+.
		/// </summary>
		/// <param name="name">
		/// Variable name of read expression result. This name can be used as the
		/// bin name when retrieving bin results from the record.
		/// </param>
		/// <param name="exp">expression to evaluate</param>
		/// <param name="flags">expression read flags</param>
		public static Operation Read(string name, Expression exp, ExpReadFlags flags)
		{
			return CreateOperation(Operation.Type.EXP_READ, name, exp, (int)flags);
		}

		private static Operation CreateOperation(Operation.Type type, string name, Expression exp, int flags)
		{
			Packer packer = new Packer();
			packer.PackArrayBegin(2);
			byte[] b = exp.Bytes;
			packer.PackByteArray(b, 0, b.Length);
			packer.PackNumber(flags);

			return new Operation(type, name, Value.Get(packer.ToByteArray()));
		}
	}
}
