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
	/// Batch user defined functions.
	/// </summary>
	public sealed class BatchUDF : BatchRecord
	{
		/// <summary>
		/// Optional UDF policy.
		/// </summary>
		public readonly BatchUDFPolicy policy;

		/// <summary>
		/// Package or lua module name.
		/// </summary>
		public readonly string packageName;

		/// <summary>
		/// Lua function name.
		/// </summary>
		public readonly string functionName;

		/// <summary>
		/// Optional arguments to lua function.
		/// </summary>
		public readonly Value[] functionArgs;

		/// <summary>
		/// Wire protocol bytes for function args. For internal use only.
		/// </summary>
		public byte[] argBytes;

		/// <summary>
		/// Constructor using default policy.
		/// </summary>
		public BatchUDF(Key key, string packageName, string functionName, Value[] functionArgs)
			: base(key, true)
		{
			this.policy = null;
			this.packageName = packageName;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
			// Do not set argBytes here because may not be necessary if batch repeat flag is used.
		}

		/// <summary>
		/// Constructor using specified policy.
		/// </summary>
		public BatchUDF(BatchUDFPolicy policy, Key key, string packageName, string functionName, Value[] functionArgs)
			: base(key, true)
		{
			this.policy = policy;
			this.packageName = packageName;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
		}

		/// <summary>
		/// Return batch command type.
		/// </summary>
		public override Type GetBatchType()
		{
			return Type.BATCH_UDF;
		}

		/// <summary>
		/// Optimized reference equality check to determine batch wire protocol repeat flag.
		/// For internal use only.
		/// </summary>
		public override bool Equals(BatchRecord obj)
		{
			if (this.GetType() != obj.GetType())
			{
				return false;
			}

			BatchUDF other = (BatchUDF)obj;
			return functionName == other.functionName && functionArgs == other.functionArgs && 
				   packageName == other.packageName && policy == other.policy;
		}

		/// <summary>
		/// Return wire protocol size. For internal use only.
		/// </summary>
		public override int Size()
		{
			int size = 6; // gen(2) + exp(4) = 6

			if (policy != null)
			{
				if (policy.filterExp != null)
				{
					size += policy.filterExp.Size();
				}

				if (policy.sendKey)
				{
					size += key.userKey.EstimateSize() + Command.FIELD_HEADER_SIZE + 1;
				}
			}
			size += ByteUtil.EstimateSizeUtf8(packageName) + Command.FIELD_HEADER_SIZE;
			size += ByteUtil.EstimateSizeUtf8(functionName) + Command.FIELD_HEADER_SIZE;
			argBytes = Packer.Pack(functionArgs);
			size += argBytes.Length + Command.FIELD_HEADER_SIZE;
			return size;
		}
	}
}
