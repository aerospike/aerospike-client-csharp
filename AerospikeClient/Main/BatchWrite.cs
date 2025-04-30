/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	/// Batch key and read/write operations with write policy.
	/// </summary>
	public sealed class BatchWrite : BatchRecord
	{
		/// <summary>
		/// Optional write policy.
		/// </summary>
		public readonly BatchWritePolicy policy;

		/// <summary>
		/// Required operations for this key.
		/// </summary>
		public readonly Operation[] ops;

		/// <summary>
		/// Initialize batch key and read/write operations.
		/// <para>
		/// <see cref="Operation.Get()"/> is not allowed because it returns a variable number of bins
		/// and makes it difficult (sometimes impossible) to lineup operations with results. Instead,
		/// use <see cref="Operation.Get(string)"/> for each bin name.
		/// </para>
		/// </summary>
		public BatchWrite(Key key, Operation[] ops)
			: base(key, true)
		{
			this.ops = ops;
			this.policy = null;
		}

		/// <summary>
		/// Initialize policy, batch key and read/write operations.
		/// <para>
		/// <see cref="Operation.Get()"/> is not allowed because it returns a variable number of bins
		/// and makes it difficult (sometimes impossible) to lineup operations with results. Instead,
		/// use <see cref="Operation.Get(string)"/> for each bin name.
		/// </para>
		/// </summary>
		public BatchWrite(BatchWritePolicy policy, Key key, Operation[] ops)
			: base(key, true)
		{
			this.ops = ops;
			this.policy = policy;
		}

		/// <summary>
		/// Return batch command type.
		/// </summary>
		public override Type GetBatchType()
		{
			return Type.BATCH_WRITE;
		}

		/// <summary>
		/// Optimized reference equality check to determine batch wire protocol repeat flag.
		/// For internal use only.
		/// </summary>
		public override bool Equals(BatchRecord obj, IConfigProvider configProvider)
		{
			if (this.GetType() != obj.GetType())
			{
				return false;
			}

			BatchWrite other = (BatchWrite)obj;

			if (ops != other.ops || policy != other.policy)
			{
				return false;
			}

			bool sendKey = false;
			if (policy != null)
			{
				sendKey = policy.sendKey;
			}
			if (configProvider != null && configProvider.ConfigurationData != null)
			{
				if (configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.HasValue)
				{
					sendKey = configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.Value;
				}
			}

			return !sendKey;
		}

		/// <summary>
		/// Return wire protocol size. For internal use only.
		/// </summary>
		public override int Size(Policy parentPolicy, IConfigProvider configProvider)
		{
			int size = 2; // gen(2) = 2

			if (policy != null)
			{
				if (policy.filterExp != null)
				{
					size += policy.filterExp.Size();
				}

				bool sendKey = policy.sendKey;
				if (configProvider != null && configProvider.ConfigurationData != null)
				{
					if (configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.HasValue)
					{
						sendKey = configProvider.ConfigurationData.dynamicConfig.batch_write.send_key.Value;
					}
				}

				if (sendKey || parentPolicy.sendKey)
				{
					size += key.userKey.EstimateSize() + Command.FIELD_HEADER_SIZE + 1;
				}
			}
			else if (parentPolicy.sendKey)
			{
				size += key.userKey.EstimateSize() + Command.FIELD_HEADER_SIZE + 1;
			}

			bool hasWrite = false;

			foreach (Operation op in ops)
			{
				if (Operation.IsWrite(op.type))
				{
					hasWrite = true;
				}
				size += ByteUtil.EstimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
				size += op.value.EstimateSize();
			}

			if (!hasWrite)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Batch write operations do not contain a write");
			}
			return size;
		}
	}
}
