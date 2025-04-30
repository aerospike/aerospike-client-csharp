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
	/// Batch delete operation.
	/// </summary>
	public sealed class BatchDelete : BatchRecord
	{
		/// <summary>
		/// Optional delete policy.
		/// </summary>
		public readonly BatchDeletePolicy policy;

		/// <summary>
		/// Initialize key.
		/// </summary>
		public BatchDelete(Key key)
			: base(key, true)
		{
			this.policy = null;
		}

		/// <summary>
		/// Initialize policy and key.
		/// </summary>
		public BatchDelete(BatchDeletePolicy policy, Key key)
			: base(key, true)
		{
			this.policy = policy;
		}

		/// <summary>
		/// Return batch command type.
		/// </summary>
		public override Type GetBatchType()
		{
			return Type.BATCH_DELETE;
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

			BatchDelete other = (BatchDelete)obj;

			if (policy != other.policy)
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
				if (configProvider.ConfigurationData.dynamicConfig.batch_delete.send_key.HasValue)
				{
					sendKey = configProvider.ConfigurationData.dynamicConfig.batch_delete.send_key.Value;
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
					if (configProvider.ConfigurationData.dynamicConfig.batch_delete.send_key.HasValue)
					{
						sendKey = configProvider.ConfigurationData.dynamicConfig.batch_delete.send_key.Value;
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
			return size;
		}
	}
}
