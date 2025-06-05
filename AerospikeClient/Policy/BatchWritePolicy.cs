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
using System.ComponentModel;

namespace Aerospike.Client
{
	/// <summary>
	/// Policy attributes used in batch write commands.
	/// </summary>
	public sealed class BatchWritePolicy
	{
		/// <summary>
		/// Optional expression filter. If filterExp exists and evaluates to false, the specific batch key
		/// request is not performed and <see cref="BatchRecord.resultCode"/> is set to
		/// <see cref="ResultCode.FILTERED_OUT"/>.
		/// <para>
		/// If exists, this filter overrides the batch parent filter <seealso cref="Policy.filterExp"/>
		/// for the specific key in batch commands that allow a different policy per key.
		/// Otherwise, this filter is ignored.
		/// </para>
		/// <para>
		/// Default: null
		/// </para>
		/// </summary>
		public Expression filterExp;

		/// <summary>
		/// Qualify how to handle writes where the record already exists.
		/// <para>
		/// Default: RecordExistsAction.UPDATE
		/// </para>
		/// </summary>
		public RecordExistsAction recordExistsAction = RecordExistsAction.UPDATE;

		/// <summary>
		/// Desired consistency guarantee when committing a command on the server. The default 
		/// (COMMIT_ALL) indicates that the server should wait for master and all replica commits to 
		/// be successful before returning success to the client. 
		/// <para>
		/// Default: CommitLevel.COMMIT_ALL
		/// </para>
		/// </summary>
		public CommitLevel commitLevel = CommitLevel.COMMIT_ALL;

		/// <summary>
		/// Qualify how to handle record deletes based on record generation. The default (NONE)
		/// indicates that the generation is not used to restrict deletes.
		/// <para>
		/// Default: GenerationPolicy.NONE
		/// </para>
		/// </summary>
		public GenerationPolicy generationPolicy = GenerationPolicy.NONE;

		/// <summary>
		/// Expected generation. Generation is the number of times a record has been modified
		/// (including creation) on the server. This field is only relevant when generationPolicy
		/// is not NONE.
		/// <para>Default: 0</para>
		/// </summary>
		public int generation;

		/// <summary>
		/// Record expiration. Also known as ttl (time to live).
		/// Seconds record will live before being removed by the server.
		/// <para>
		/// Expiration values:
		/// <ul>
		/// <li>-2: Do not change ttl when record is updated.</li>
		/// <li>-1: Never expire.</li>
		/// <li>0: Default to namespace configuration variable "default-ttl" on the server.</li>
		/// <li>&gt; 0: Actual ttl in seconds.</li>
		/// </ul>
		/// </para>
		/// <para>Default: 0</para>
		/// </summary>
		public int expiration;

		/// <summary>
		/// If the command results in a record deletion, leave a tombstone for the record.
		/// This prevents deleted records from reappearing after node failures.
		/// Valid for Aerospike Server Enterprise Edition only.
		/// <para>
		/// Default: false (do not tombstone deleted records).
		/// </para>
		/// </summary>
		public bool durableDelete;

		/// <summary>
		/// Execute the write command only if the record is not already locked by this transaction.
		/// If this field is true and the record is already locked by this transaction, the command
		/// will throw an exception with the <see cref="ResultCode.MRT_ALREADY_LOCKED"/>
		/// error code.
		/// <para>
		/// This field is useful for safely retrying non-idempotent writes as an alternative to simply
		/// aborting the transaction. This field is not applicable to record delete commands.
		/// </para>
		/// <para>
		/// Default: false.
		/// </para>
		/// </summary>
		public bool OnLockingOnly;

		/// <summary>
		/// Send user defined key in addition to hash digest.
		/// If true, the key will be stored with the record on the server.
		/// <para>
		/// Default: false (do not send the user defined key)
		/// </para>
		/// </summary>
		public bool sendKey;

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public BatchWritePolicy(BatchWritePolicy other)
		{
			this.filterExp = other.filterExp;
			this.recordExistsAction = other.recordExistsAction;
			this.commitLevel = other.commitLevel;
			this.generationPolicy = other.generationPolicy;
			this.generation = other.generation;
			this.expiration = other.expiration;
			this.durableDelete = other.durableDelete;
			this.OnLockingOnly = other.OnLockingOnly;
			this.sendKey = other.sendKey;
		}

		/// <summary>
		/// Copy batch write policy from another policy and override according to the AerospikeConfigProvider.
		/// </summary>
		public BatchWritePolicy(BatchWritePolicy other, IConfigProvider configProvider) : this(other)
		{
			if (configProvider == null)
			{
				return;
			}

			if (configProvider.ConfigurationData == null)
			{
				return;
			}

			var batch_write = configProvider.ConfigurationData.dynamicConfig.batch_write;
			if (batch_write == null)
			{
				return;
			}

			if (batch_write.send_key.HasValue)
			{
				this.sendKey = batch_write.send_key.Value;
			}
			if (batch_write.durable_delete.HasValue)
			{
				this.durableDelete = batch_write.durable_delete.Value;
			}
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public BatchWritePolicy()
		{
		}

		/// <summary>
		/// Creates a deep copy of this batch write policy.
		/// </summary>
		/// <returns></returns>
		public BatchWritePolicy Clone()
		{
			return new BatchWritePolicy(this);
		}
	}
}
