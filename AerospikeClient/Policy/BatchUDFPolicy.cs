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
	/// Policy attributes used in batch UDF execute commands.
	/// </summary>
	public sealed class BatchUDFPolicy
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
		/// Desired consistency guarantee when committing a command on the server. The default 
		/// (COMMIT_ALL) indicates that the server should wait for master and all replica commits to 
		/// be successful before returning success to the client. 
		/// <para>
		/// Default: CommitLevel.COMMIT_ALL
		/// </para>
		/// </summary>
		public CommitLevel commitLevel = CommitLevel.COMMIT_ALL;
	
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
		/// If true and the UDF writes a record, the key will be stored with the record on the server.
		/// <para>
		/// Default: false (do not send the user defined key)
		/// </para>
		/// </summary>
		public bool sendKey;

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public BatchUDFPolicy(BatchUDFPolicy other)
		{
			this.filterExp = other.filterExp;
			this.commitLevel = other.commitLevel;
			this.expiration = other.expiration;
			this.durableDelete = other.durableDelete;
			this.OnLockingOnly = other.OnLockingOnly;
			this.sendKey = other.sendKey;
		}

		/// <summary>
		/// Copy batch UDF policy from another policy and override according to the AerospikeConfigProvider.
		/// </summary>
		public BatchUDFPolicy(BatchUDFPolicy other, IConfigProvider configProvider) : this(other)
		{
			if (configProvider == null)
			{
				return;
			}

			if (configProvider.ConfigurationData == null)
			{
				return;
			}

			var batch_udf = configProvider.ConfigurationData.dynamicConfig.batch_udf;

			if (batch_udf.durable_delete.HasValue)
			{
				this.durableDelete = batch_udf.durable_delete.Value;
			}
			if (batch_udf.send_key.HasValue)
			{
				this.sendKey = batch_udf.send_key.Value;
			}
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public BatchUDFPolicy()
		{
		}

		/// <summary>
		/// Creates a deep copy of this batch UDF policy.
		/// </summary>
		/// <returns></returns>
		public BatchUDFPolicy Clone()
		{
			return new BatchUDFPolicy(this);
		}
	}
}
