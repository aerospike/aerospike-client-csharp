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
	/// Transaction policy fields used to batch verify record versions on commit.
	/// Used a placeholder for now as there are no additional fields beyond BatchPolicy.
	/// </summary>
	public sealed class TxnVerifyPolicy : BatchPolicy
	{
		/// <summary>
		/// Copy policy from another policy.
		/// </summary>
		public TxnVerifyPolicy(TxnVerifyPolicy other) : 
			base(other)
		{
		}

		public TxnVerifyPolicy(TxnVerifyPolicy other, IConfigProvider configProvider) : this(other)
		{
			if (configProvider == null)
			{
				return;
			}

			if (configProvider.ConfigurationData == null)
			{
				return;
			}

			var txn_verify = configProvider.ConfigurationData.dynamicConfig.txn_verify;
			if (txn_verify == null)
			{
				return;
			}

			if (txn_verify.read_mode_ap.HasValue)
			{
				this.readModeAP = txn_verify.read_mode_ap.Value;
			}
			if (txn_verify.read_mode_sc.HasValue)
			{
				this.readModeSC = txn_verify.read_mode_sc.Value;
			}
			if (txn_verify.replica.HasValue)
			{
				this.replica = txn_verify.replica.Value;
			}
			if (txn_verify.sleep_between_retries.HasValue)
			{
				this.sleepBetweenRetries = txn_verify.sleep_between_retries.Value;
			}
			if (txn_verify.socket_timeout.HasValue)
			{
				this.socketTimeout = txn_verify.socket_timeout.Value;
			}
			if (txn_verify.timeout_delay.HasValue)
			{
				this.TimeoutDelay = txn_verify.timeout_delay.Value;
			}
			if (txn_verify.total_timeout.HasValue)
			{
				this.totalTimeout = txn_verify.total_timeout.Value;
			}
			if (txn_verify.max_retries.HasValue)
			{
				this.maxRetries = txn_verify.max_retries.Value;
			}
			if (txn_verify.max_concurrent_threads.HasValue)
			{
				this.maxConcurrentThreads = txn_verify.max_concurrent_threads.Value;
			}
			if (txn_verify.allow_inline.HasValue)
			{
				this.allowInline = txn_verify.allow_inline.Value;
			}
			if (txn_verify.allow_inline_ssd.HasValue)
			{
				this.allowInlineSSD = txn_verify.allow_inline_ssd.Value;
			}
			if (txn_verify.respond_all_keys.HasValue)
			{
				this.respondAllKeys = txn_verify.respond_all_keys.Value;
			}
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public TxnVerifyPolicy()
		{
			readModeSC = ReadModeSC.LINEARIZE;
			replica = Replica.MASTER;
			maxRetries = 5;
			socketTimeout = 3000;
			totalTimeout = 10000;
			sleepBetweenRetries = 1000;
		}

		/// <summary>
		/// Creates a deep copy of this txn verify policy.
		/// </summary>
		/// <returns></returns>
		public new TxnVerifyPolicy Clone()
		{
			return new TxnVerifyPolicy(this);
		}
	}
}
