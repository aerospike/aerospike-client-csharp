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
	/// Transaction policy fields used to batch roll forward/backward records on
	/// commit or abort.Used a placeholder for now as there are no additional fields beyond BatchPolicy.
	/// </summary>
	public sealed class TxnRollPolicy : BatchPolicy
	{
		/// <summary>
		/// Copy policy from another policy.
		/// </summary>
		public TxnRollPolicy(TxnRollPolicy other) :
			base(other)
		{
		}

		public TxnRollPolicy(TxnRollPolicy other, IConfigProvider configProvider) : this(other)
		{
			if (configProvider == null)
			{
				return;
			}

			if (configProvider.ConfigurationData == null)
			{
				return;
			}

			var txn_roll = configProvider.ConfigurationData.dynamicConfig.txn_roll;
			if (txn_roll == null)
			{
				return;
			}

			if (txn_roll.read_mode_ap.HasValue)
			{
				this.readModeAP = txn_roll.read_mode_ap.Value;
			}
			if (txn_roll.read_mode_sc.HasValue)
			{
				this.readModeSC = txn_roll.read_mode_sc.Value;
			}
			if (txn_roll.replica.HasValue)
			{
				this.replica = txn_roll.replica.Value;
			}
			if (txn_roll.sleep_between_retries.HasValue)
			{
				this.sleepBetweenRetries = txn_roll.sleep_between_retries.Value;
			}
			if (txn_roll.socket_timeout.HasValue)
			{
				this.socketTimeout = txn_roll.socket_timeout.Value;
			}
			if (txn_roll.timeout_delay.HasValue)
			{
				this.TimeoutDelay = txn_roll.timeout_delay.Value;
			}
			if (txn_roll.total_timeout.HasValue)
			{
				this.totalTimeout = txn_roll.total_timeout.Value;
			}
			if (txn_roll.max_retries.HasValue)
			{
				this.maxRetries = txn_roll.max_retries.Value;
			}
			if (txn_roll.max_concurrent_threads.HasValue)
			{
				this.maxConcurrentThreads = txn_roll.max_concurrent_threads.Value;
			}
			if (txn_roll.allow_inline.HasValue)
			{
				this.allowInline = txn_roll.allow_inline.Value;
			}
			if (txn_roll.allow_inline_ssd.HasValue)
			{
				this.allowInlineSSD = txn_roll.allow_inline_ssd.Value;
			}
			if (txn_roll.respond_all_keys.HasValue)
			{
				this.respondAllKeys = txn_roll.respond_all_keys.Value;
			}
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public TxnRollPolicy()
		{
			replica = Replica.MASTER;
			maxRetries = 5;
			socketTimeout = 3000;
			totalTimeout = 10000;
			sleepBetweenRetries = 1000;
		}

		/// <summary>
		/// Creates a deep copy of this txn roll policy.
		/// </summary>
		/// <returns></returns>
		public new TxnRollPolicy Clone()
		{
			return new TxnRollPolicy(this);
		}
	}
}
