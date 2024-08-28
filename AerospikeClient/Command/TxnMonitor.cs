/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
	public sealed class TxnMonitor
	{
		private static readonly ListPolicy OrderedListPolicy = new(ListOrder.ORDERED,
			ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);

		private static readonly string BinNameId = "id";
		private static readonly string BinNameDigests = "keyds";

		public static void AddKey(Cluster cluster, WritePolicy policy, Key cmdKey)
		{
			Txn txn = policy.Txn;

			if (txn.Writes.Contains(cmdKey))
			{
				// Transaction monitor already contains this key.
				return;
			}

			Operation[] ops = GetTxnOps(txn, cmdKey);
			AddWriteKeys(cluster, policy, ops);
		}

		public static void AddKeys(Cluster cluster, BatchPolicy policy, Key[] keys)
		{
			Operation[] ops = GetTxnOps(policy.Txn, keys);
			AddWriteKeys(cluster, policy, ops);
		}

		public static void AddKeys(Cluster cluster, BatchPolicy policy, List<BatchRecord> records)
		{
			Operation[] ops = GetTxnOps(policy.Txn, records);

			if (ops != null)
			{
				AddWriteKeys(cluster, policy, ops);
			}
		}

		public static Operation[] GetTxnOps(Txn txn, Key cmdKey)
		{
			txn.SetNamespace(cmdKey.ns);

			if (txn.MonitorExists()) 
			{
				// No existing monitor record.
				return new Operation[] {
					ListOperation.Append(OrderedListPolicy, BinNameDigests, Value.Get(cmdKey.digest))
				};
			}
			else
			{
				return new Operation[] {
					Operation.Put(new Bin(BinNameId, txn.Id)),
					ListOperation.Append(OrderedListPolicy, BinNameDigests, Value.Get(cmdKey.digest))
				};
			}
		}

		public static Operation[] GetTxnOps(Txn txn, Key[] keys)
		{
			List<Value> list = new(keys.Length);

			foreach (Key key in keys) 
			{
				txn.SetNamespace(key.ns);
				list.Add(Value.Get(key.digest));
			}
			return GetTxnOps(txn, list);
		}

		public static Operation[] GetTxnOps(Txn txn, List<BatchRecord> records)
		{
			List<Value> list = new(records.Count);

			foreach (BatchRecord br in records) {
				txn.SetNamespace(br.key.ns);

				if (br.hasWrite) 
				{
					list.Add(Value.Get(br.key.digest));
				}
			}

			if (list.Count == 0)
			{
				// Readonly batch does not need to add key digests.
				return null;
			}
			return GetTxnOps(txn, list);
		}

		private static Operation[] GetTxnOps(Txn txn, List<Value> list)
		{
			if (txn.MonitorExists())
			{
				// No existing monitor record.
				return new Operation[] {
						ListOperation.AppendItems(OrderedListPolicy, BinNameDigests, list)
					};
			}
			else
			{
				return new Operation[] {
						Operation.Put(new Bin(BinNameId, txn.Id)),
						ListOperation.AppendItems(OrderedListPolicy, BinNameDigests, list)
					};
			}
		}

		private static void AddWriteKeys(Cluster cluster, Policy policy, Operation[] ops)
		{
			Key txnKey = GetTxnMonitorKey(policy.Txn);
			WritePolicy wp = CopyTimeoutPolicy(policy);
			OperateArgs args = new(wp, null, null, ops);
			TxnAddKeys cmd = new(cluster, txnKey, args);
			cmd.Execute();
		}

		public static Key GetTxnMonitorKey(Txn txn)
		{
			return new Key(txn.Ns, "<ERO~MRT", txn.Id);
		}

		public static WritePolicy CopyTimeoutPolicy(Policy policy)
		{
			// Inherit some fields from the original command's policy.
			WritePolicy wp = new()
			{
				Txn = policy.Txn,
				socketTimeout = policy.socketTimeout,
				totalTimeout = policy.totalTimeout,
				TimeoutDelay = policy.TimeoutDelay,
				maxRetries = policy.maxRetries,
				sleepBetweenRetries = policy.sleepBetweenRetries,
				compress = policy.compress,
				respondAllOps = true
			};
			return wp;
		}
	}
}
