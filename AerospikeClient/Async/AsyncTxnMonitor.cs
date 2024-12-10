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
	public abstract class AsyncTxnMonitor
	{
		public static void Execute(AsyncCluster cluster, WritePolicy policy, AsyncWriteBase command)
		{
			if (policy.Txn == null)
			{
				// Command is not run under a MRT monitor. Run original command.
				command.Execute();
				return;
			}

			Txn txn = policy.Txn;
			Key cmdKey = command.Key;

			if (txn.Writes.Contains(cmdKey))
			{
				// MRT monitor already contains this key. Run original command.
				command.Execute();
				return;
			}

			// Add key to MRT monitor and then run original command.
			Operation[] ops = TxnMonitor.GetTxnOps(txn, cmdKey);
			SingleTxnMonitor stm = new(cluster, command);
			stm.Execute(cluster, policy, ops);
		}

		public static void ExecuteBatch(
			BatchPolicy policy,
			AsyncBatchExecutor executor,
			Key[] keys
		)
		{
			if (policy.Txn == null)
			{
				// Command is not run under a MRT monitor. Run original command.
				executor.Execute(executor.commands);
				return;
			}

			// Add write keys to MRT monitor and then run original command.
			Operation[] ops = TxnMonitor.GetTxnOps(policy.Txn, keys);
			BatchTxnMonitor ate = new(executor);
			ate.Execute(executor.cluster, policy, ops);
		}

		public static void ExecuteBatch(
			BatchPolicy policy,
			AsyncBatchExecutor executor,
			List<BatchRecord> records
		)
		{
			if (policy.Txn == null)
			{
				// Command is not run under a MRT monitor. Run original command.
				executor.Execute();
				return;
			}

			// Add write keys to MRT monitor and then run original command.
			Operation[] ops = TxnMonitor.GetTxnOps(policy.Txn, records);

			if (ops == null)
			{
				// Readonly batch does not need to add key digests. Run original command.
				executor.Execute();
				return;
			}

			BatchTxnMonitor ate = new(executor);
			ate.Execute(executor.cluster, policy, ops);
		}

		public sealed class SingleTxnMonitor : AsyncTxnMonitor
		{
			public SingleTxnMonitor(AsyncCluster cluster, AsyncWriteBase command)
				: base(command, cluster)
			{
			}

			public override void RunCommand()
			{
				command.Execute();
			}

			public override void OnFailure(AerospikeException ae)
			{
				command.OnFailure(ae);
			}
		}

		public sealed class BatchTxnMonitor : AsyncTxnMonitor
		{
			private readonly AsyncBatchExecutor executor;

			public BatchTxnMonitor(AsyncBatchExecutor executor)
				: base(null, null)
			{
				this.executor = executor;
			}

			public override void RunCommand()
			{
				executor.Execute();
			}

			public override void OnFailure(AerospikeException ae)
			{
				executor.OnFailure(ae);
			}
		}

		readonly AsyncCommand command;
		readonly AsyncCluster cluster;

		private AsyncTxnMonitor(AsyncCommand command, AsyncCluster cluster)
		{
			this.command = command;
			this.cluster = cluster;
		}

		void Execute(AsyncCluster cluster, Policy policy, Operation[] ops)
		{
			Txn txn = policy.Txn;
			Key txnKey = TxnMonitor.GetTxnMonitorKey(policy.Txn);
			WritePolicy wp = TxnMonitor.CopyTimeoutPolicy(policy);

			ExecuteRecordListener txnListener = new(this);

			// Add write key(s) to MRT monitor.
			OperateArgs args = new(wp, null, null, ops);
			AsyncTxnAddKeys txnCommand = new(cluster, txnListener, txnKey, args, txn);
			txnCommand.Execute();
		}

		private void NotifyFailure(AerospikeException ae)
		{
			try
			{
				OnFailure(ae);
			}
			catch (Exception t)
			{
				Log.Error("notifyCommandFailure onFailure() failed: " + t.StackTrace);
			}
		}

		public abstract void OnFailure(AerospikeException ae);
		public abstract void RunCommand();

		private sealed class ExecuteRecordListener : RecordListener
		{
			private readonly AsyncTxnMonitor monitor;

			public ExecuteRecordListener(AsyncTxnMonitor monitor)
			{
				this.monitor = monitor;
			}

			public void OnSuccess(Key key, Record record)
			{
				try
				{
					// Run original command.
					monitor.RunCommand();
				}
				catch (AerospikeException ae)
				{
					monitor.NotifyFailure(ae);
				}
				catch (Exception t)
				{
					monitor.NotifyFailure(new AerospikeException(t));
				}
			}

			public void OnFailure(AerospikeException ae)
			{
				monitor.NotifyFailure(new AerospikeException(ResultCode.TXN_FAILED, "Failed to add key(s) to MRT monitor", ae));
			}
		}
	}
}

