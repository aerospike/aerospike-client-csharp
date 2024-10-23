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
using static Aerospike.Client.AbortStatus;
using static Aerospike.Client.CommitError;
using static Aerospike.Client.CommitStatus;

namespace Aerospike.Client
{
	public sealed class AsyncTxnRoll
	{
		private readonly AsyncCluster cluster;
		private readonly BatchPolicy verifyPolicy;
		private readonly BatchPolicy rollPolicy;
		private readonly WritePolicy writePolicy;
		private readonly Txn txn;
		private readonly Key txnKey;
		private CommitListener commitListener;
		private AbortListener abortListener;
		private BatchRecord[] verifyRecords;
		private BatchRecord[] rollRecords;
		private AerospikeException verifyException;

		public AsyncTxnRoll
		(
			AsyncCluster cluster,
			BatchPolicy verifyPolicy,
			BatchPolicy rollPolicy,
			Txn txn
		)
		{
			this.cluster = cluster;
			this.verifyPolicy = verifyPolicy;
			this.rollPolicy = rollPolicy;
			this.writePolicy = new WritePolicy(rollPolicy);
			this.txn = txn;
			this.txnKey = TxnMonitor.GetTxnMonitorKey(txn);
		}

		public void Verify(CommitListener listener)
		{
			commitListener = listener;
			Verify(new VerifyListener(this));
		}

		public void Commit(CommitListener listener)
		{
			commitListener = listener;
			Commit();
		}

		private void Commit()
		{
			if (txn.MonitorExists())
			{
				MarkRollForward();
			}
			else
			{
				txn.State = Txn.TxnState.COMMITTED;
				CloseOnCommit(true);
			}
		}

		public void Abort(AbortListener listener)
		{
			abortListener = listener;
			txn.State = Txn.TxnState.ABORTED;

			Roll(new RollListener(this), Command.INFO4_MRT_ROLL_BACK);
		}

		private void Verify(BatchRecordArrayListener verifyListener)
		{
			// Validate record versions in a batch.
			int count = 0;
			BatchRecord[] records = null;
			Key[] keys = null;
			long?[] versions = null;

			bool actionPerformed = txn.Reads.PerformActionOnEachElement(max =>
			{
				if (max == 0) return false;

				records = new BatchRecord[max];
				keys = new Key[max];
				versions = new long?[max];
				return true;
			},
			(key, value, count) =>
			{
				keys[count] = key;
				records[count] = new BatchRecord(key, false);
				versions[count] = value;
			});

			if (!actionPerformed) // If no action was performed, there are no elements. Return.
			{
				verifyListener.OnSuccess(new BatchRecord[0], true);
				return;
			}

			this.verifyRecords = records;

			AsyncBatchTxnVerifyExecutor executor = new(cluster, verifyPolicy, verifyListener, keys, versions, records);
			executor.Execute();
		}

		private void MarkRollForward()
		{
			// Tell MRT monitor that a roll-forward will commence.
			try
			{
				MarkRollForwardListener writeListener = new(this);
				AsyncTxnMarkRollForward command = new(cluster, writeListener, writePolicy, txnKey);
				command.Execute();
			}
			catch (Exception t) 
			{
				NotifyCommitFailure(CommitErrorType.MARK_ROLL_FORWARD_ABANDONED, t, false);
			}
		}

		private void RollForward()
		{
			try
			{
				RollForwardListener rollListener = new(this);
				Roll(rollListener, Command.INFO4_MRT_ROLL_FORWARD);
			}
			catch (Exception)
			{
				NotifyCommitSuccess(CommitStatusType.ROLL_FORWARD_ABANDONED);
			}
		}

		private void RollBack()
		{
			try
			{
				RollForwardListener rollListener = new(this);
				Roll(rollListener, Command.INFO4_MRT_ROLL_BACK);
			}
			catch (Exception t)
			{
				NotifyCommitFailure(CommitErrorType.VERIFY_FAIL_ABORT_ABANDONED, t, false);
			}
		}

		private void Roll(BatchRecordArrayListener rollListener, int txnAttr)
		{
			HashSet<Key> keySet = txn.Writes;

			if (keySet.Count == 0)
			{
				rollListener.OnSuccess(new BatchRecord[0], true);
				return;
			}

			Key[] keys = keySet.ToArray<Key>();
			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], true);
			}

			BatchAttr attr = new();
			attr.SetTxn(txnAttr);

			AsyncBatchTxnRollExecutor executor = new(cluster, rollPolicy, rollListener, txn, keys, records, attr);
			executor.Execute();
		}

		private void CloseOnCommit(bool verified)
		{
			if (!txn.MonitorMightExist())
			{
				if (verified)
				{
					NotifyCommitSuccess(CommitStatusType.OK);
				}
				else
				{
					NotifyCommitFailure(CommitErrorType.VERIFY_FAIL, null, false);
				}
				return;
			}

			try
			{
				AsyncTxnClose command = new(cluster, txn, new CloseOnCommitListener(this, verified), writePolicy, txnKey);
				command.Execute();
			}
			catch (Exception e) 
			{
				if (verified) {
					NotifyCommitSuccess(CommitStatusType.CLOSE_ABANDONED);
				}
				else {
					NotifyCommitFailure(CommitErrorType.VERIFY_FAIL_CLOSE_ABANDONED, e, false);
				}
			}
		}

		private void CloseOnAbort()
		{
			if (!txn.MonitorMightExist())
			{
				// There is no MRT monitor record to remove.
				NotifyAbortSuccess(AbortStatusType.OK);
				return;
			}

			try
			{
				CloseOnAbortListener deleteListener = new(this);
				AsyncTxnClose command = new(cluster, txn, deleteListener, writePolicy, txnKey);
				command.Execute();
			}
			catch (Exception) 
			{
				NotifyAbortSuccess(AbortStatusType.CLOSE_ABANDONED);
			}
		}

		private void NotifyCommitSuccess(CommitStatusType status)
		{
			txn.Clear();

			try
			{
				commitListener.OnSuccess(status);
			}
			catch (Exception t)
			{
				Log.Error("CommitListener onSuccess() failed: " + t.StackTrace);
			}
		}

		private void NotifyCommitFailure(CommitErrorType error, Exception cause, bool setInDoubt)
		{
			try
			{
				AerospikeException.Commit aec;

				if (verifyException != null)
				{
					if (cause == null)
					{
						aec = new AerospikeException.Commit(error, verifyRecords, rollRecords, verifyException);
					}
					else
					{
						aec = new AerospikeException.Commit(error, verifyRecords, rollRecords, new[] { cause, verifyException });
					}
				}
				else if (cause != null)
				{
					aec = new AerospikeException.Commit(error, verifyRecords, rollRecords, cause);
				}
				else
				{
					aec = new AerospikeException.Commit(error, verifyRecords, rollRecords);
				}

				if (cause is AerospikeException) {
					AerospikeException src = (AerospikeException)cause;
					aec.Node = src.Node;
					aec.Policy = src.Policy;
					aec.Iteration = src.Iteration;

					if (setInDoubt)
					{
						aec.SetInDoubt(src.InDoubt);
					}
				}

				commitListener.OnFailure(aec);
			}
			catch (Exception t)
			{
				Log.Error("CommitListener onFailure() failed: " + t.StackTrace);
			}
		}

		private void NotifyAbortSuccess(AbortStatusType status)
		{
			txn.Clear();

			try
			{
				abortListener.OnSuccess(status);
			}
			catch (Exception t)
			{
				Log.Error("AbortListener onSuccess() failed: " + t.StackTrace);
			}
		}

		private sealed class VerifyListener : BatchRecordArrayListener
		{
			private readonly AsyncTxnRoll command;
			
			public VerifyListener(AsyncTxnRoll command) 
			{ 
				this.command = command;
			}
			
			public void OnSuccess(BatchRecord[] records, bool status)
			{
				command.verifyRecords = records;

				if (status)
				{
					command.txn.State = Txn.TxnState.VERIFIED;
					command.Commit();
				}
				else
				{
					command.txn.State = Txn.TxnState.ABORTED;
					command.RollBack();
				}
			}

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				command.verifyRecords = records;
				command.verifyException = ae;
				command.RollBack();
			}
		};

		private sealed class RollListener : BatchRecordArrayListener 
		{
			private readonly AsyncTxnRoll command;

			public RollListener(AsyncTxnRoll command)
			{
				this.command = command;
			}

			public void OnSuccess(BatchRecord[] records, bool status)
			{
				command.rollRecords = records;

				if (status)
				{
					command.CloseOnAbort();
				}
				else
				{
					command.NotifyAbortSuccess(AbortStatusType.ROLL_BACK_ABANDONED);
				}
			}
		
			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				command.rollRecords = records;
				command.NotifyAbortSuccess(AbortStatusType.ROLL_BACK_ABANDONED);
			}
		};

		private sealed class MarkRollForwardListener : WriteListener
		{
			private readonly AsyncTxnRoll command;

			public MarkRollForwardListener(AsyncTxnRoll command)
			{
				this.command = command;
			}

			public void OnSuccess(Key key)
			{
				command.txn.State = Txn.TxnState.VERIFIED;
				command.RollForward();
			}

			public void OnFailure(AerospikeException ae)
			{
				command.NotifyCommitFailure(CommitErrorType.MARK_ROLL_FORWARD_ABANDONED, ae, true);
			}
		};

		private sealed class RollForwardListener : BatchRecordArrayListener
		{
			private readonly AsyncTxnRoll command;

			public RollForwardListener(AsyncTxnRoll command)
			{
				this.command = command;
			}

			public void OnSuccess(BatchRecord[] records, bool status)
			{
				command.rollRecords = records;

				if (status)
				{
					command.CloseOnCommit(true);
				}
				else
				{
					command.NotifyCommitSuccess(CommitStatusType.ROLL_FORWARD_ABANDONED);
				}
			}

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				command.rollRecords = records;
				command.NotifyCommitSuccess(CommitStatusType.ROLL_FORWARD_ABANDONED);
			}
		};

		private sealed class RollBackListener : BatchRecordArrayListener
		{
			private readonly AsyncTxnRoll command;

			public RollBackListener(AsyncTxnRoll command)
			{
				this.command = command;
			}

			public void OnSuccess(BatchRecord[] records, bool status)
			{
				command.rollRecords = records;

				if (status)
				{
					command.CloseOnCommit(false);
				}
				else
				{
					command.NotifyCommitFailure(CommitErrorType.VERIFY_FAIL_ABORT_ABANDONED, null, false);
				}
			}

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				command.rollRecords = records;
				command.NotifyCommitFailure(CommitErrorType.VERIFY_FAIL_ABORT_ABANDONED, ae, false);
			}
		};


		private sealed class CloseOnCommitListener : DeleteListener
		{
			private readonly AsyncTxnRoll command;
			private readonly bool verified;

			public CloseOnCommitListener(AsyncTxnRoll command, bool verified)
			{
				this.command = command;
				this.verified = verified;
			}

			public void OnSuccess(Key key, bool existed)
			{
				if (verified)
				{
					command.NotifyCommitSuccess(CommitStatusType.OK);
				}
				else
				{
					command.NotifyCommitFailure(CommitErrorType.VERIFY_FAIL, null, false);
				}
			}

			public void OnFailure(AerospikeException ae)
			{
				if (verified)
				{
					command.NotifyCommitSuccess(CommitStatusType.CLOSE_ABANDONED);
				}
				else
				{
					command.NotifyCommitFailure(CommitErrorType.VERIFY_FAIL_CLOSE_ABANDONED, ae, false);
				}
			}
		};
		private sealed class CloseOnAbortListener : DeleteListener
		{
			private readonly AsyncTxnRoll command;

			public CloseOnAbortListener(AsyncTxnRoll command)
			{
				this.command = command;
			}

			public void OnSuccess(Key key, bool existed)
			{
				command.NotifyAbortSuccess(AbortStatusType.OK);
			}

			public void OnFailure(AerospikeException ae)
			{
				command.NotifyAbortSuccess(AbortStatusType.CLOSE_ABANDONED);
			}
		};
	}
}

