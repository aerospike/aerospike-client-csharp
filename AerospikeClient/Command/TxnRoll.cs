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

using static Aerospike.Client.CommitStatus;
using static Aerospike.Client.CommitError;
using static Aerospike.Client.AbortStatus;

namespace Aerospike.Client
{
	public sealed class TxnRoll
	{
		private readonly Cluster cluster;
		private readonly Txn txn;
		private BatchRecord[] verifyRecords;
		private BatchRecord[] rollRecords;

		public TxnRoll(Cluster cluster, Txn txn)
		{
			this.cluster = cluster;
			this.txn = txn;
		}

		public void Verify(BatchPolicy verifyPolicy, BatchPolicy rollPolicy)
		{
			WritePolicy writePolicy;
			Key txnKey;

			try
			{
				// Verify read versions in batch.
				VerifyRecordVersions(verifyPolicy);
			}
			catch (Exception e)
			{
				// Verify failed. Abort.
				txn.State = Txn.TxnState.ABORTED;
				try
				{
					Roll(rollPolicy, Command.INFO4_TXN_ROLL_BACK);
				}
				catch (Exception e2)
				{
					// Throw combination of verify and roll exceptions.
					throw CreateCommitException(CommitErrorType.VERIFY_FAIL_ABORT_ABANDONED, e, e2);
				}

				if (txn.CloseMonitor())
				{
					try
					{
						writePolicy = new WritePolicy(rollPolicy);
						txnKey = TxnMonitor.GetTxnMonitorKey(txn);
						Close(writePolicy, txnKey);
					}
					catch (Exception e3)
					{
						// Throw combination of verify and close exceptions.
						throw CreateCommitException(CommitErrorType.VERIFY_FAIL_CLOSE_ABANDONED, e, e3);
					}
				}

				// Throw original exception when abort succeeds.
				throw CreateCommitException(CommitErrorType.VERIFY_FAIL, e);
			}

			txn.State = Txn.TxnState.VERIFIED;
		}

		public CommitStatusType Commit(BatchPolicy rollPolicy)
		{
			var writePolicy = new WritePolicy(rollPolicy);
			var txnKey = TxnMonitor.GetTxnMonitorKey(txn);

			if (txn.MonitorExists())
			{
				// Tell transaction monitor that a roll-forward will commence.
				try
				{
					MarkRollForward(writePolicy, txnKey);
				}
				catch (AerospikeException ae)
				{
					AerospikeException.Commit aec = CreateCommitException(CommitErrorType.MARK_ROLL_FORWARD_ABANDONED, ae);

					if (ae.Result == ResultCode.MRT_ABORTED)
					{
						aec.SetInDoubt(false);
						txn.InDoubt = false;
						txn.State = Txn.TxnState.ABORTED;
					}
					else if (txn.InDoubt)
					{
						// The transaction was already InDoubt and just failed again,
						// so the new exception should also be InDoubt.
						aec.SetInDoubt(true);
					}
					else if (ae.InDoubt)
					{
						// The current exception is inDoubt.
						aec.SetInDoubt(true);
						txn.InDoubt = true;
					}
					throw aec;
				}
				catch (Exception e)
				{
					AerospikeException.Commit aec = CreateCommitException(CommitErrorType.MARK_ROLL_FORWARD_ABANDONED, e);
					
					if (txn.InDoubt)
					{
						aec.SetInDoubt(true);
					}

					throw aec;
				}

				txn.State = Txn.TxnState.COMMITTED;
				txn.InDoubt = false;

				// Roll-forward writes in batch.
				try
				{
					Roll(rollPolicy, Command.INFO4_TXN_ROLL_FORWARD);
				}
				catch (Exception)
				{
					return CommitStatusType.ROLL_FORWARD_ABANDONED;
				}
			}

			if (txn.CloseMonitor())
			{
				// Remove transaction monitor.
				try
				{
					Close(writePolicy, txnKey);
				}
				catch (Exception)
				{
					return CommitStatusType.CLOSE_ABANDONED;
				}
			}

			return CommitStatusType.OK;
		}

		private AerospikeException.Commit CreateCommitException(CommitErrorType error, Exception cause, Exception innerException = null) 
		{
			AerospikeException.Commit aec;

			if (innerException != null)
			{
				if (cause == null)
				{
					aec = new AerospikeException.Commit(error, verifyRecords, rollRecords, innerException);
				}
				else
				{
					aec = new AerospikeException.Commit(error, verifyRecords, rollRecords, new[] { cause, innerException });
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

			if (cause is AerospikeException)
			{
				AerospikeException src = (AerospikeException)cause;
				aec.Node = src.Node;
				aec.Policy = src.Policy;
				aec.Iteration = src.Iteration;
				aec.SetInDoubt(src.InDoubt);
			}
			return aec;
		}

		public AbortStatusType Abort(BatchPolicy rollPolicy)
		{
			txn.State = Txn.TxnState.ABORTED;
			
			try
			{
				Roll(rollPolicy, Command.INFO4_TXN_ROLL_BACK);
			}
			catch (Exception)
			{
				return AbortStatusType.ROLL_BACK_ABANDONED;
			}

			if (txn.CloseMonitor())
			{
				try
				{
					WritePolicy writePolicy = new(rollPolicy);
					Key txnKey = TxnMonitor.GetTxnMonitorKey(txn);
					Close(writePolicy, txnKey);
				}
				catch (Exception)
				{
					return AbortStatusType.CLOSE_ABANDONED;
				}
			}

			return AbortStatusType.OK;
		}
		private void VerifyRecordVersions(BatchPolicy verifyPolicy)
		{
			// Validate record versions in a batch.
			BatchRecord[] records = null;
			Key[] keys = null;
			long?[] versions = null;

			bool actionPerformed = txn.Reads.PerformReadActionOnEachElement(max =>
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
				return;
			}

			this.verifyRecords = records;

			BatchStatus status = new(true);
			List<BatchNode> bns = BatchNode.GenerateList(cluster, verifyPolicy, keys, records, false, status);
			BatchCommand[] commands = new BatchCommand[bns.Count];

			int count = 0;

			foreach (BatchNode bn in bns)
			{
				commands[count++] = new BatchTxnVerify(
					cluster, bn, verifyPolicy, keys, versions, records, status);
			}

			BatchExecutor.Execute(cluster, verifyPolicy, commands, status);

			if (!status.GetStatus())
			{
				throw new AerospikeException("Failed to verify one or more record versions");
			}
		}

		private void MarkRollForward(WritePolicy writePolicy, Key txnKey)
		{
			// Tell transaction monitor that a roll-forward will commence.
			TxnMarkRollForward cmd = new(cluster, txn, writePolicy, txnKey);
			cmd.Execute();
		}

		private void Roll(BatchPolicy rollPolicy, int txnAttr)
		{
			BatchRecord[] records = null;
			Key[] keys = null;

			bool actionPerformed = txn.Writes.PerformActionOnEachElement(max =>
			{
				if (max == 0) return false;

				records = new BatchRecord[max];
				keys = new Key[max];
				return true;
			},
			(item, count) =>
			{
				keys[count] = item;
				records[count] = new BatchRecord(item, true);
			});

			if (!actionPerformed)
			{
				return;
			}

			this.rollRecords = records;

			BatchAttr attr = new();
			attr.SetTxn(txnAttr);
			BatchStatus status = new(true);

			// generate() requires a null transaction instance.
			List<BatchNode> bns = BatchNode.GenerateList(cluster, rollPolicy, keys, records, true, status);
			BatchCommand[] commands = new BatchCommand[bns.Count];

			int count = 0;

			foreach (BatchNode bn in bns)
			{
				commands[count++] = new BatchTxnRoll(
					cluster, bn, rollPolicy, txn, keys, records, attr, status);
			}
			BatchExecutor.Execute(cluster, rollPolicy, commands, status);

			if (!status.GetStatus())
			{
				string rollString = txnAttr == Command.INFO4_TXN_ROLL_FORWARD ? "commit" : "abort";
				throw new AerospikeException("Failed to " + rollString + " one or more records");
			}
		}

		private void Close(WritePolicy writePolicy, Key txnKey)
		{
			// Delete transaction monitor on server.
			TxnClose cmd = new(cluster, txn, writePolicy, txnKey);
			cmd.Execute();

			// Reset transaction on client.
			txn.Clear();
		}
	}
}
