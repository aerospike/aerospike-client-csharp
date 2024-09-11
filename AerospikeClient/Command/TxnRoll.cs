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

		public CommitStatusType Commit(BatchPolicy verifyPolicy, BatchPolicy rollPolicy)
		{
			WritePolicy writePolicy;
			Key txnKey;

			try
			{
				// Verify read versions in batch.
				Verify(verifyPolicy);
			}
			catch (Exception e)
			{
				// Verify failed. Abort.
				try
				{
					Roll(rollPolicy, Command.INFO4_MRT_ROLL_BACK);
				}
				catch (Exception e2)
				{
					// Throw combination of verify and roll exceptions.
					
					throw OnCommitError(CommitErrorType.VERIFY_FAIL_ABORT_ABANDONED, e, false);
				}

				if (txn.MonitorMightExist())
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
						//t.AddSuppressed(t3);
						throw OnCommitError(CommitErrorType.VERIFY_FAIL_CLOSE_ABANDONED, e, false);
					}
				}

				// Throw original exception when abort succeeds.
				throw OnCommitError(CommitErrorType.VERIFY_FAIL, e, false);
			}

			writePolicy = new WritePolicy(rollPolicy);
			txnKey = TxnMonitor.GetTxnMonitorKey(txn);

			if (txn.MonitorExists())
			{
				// Tell MRT monitor that a roll-forward will commence.
				try
				{
					MarkRollForward(writePolicy, txnKey);
				}
				catch (Exception e)
				{
					throw OnCommitError(CommitErrorType.MARK_ROLL_FORWARD_ABANDONED, e, true);
				}

				// Roll-forward writes in batch.
				try
				{
					Roll(rollPolicy, Command.INFO4_MRT_ROLL_FORWARD);
				}
				catch (Exception)
				{
					return CommitStatusType.ROLL_FORWARD_ABANDONED;
				}
			}

			if (txn.MonitorMightExist())
			{
				// Remove MRT monitor.
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

		private AerospikeException.Commit OnCommitError(CommitErrorType error, Exception cause, bool setInDoubt) 
		{
			AerospikeException.Commit aec = new(error, verifyRecords, rollRecords, cause);
			
			if (cause is AerospikeException) {
				AerospikeException src = (AerospikeException)cause;
				aec.Node = src.Node;
				aec.Policy = src.Policy;
				aec.Iteration = src.Iteration;
				if (setInDoubt) {
					aec.SetInDoubt(src.InDoubt);
				}
			}
			return aec;
		}

		public AbortStatusType Abort(BatchPolicy rollPolicy)
		{
			try
			{
				Roll(rollPolicy, Command.INFO4_MRT_ROLL_BACK);
			}
			catch (Exception)
			{
				return AbortStatusType.ROLL_BACK_ABANDONED;
			}

			if (txn.MonitorMightExist())
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
		private void Verify(BatchPolicy verifyPolicy)
		{
			// Validate record versions in a batch.
			HashSet<KeyValuePair<Key, long>> reads = txn.Reads.ToHashSet<KeyValuePair<Key, long>>();
			int max = reads.Count;
			if (max == 0)
			{
				return;
			}

			BatchRecord[] records = new BatchRecord[max];
			Key[] keys = new Key[max];
			long?[] versions = new long?[max];
			int count = 0;

			foreach (KeyValuePair<Key, long> entry in reads)
			{
				Key key = entry.Key;
				keys[count] = key;
				records[count] = new BatchRecord(key, false);
				versions[count] = entry.Value;
				count++;
			}
			this.verifyRecords = records;

			BatchStatus status = new(true);
			List<BatchNode> bns = BatchNode.GenerateList(cluster, verifyPolicy, keys, records, false, status);
			BatchCommand[] commands = new BatchCommand[bns.Count];

			count = 0;

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
			// Tell MRT monitor that a roll-forward will commence.
			TxnMarkRollForward cmd = new(cluster, txn, writePolicy, txnKey);
			cmd.Execute();
		}

		private void Roll(BatchPolicy rollPolicy, int txnAttr)
		{
			HashSet<Key> keySet = txn.Writes;

			if (keySet.Count == 0)
			{
				return;
			}

			Key[] keys = keySet.ToArray<Key>();
			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], true);
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
				string rollString = txnAttr == Command.INFO4_MRT_ROLL_FORWARD ? "commit" : "abort";
				throw new AerospikeException("Failed to " + rollString + " one or more records");
			}
		}

		private void Close(WritePolicy writePolicy, Key txnKey)
		{
			// Delete MRT monitor on server.
			TxnClose cmd = new(cluster, txn, writePolicy, txnKey);
			cmd.Execute();

			// Reset MRT on client.
			txn.Clear();
		}
	}
}
