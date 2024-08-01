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

using static Aerospike.Client.CommitError;
using static Aerospike.Client.AbortError;

namespace Aerospike.Client
{
	public sealed class TranRoll
	{
		private readonly Cluster cluster;
		private readonly Tran tran;
		private BatchRecord[] verifyRecords;
		private BatchRecord[] rollRecords;

		public TranRoll(Cluster cluster, Tran tran)
		{
			this.cluster = cluster;
			this.tran = tran;
		}

		public void Commit(BatchPolicy verifyPolicy, BatchPolicy rollPolicy)
		{
			WritePolicy writePolicy;
			Key tranKey;

			try
			{
				// Verify read versions in batch.
				Verify(verifyPolicy);
			}
			catch (Exception t)
			{
				// Verify failed. Abort.
				try
				{
					Roll(rollPolicy, Command.INFO4_MRT_ROLL_BACK);
				}
				catch (Exception t2)
				{
					// Throw combination of verify and roll exceptions.
					//t.InnerException = t2; //TODO: Ask about this
					throw new AerospikeException.Commit(CommitError.CommitErrorType.VERIFY_FAIL_ABORT_ABANDONED, verifyRecords, rollRecords, t);
				}

				if (tran.Deadline != 0)
				{
					try
					{
						writePolicy = new WritePolicy(rollPolicy);
						tranKey = TranMonitor.GetTranMonitorKey(tran);
						Close(writePolicy, tranKey);
					}
					catch (Exception t3)
					{
						// Throw combination of verify and close exceptions.
						//t.AddSuppressed(t3);
						throw new AerospikeException.Commit(CommitErrorType.VERIFY_FAIL_CLOSE_ABANDONED, verifyRecords, rollRecords, t);
					}
				}

				// Throw original exception when abort succeeds.
				throw new AerospikeException.Commit(CommitErrorType.VERIFY_FAIL, verifyRecords, rollRecords, t);
			}

			writePolicy = new WritePolicy(rollPolicy);
			tranKey = TranMonitor.GetTranMonitorKey(tran);
			HashSet<Key> keySet = tran.Writes;

			if (keySet.Count != 0)
			{
				// Tell MRT monitor that a roll-forward will commence.
				try
				{
					MarkRollForward(writePolicy, tranKey);
				}
				catch (Exception t)
				{
					throw new AerospikeException.Commit(CommitErrorType.MARK_ROLL_FORWARD_ABANDONED, verifyRecords, rollRecords, t);
				}

				// Roll-forward writes in batch.
				try
				{
					Roll(rollPolicy, Command.INFO4_MRT_ROLL_FORWARD);
				}
				catch (Exception t)
				{
					throw new AerospikeException.Commit(CommitErrorType.ROLL_FORWARD_ABANDONED, verifyRecords, rollRecords, t);
				}
			}

			if (tran.Deadline != 0)
			{
				// Remove MRT monitor.
				try
				{
					Close(writePolicy, tranKey);
				}
				catch (Exception t)
				{
					throw new AerospikeException.Commit(CommitErrorType.CLOSE_ABANDONED, verifyRecords, rollRecords, t);
				}
			}
		}

		public void Abort(BatchPolicy rollPolicy)
		{
			HashSet<Key> keySet = tran.Writes;

			if (keySet.Count != 0)
			{
				try
				{
					Roll(rollPolicy, Command.INFO4_MRT_ROLL_BACK);
				}
				catch (Exception t)
				{
					throw new AerospikeException.Abort(AbortErrorType.ROLL_BACK_ABANDONED, rollRecords, t);
				}
			}

			if (tran.Deadline != 0)
			{
				try
				{
					WritePolicy writePolicy = new(rollPolicy);
					Key tranKey = TranMonitor.GetTranMonitorKey(tran);
					Close(writePolicy, tranKey);
				}
				catch (Exception t)
				{
					throw new AerospikeException.Abort(AbortErrorType.CLOSE_ABANDONED, rollRecords, t);
				}
			}
		}
		private void Verify(BatchPolicy verifyPolicy)
		{
			// Validate record versions in a batch.
			HashSet<KeyValuePair<Key, long>> reads = tran.Reads.ToHashSet<KeyValuePair<Key, long>>();
			int max = reads.Count;
			if (max == 0)
			{
				return;
			}

			BatchRecord[] records = new BatchRecord[max];
			Key[] keys = new Key[max];
			long[] versions = new long[max];
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
				commands[count++] = new BatchTranVerify(
					cluster, bn, verifyPolicy, tran, keys, versions, records, status);
			}

			BatchExecutor.Execute(cluster, verifyPolicy, commands, status);

			if (!status.GetStatus())
			{
				throw new AerospikeException("Failed to verify one or more record versions");
			}
		}

		private void MarkRollForward(WritePolicy writePolicy, Key tranKey)
		{
			// Tell MRT monitor that a roll-forward will commence.
			TranMarkRollForward cmd = new(cluster, tran, writePolicy, tranKey);
			cmd.Execute();
		}

		private void Roll(BatchPolicy rollPolicy, int tranAttr)
		{
			HashSet<Key> keySet = tran.Writes;

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

			// Copy tran roll policy because it needs to be modified.
			BatchPolicy batchPolicy = new(rollPolicy);

			BatchAttr attr = new();
			attr.SetTran(tranAttr);
			BatchStatus status = new(true);

			// generate() requires a null tran instance.
			List<BatchNode> bns = BatchNode.GenerateList(cluster, batchPolicy, keys, records, true, status);
			BatchCommand[] commands = new BatchCommand[bns.Count];

			// Batch roll forward requires the tran instance.
			batchPolicy.Tran = tran;

			int count = 0;

			foreach (BatchNode bn in bns)
			{
				commands[count++] = new BatchTranRoll(
					cluster, bn, batchPolicy, keys, records, attr, status);
			}
			BatchExecutor.Execute(cluster, batchPolicy, commands, status);

			if (!status.GetStatus())
			{
				string rollString = tranAttr == Command.INFO4_MRT_ROLL_FORWARD ? "commit" : "abort";
				throw new AerospikeException("Failed to " + rollString + " one or more records");
			}
		}

		private void Close(WritePolicy writePolicy, Key tranKey)
		{
			// Delete MRT monitor on server.
			TranClose cmd = new(cluster, tran, writePolicy, tranKey);
			cmd.Execute();

			// Reset MRT on client.
			tran.Clear();
		}
	}
}
