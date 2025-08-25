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
	public sealed class AsyncBatchRecordArrayExecutor
	(
		AsyncCluster cluster,
		BatchRecordArrayListener listener,
		BatchRecord[] records
	) : AsyncBatchExecutor(cluster, true)
	{
		private readonly BatchRecordArrayListener listener = listener;
		private readonly BatchRecord[] records = records;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(records, GetStatus());
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(records, ae);
		}
	}

	public sealed class AsyncBatchRecordSequenceExecutor
	(
		AsyncCluster cluster,
		BatchRecordSequenceListener listener,
		bool[] sent
	) : AsyncBatchExecutor(cluster, true)
	{
		private readonly BatchRecordSequenceListener listener = listener;
		private readonly bool[] sent = sent;

		public void SetSent(int index)
		{
			sent[index] = true;
		}

		public bool ExchangeSent(int index)
		{
			bool prev = sent[index];
			sent[index] = true;
			return prev;
		}

		public override void BatchKeyError(Cluster cluster, Key key, int index, AerospikeException ae, bool inDoubt, bool hasWrite)
		{
			sent[index] = true;
			BatchRecord record = new(key, null, ae.Result, inDoubt, hasWrite);
			AsyncBatch.OnRecord(cluster, listener, record, index);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess();
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	public sealed class AsyncBatchExistsArrayExecutor
	(
		AsyncCluster cluster,
		ExistsArrayListener listener,
		Key[] keys,
		bool[] existsArray
	) : AsyncBatchExecutor(cluster, false)
	{
		private readonly ExistsArrayListener listener = listener;
		private readonly Key[] keys = keys;
		private readonly bool[] existsArray = existsArray;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(keys, existsArray);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(new AerospikeException.BatchExists(existsArray, ae));
		}
	}

	public sealed class AsyncBatchExistsSequenceExecutor
	(
		AsyncCluster cluster,
		ExistsSequenceListener listener
	) : AsyncBatchExecutor(cluster, false)
	{
		private readonly ExistsSequenceListener listener = listener;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess();
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	public sealed class AsyncBatchReadListExecutor
	(
		AsyncCluster cluster,
		BatchListListener listener,
		List<BatchRead> records
	) : AsyncBatchExecutor(cluster, true)
	{
		private readonly BatchListListener listener = listener;
		private readonly List<BatchRead> records = records;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(records);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}
	
	public sealed class AsyncBatchReadSequenceExecutor
	(
		AsyncCluster cluster,
		BatchSequenceListener listener
	) : AsyncBatchExecutor(cluster, true)
	{
		private readonly BatchSequenceListener listener = listener;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess();
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	public sealed class AsyncBatchGetArrayExecutor
	(
		AsyncCluster cluster,
		RecordArrayListener listener,
		Key[] keys,
		Record[] records
	) : AsyncBatchExecutor(cluster, false)
	{
		private readonly Key[] keys = keys;
		private readonly Record[] records = records;
		private readonly RecordArrayListener listener = listener;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(keys, records);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(new AerospikeException.BatchRecords(records, ae));
		}
	}

	public sealed class AsyncBatchGetSequenceExecutor
	(
		AsyncCluster cluster,
		RecordSequenceListener listener
	) : AsyncBatchExecutor(cluster, false)
	{
		private readonly RecordSequenceListener listener = listener;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess();
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}


	public sealed class AsyncBatchOperateListExecutor
	(
		AsyncCluster cluster,
		BatchOperateListListener listener,
		List<BatchRecord> records
	) : AsyncBatchExecutor(cluster, true)
	{
		internal readonly BatchOperateListListener listener = listener;
		internal readonly List<BatchRecord> records = records;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(records, GetStatus());
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	public sealed class AsyncBatchOperateSequenceExecutor
	(
		AsyncCluster cluster,
		BatchRecordSequenceListener listener
	) : AsyncBatchExecutor(cluster, true)
	{
		internal readonly BatchRecordSequenceListener listener = listener;

		protected internal override void OnSuccess()
		{
			listener.OnSuccess();
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	//-------------------------------------------------------
	// Batch Base Executor
	//-------------------------------------------------------

	public abstract class AsyncBatchExecutor : IBatchStatus
	{
		private AerospikeException exception;
		private int max;
		private int count;
		private readonly bool hasResultCode;
		private bool error;
		public AsyncCommand[] commands;
		public AsyncCluster cluster;

		public AsyncBatchExecutor(AsyncCluster cluster, bool hasResultCode)
		{
			this.hasResultCode = hasResultCode;
			this.cluster = cluster;
			cluster.AddCommandCount();
		}

		public void Execute(AsyncCommand[] commands)
		{
			this.commands = commands;
			max = commands.Length;

			foreach (AsyncCommand command in commands)
			{
				command.Execute();
			}
		}

		public void Retry(AsyncMultiCommand[] commands)
		{
			lock (this)
			{
				// Adjust max for new commands minus failed command.
				max += commands.Length - 1;
			}

			foreach (AsyncBatchCommand command in commands.Cast<AsyncBatchCommand>())
			{
				command.ExecuteBatchRetry();
			}
		}

		public void ChildSuccess(AsyncNode node)
		{
			bool complete;

			lock (this)
			{
				complete = ++count == max;
			}

			if (complete)
			{
				Finish();
			}
		}

		public void ChildFailure(AerospikeException ae)
		{
			bool complete;

			lock (this)
			{
				if (exception == null)
				{
					exception = ae;
				}
				complete = ++count == max;
			}

			if (complete)
			{
				Finish();
			}
		}

		private void Finish()
		{
			if (exception == null)
			{
				OnSuccess();
			}
			else
			{
				OnFailure(exception);
			}
		}

		public virtual void BatchKeyError(Cluster cluster, Key key, int index, AerospikeException ae, bool inDoubt, bool hasWrite)
		{
			// Only used in executors with sequence listeners.
			// These executors will override this method.
		}

		public void BatchKeyError(AerospikeException ae)
		{
			error = true;

			if (!hasResultCode)
			{
				// Legacy batch read commands that do not store a key specific resultCode.
				// Store exception which will be passed to the listener on batch completion.
				if (exception == null)
				{
					exception = ae;
				}
			}
		}

		public void SetRowError()
		{
			// Indicate that a key specific error occurred.
			error = true;
		}

		public bool GetStatus()
		{
			return !error;
		}

		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}
