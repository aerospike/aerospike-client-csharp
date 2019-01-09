/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Client
{
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public sealed class AsyncBatchReadListExecutor : AsyncMultiExecutor
	{
		private readonly BatchListListener listener;
		private readonly List<BatchRead> records;

		public AsyncBatchReadListExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchListListener listener,
			List<BatchRead> records
		)
		{
			this.listener = listener;
			this.records = records;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records);
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchReadListCommand(this, cluster, batchNode, policy, records);
			}
			// Dispatch commands to nodes.
			Execute(tasks, 0);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(records);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	sealed class AsyncBatchReadListCommand : AsyncBatchCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy batchPolicy;
		private readonly List<BatchRead> records;

		public AsyncBatchReadListCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			List<BatchRead> records
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.records = records;
		}

		public AsyncBatchReadListCommand(AsyncBatchReadListCommand other) : base(other)
		{
			this.batch = other.batch;
			this.batchPolicy = other.batchPolicy;
			this.records = other.records;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, records, batch);
		}

		protected internal override void ParseRow(Key key)
		{
			BatchRead record = records[batchIndex];

			if (Util.ByteArrayEquals(key.digest, record.key.digest))
			{
				if (resultCode == 0)
				{
					record.record = ParseRecord();
				}
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchReadListCommand(this);
		}

		internal override AsyncMultiCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchReadListCommand(parent, cluster, batchNode, batchPolicy, records);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequence, batch);
		}
	}

	//-------------------------------------------------------
	// ReadSequence
	//-------------------------------------------------------
	
	public sealed class AsyncBatchReadSequenceExecutor : AsyncMultiExecutor
	{
		private readonly BatchSequenceListener listener;

		public AsyncBatchReadSequenceExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchSequenceListener listener,
			List<BatchRead> records
		)
		{
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records);
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchReadSequenceCommand(this, cluster, batchNode, policy, listener, records);
			}
			// Dispatch commands to nodes.
			Execute(tasks, 0);
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

	sealed class AsyncBatchReadSequenceCommand : AsyncBatchCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy batchPolicy;
		private readonly BatchSequenceListener listener;
		private readonly List<BatchRead> records;

		public AsyncBatchReadSequenceCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			BatchSequenceListener listener,
			List<BatchRead> records
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.listener = listener;
			this.records = records;
		}

		public AsyncBatchReadSequenceCommand(AsyncBatchReadSequenceCommand other) : base(other)
		{
			this.batch = other.batch;
			this.batchPolicy = other.batchPolicy;
			this.listener = other.listener;
			this.records = other.records;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, records, batch);
		}

		protected internal override void ParseRow(Key key)
		{
			BatchRead record = records[batchIndex];

			if (Util.ByteArrayEquals(key.digest, record.key.digest))
			{
				if (resultCode == 0)
				{
					record.record = ParseRecord();
				}
				listener.OnRecord(record);
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchReadSequenceCommand(this);
		}

		internal override AsyncMultiCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchReadSequenceCommand(parent, cluster, batchNode, batchPolicy, listener, records);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequence, batch);
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public sealed class AsyncBatchGetArrayExecutor : AsyncBatchExecutor
	{
		private readonly RecordArrayListener listener;
		private readonly Record[] recordArray;

		public AsyncBatchGetArrayExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			RecordArrayListener listener,
			Key[] keys,
			string[] binNames,
			int readAttr
		) : base(cluster, policy, keys)
		{
			this.recordArray = new Record[keys.Length];
			this.listener = listener;

			// Create commands.
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[base.taskSize];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchGetArrayCommand(this, cluster, batchNode, policy, keys, binNames, recordArray, readAttr);
			}
			// Dispatch commands to nodes.
			Execute(tasks, 0);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(keys, recordArray);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	sealed class AsyncBatchGetArrayCommand : AsyncBatchCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy batchPolicy;
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Record[] records;
		private readonly int readAttr;

		public AsyncBatchGetArrayCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string[] binNames,
			Record[] records,
			int readAttr
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		public AsyncBatchGetArrayCommand(AsyncBatchGetArrayCommand other) : base(other)
		{
			this.batch = other.batch;
			this.batchPolicy = other.batchPolicy;
			this.keys = other.keys;
			this.binNames = other.binNames;
			this.records = other.records;
			this.readAttr = other.readAttr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, keys, batch, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			if (Util.ByteArrayEquals(key.digest, keys[batchIndex].digest))
			{
				if (resultCode == 0)
				{
					records[batchIndex] = ParseRecord();
				}
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchGetArrayCommand(this);
		}

		internal override AsyncMultiCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchGetArrayCommand(parent, cluster, batchNode, batchPolicy, keys, binNames, records, readAttr);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequence, batch);
		}
	}

	//-------------------------------------------------------
	// GetSequence
	//-------------------------------------------------------

	public sealed class AsyncBatchGetSequenceExecutor : AsyncBatchExecutor
	{
		private readonly RecordSequenceListener listener;

		public AsyncBatchGetSequenceExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			RecordSequenceListener listener,
			Key[] keys,
			string[] binNames,
			int readAttr
		) : base(cluster, policy, keys)
		{
			this.listener = listener;

			// Create commands.
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[base.taskSize];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchGetSequenceCommand(this, cluster, batchNode, policy, keys, binNames, listener, readAttr);
			}
			// Dispatch commands to nodes.
			Execute(tasks, 0);
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

	sealed class AsyncBatchGetSequenceCommand : AsyncBatchCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy batchPolicy;
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly RecordSequenceListener listener;
		private readonly int readAttr;

		public AsyncBatchGetSequenceCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string[] binNames,
			RecordSequenceListener listener,
			int readAttr
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.keys = keys;
			this.binNames = binNames;
			this.listener = listener;
			this.readAttr = readAttr;
		}

		public AsyncBatchGetSequenceCommand(AsyncBatchGetSequenceCommand other) : base(other)
		{
			this.batch = other.batch;
			this.batchPolicy = other.batchPolicy;
			this.keys = other.keys;
			this.binNames = other.binNames;
			this.listener = other.listener;
			this.readAttr = other.readAttr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, keys, batch, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			Key keyOrig = keys[batchIndex];

			if (Util.ByteArrayEquals(key.digest, keyOrig.digest))
			{
				if (resultCode == 0)
				{
					Record record = ParseRecord();
					listener.OnRecord(keyOrig, record);
				}
				else
				{
					listener.OnRecord(keyOrig, null);
				}
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchGetSequenceCommand(this);
		}

		internal override AsyncMultiCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchGetSequenceCommand(parent, cluster, batchNode, batchPolicy, keys, binNames, listener, readAttr);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequence, batch);
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------
	
	public sealed class AsyncBatchExistsArrayExecutor : AsyncBatchExecutor
	{
		private readonly ExistsArrayListener listener;
		private readonly bool[] existsArray;

		public AsyncBatchExistsArrayExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			Key[] keys,
			ExistsArrayListener listener
		) : base(cluster, policy, keys)
		{
			this.existsArray = new bool[keys.Length];
			this.listener = listener;

			// Create commands.
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[base.taskSize];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchExistsArrayCommand(this, cluster, batchNode, policy, keys, existsArray);
			}
			// Dispatch commands to nodes.
			Execute(tasks, 0);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(keys, existsArray);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	sealed class AsyncBatchExistsArrayCommand : AsyncBatchCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy batchPolicy;
		private readonly Key[] keys;
		private readonly bool[] existsArray;

		public AsyncBatchExistsArrayCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			bool[] existsArray
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.keys = keys;
			this.existsArray = existsArray;
		}

		public AsyncBatchExistsArrayCommand(AsyncBatchExistsArrayCommand other) : base(other)
		{
			this.batch = other.batch;
			this.batchPolicy = other.batchPolicy;
			this.keys = other.keys;
			this.existsArray = other.existsArray;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			if (Util.ByteArrayEquals(key.digest, keys[batchIndex].digest))
			{
				existsArray[batchIndex] = resultCode == 0;
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchExistsArrayCommand(this);
		}

		internal override AsyncMultiCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchExistsArrayCommand(parent, cluster, batchNode, batchPolicy, keys, existsArray);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequence, batch);
		}
	}

	//-------------------------------------------------------
	// ExistsSequence
	//-------------------------------------------------------

	public sealed class AsyncBatchExistsSequenceExecutor : AsyncBatchExecutor
	{
		private readonly ExistsSequenceListener listener;

		public AsyncBatchExistsSequenceExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			Key[] keys,
			ExistsSequenceListener listener
		) : base(cluster, policy, keys)
		{
			this.listener = listener;

			// Create commands.
			AsyncMultiCommand[] tasks = new AsyncMultiCommand[base.taskSize];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchExistsSequenceCommand(this, cluster, batchNode, policy, keys, listener);
			}
			// Dispatch commands to nodes.
			Execute(tasks, 0);
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

	sealed class AsyncBatchExistsSequenceCommand : AsyncBatchCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy batchPolicy;
		private readonly Key[] keys;
		private readonly ExistsSequenceListener listener;

		public AsyncBatchExistsSequenceCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			ExistsSequenceListener listener
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.keys = keys;
			this.listener = listener;
		}

		public AsyncBatchExistsSequenceCommand(AsyncBatchExistsSequenceCommand other) : base(other)
		{
			this.batch = other.batch;
			this.batchPolicy = other.batchPolicy;
			this.keys = other.keys;
			this.listener = other.listener;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			Key keyOrig = keys[batchIndex];

			if (Util.ByteArrayEquals(key.digest, keyOrig.digest))
			{
				listener.OnExists(keyOrig, resultCode == 0);
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + batchIndex);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchExistsSequenceCommand(this);
		}

		internal override AsyncMultiCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchExistsSequenceCommand(parent, cluster, batchNode, batchPolicy, keys, listener);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequence, batch);
		}
	}

	//-------------------------------------------------------
	// Batch Base Executor
	//-------------------------------------------------------
	
	public abstract class AsyncBatchExecutor : AsyncMultiExecutor
	{
		protected internal readonly Key[] keys;
		protected internal readonly List<BatchNode> batchNodes;
		protected internal readonly int taskSize;

		public AsyncBatchExecutor(Cluster cluster, BatchPolicy policy, Key[] keys)
		{
			this.keys = keys;
			this.batchNodes = BatchNode.GenerateList(cluster, policy, keys);
			this.taskSize = batchNodes.Count;
		}
	}

	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	public abstract class AsyncBatchCommand : AsyncMultiCommand
	{
		readonly BatchNode batch;
		readonly BatchPolicy batchPolicy;

		public AsyncBatchCommand(AsyncMultiExecutor parent, AsyncCluster cluster, BatchNode batch, BatchPolicy batchPolicy)
			: base(parent, cluster, batchPolicy, (AsyncNode)batch.node, false)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
		}

		public AsyncBatchCommand(AsyncBatchCommand other) : base(other)
		{
		}

		protected internal override void Retry(AerospikeException ae)
		{
			if (!(policy.replica == Replica.SEQUENCE || policy.replica == Replica.PREFER_RACK) || parent.IsDone())
			{
				base.Retry(ae);
				return;
			}

			// Retry requires keys for this node to be split among other nodes.
			// This can cause an exponential number of commands.
			List<BatchNode> batchNodes = GenerateBatchNodes();

			if (batchNodes.Count == 1 && batchNodes[0].node == batch.node)
			{
				// Batch node is the same.  Go through normal retry.
				base.Retry(ae);
				return;
			}

			// Close original command.
			base.PutBackArgsOnError();

			// Execute new commands.
			AsyncMultiCommand[] cmds = new AsyncMultiCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				AsyncMultiCommand cmd = CreateCommand(batchNode);
				cmd.SetBatchRetry(this);
				cmds[count++] = cmd;
			}
			parent.ExecuteBatchRetry(cmds, this);
		}

		internal abstract AsyncMultiCommand CreateCommand(BatchNode batchNode);
		internal abstract List<BatchNode> GenerateBatchNodes();
	}
}
