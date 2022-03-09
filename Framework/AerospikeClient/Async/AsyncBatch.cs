/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public sealed class AsyncBatchReadListExecutor : AsyncBatchExecutor
	{
		private readonly BatchListListener listener;
		private readonly List<BatchRead> records;

		public AsyncBatchReadListExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchListListener listener,
			List<BatchRead> records
		) : base(true)
		{
			this.listener = listener;
			this.records = records;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records, this);
			AsyncBatchCommand[] commands = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new AsyncBatchReadListCommand(this, cluster, batchNode, policy, records);
			}
			// Dispatch commands to nodes.
			Execute(commands);
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
		private readonly List<BatchRead> records;

		public AsyncBatchReadListCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			List<BatchRead> records
		) : base(parent, cluster, batch, batchPolicy, true)
		{
			this.records = records;
		}

		public AsyncBatchReadListCommand(AsyncBatchReadListCommand other) : base(other)
		{
			this.records = other.records;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node.HasBatchAny)
			{
				SetBatchOperate(batchPolicy, records, batch);
			}
			else
			{
				SetBatchRead(batchPolicy, records, batch);
			}
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			BatchRead record = records[batchIndex];

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else
			{
				record.SetError(resultCode, false);
			}
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.SetError(resultCode, false);
				}
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchReadListCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchReadListCommand(parent, cluster, batchNode, batchPolicy, records);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, parent);
		}
	}

	//-------------------------------------------------------
	// ReadSequence
	//-------------------------------------------------------
	
	public sealed class AsyncBatchReadSequenceExecutor : AsyncBatchExecutor
	{
		private readonly BatchSequenceListener listener;

		public AsyncBatchReadSequenceExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchSequenceListener listener,
			List<BatchRead> records
		) : base(true)
		{
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records, this);
			AsyncBatchCommand[] commands = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new AsyncBatchReadSequenceCommand(this, cluster, batchNode, policy, listener, records);
			}
			// Dispatch commands to nodes.
			Execute(commands);
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
		private readonly BatchSequenceListener listener;
		private readonly List<BatchRead> records;

		public AsyncBatchReadSequenceCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			BatchSequenceListener listener,
			List<BatchRead> records
		) : base(parent, cluster, batch, batchPolicy, true)
		{
			this.listener = listener;
			this.records = records;
		}

		public AsyncBatchReadSequenceCommand(AsyncBatchReadSequenceCommand other) : base(other)
		{
			this.listener = other.listener;
			this.records = other.records;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node.HasBatchAny)
			{
				SetBatchOperate(batchPolicy, records, batch);
			}
			else
			{
				SetBatchRead(batchPolicy, records, batch);
			}
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			BatchRead record = records[batchIndex];

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else
			{
				record.SetError(resultCode, false);
			}
			listener.OnRecord(record);
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			foreach (int index in batch.offsets)
			{
				BatchRead record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.SetError(resultCode, false);
					listener.OnRecord(record);
				}
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchReadSequenceCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchReadSequenceCommand(parent, cluster, batchNode, batchPolicy, listener, records);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, parent);
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public sealed class AsyncBatchGetArrayExecutor : AsyncBatchExecutor
	{
		private readonly Key[] keys;
		private readonly Record[] records;
		private readonly RecordArrayListener listener;

		public AsyncBatchGetArrayExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			RecordArrayListener listener,
			Key[] keys,
			string[] binNames,
			Operation[] ops,
			int readAttr,
			bool isOperation
		) : base(false)
		{
			this.keys = keys;
			this.records = new Record[keys.Length];
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, this);
			AsyncBatchCommand[] commands = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new AsyncBatchGetArrayCommand(this, cluster, batchNode, policy, keys, binNames, ops, records, readAttr, isOperation);
			}
			// Dispatch commands to nodes.
			Execute(commands);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(keys, records);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(new AerospikeException.BatchRecords(records, ae));
		}
	}

	sealed class AsyncBatchGetArrayCommand : AsyncBatchCommand
	{
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Operation[] ops;
		private readonly Record[] records;
		private readonly int readAttr;

		public AsyncBatchGetArrayCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string[] binNames,
			Operation[] ops,
			Record[] records,
			int readAttr,
			bool isOperation
		) : base(parent, cluster, batch, batchPolicy, isOperation)
		{
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.records = records;
			this.readAttr = readAttr;
		}

		public AsyncBatchGetArrayCommand(AsyncBatchGetArrayCommand other) : base(other)
		{
			this.keys = other.keys;
			this.binNames = other.binNames;
			this.ops = other.ops;
			this.records = other.records;
			this.readAttr = other.readAttr;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node.HasBatchAny)
			{
				BatchAttr attr = new BatchAttr(batchPolicy, readAttr);
				SetBatchOperate(batchPolicy, keys, batch, binNames, ops, attr);
			}
			else
			{
				SetBatchRead(batchPolicy, keys, batch, binNames, ops, readAttr);
			}
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			if (resultCode == 0)
			{
				records[batchIndex] = ParseRecord();
			}
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			// records does not store error/inDoubt.
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchGetArrayCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchGetArrayCommand(parent, cluster, batchNode, batchPolicy, keys, binNames, ops, records, readAttr, isOperation);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, null, sequenceAP, sequenceSC, batch, false, parent);
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
			Operation[] ops,
			int readAttr,
			bool isOperation
		) : base(false)
		{
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, this);
			AsyncBatchCommand[] commands = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new AsyncBatchGetSequenceCommand(this, cluster, batchNode, policy, keys, binNames, ops, listener, readAttr, isOperation);
			}
			// Dispatch commands to nodes.
			Execute(commands);
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
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Operation[] ops;
		private readonly RecordSequenceListener listener;
		private readonly int readAttr;

		public AsyncBatchGetSequenceCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string[] binNames,
			Operation[] ops,
			RecordSequenceListener listener,
			int readAttr,
			bool isOperation
		) : base(parent, cluster, batch, batchPolicy, isOperation)
		{
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.listener = listener;
			this.readAttr = readAttr;
		}

		public AsyncBatchGetSequenceCommand(AsyncBatchGetSequenceCommand other) : base(other)
		{
			this.keys = other.keys;
			this.binNames = other.binNames;
			this.ops = other.ops;
			this.listener = other.listener;
			this.readAttr = other.readAttr;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node.HasBatchAny)
			{
				BatchAttr attr = new BatchAttr(batchPolicy, readAttr);
				SetBatchOperate(batchPolicy, keys, batch, binNames, ops, attr);
			}
			else
			{
				SetBatchRead(batchPolicy, keys, batch, binNames, ops, readAttr);
			}
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			Key keyOrig = keys[batchIndex];

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

		internal override void SetError(int resultCode, bool inDoubt)
		{
			// error/inDoubt not sent to listener.
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchGetSequenceCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchGetSequenceCommand(parent, cluster, batchNode, batchPolicy, keys, binNames, ops, listener, readAttr, isOperation);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, null, sequenceAP, sequenceSC, batch, false, parent);
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------
	
	public sealed class AsyncBatchExistsArrayExecutor : AsyncBatchExecutor
	{
		private readonly Key[] keys;
		private readonly bool[] existsArray;
		private readonly ExistsArrayListener listener;

		public AsyncBatchExistsArrayExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			Key[] keys,
			ExistsArrayListener listener
		) : base(false)
		{
			this.keys = keys;
			this.existsArray = new bool[keys.Length];
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, this);
			AsyncBatchCommand[] commands = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new AsyncBatchExistsArrayCommand(this, cluster, batchNode, policy, keys, existsArray);
			}
			// Dispatch commands to nodes.
			Execute(commands);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(keys, existsArray);
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(new AerospikeException.BatchExists(existsArray, ae));
		}
	}

	sealed class AsyncBatchExistsArrayCommand : AsyncBatchCommand
	{
		private readonly Key[] keys;
		private readonly bool[] existsArray;

		public AsyncBatchExistsArrayCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			bool[] existsArray
		) : base(parent, cluster, batch, batchPolicy, false)
		{
			this.keys = keys;
			this.existsArray = existsArray;
		}

		public AsyncBatchExistsArrayCommand(AsyncBatchExistsArrayCommand other) : base(other)
		{
			this.keys = other.keys;
			this.existsArray = other.existsArray;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node.HasBatchAny)
			{
				BatchAttr attr = new BatchAttr(batchPolicy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
				SetBatchOperate(batchPolicy, keys, batch, null, null, attr);
			}
			else
			{
				SetBatchRead(batchPolicy, keys, batch, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			}
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			existsArray[batchIndex] = resultCode == 0;
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			// existsArray does not store error/inDoubt.
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchExistsArrayCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchExistsArrayCommand(parent, cluster, batchNode, batchPolicy, keys, existsArray);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, null, sequenceAP, sequenceSC, batch, false, parent);
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
		) : base(false)
		{
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, this);
			AsyncBatchCommand[] commands = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new AsyncBatchExistsSequenceCommand(this, cluster, batchNode, policy, keys, listener);
			}
			// Dispatch commands to nodes.
			Execute(commands);
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
		private readonly Key[] keys;
		private readonly ExistsSequenceListener listener;

		public AsyncBatchExistsSequenceCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			ExistsSequenceListener listener
		) : base(parent, cluster, batch, batchPolicy, false)
		{
			this.keys = keys;
			this.listener = listener;
		}

		public AsyncBatchExistsSequenceCommand(AsyncBatchExistsSequenceCommand other) : base(other)
		{
			this.keys = other.keys;
			this.listener = other.listener;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node.HasBatchAny)
			{
				BatchAttr attr = new BatchAttr(batchPolicy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
				SetBatchOperate(batchPolicy, keys, batch, null, null, attr);
			}
			else
			{
				SetBatchRead(batchPolicy, keys, batch, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			}
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			Key keyOrig = keys[batchIndex];
			listener.OnExists(keyOrig, resultCode == 0);
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			// error/inDoubt not sent to listener.
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchExistsSequenceCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchExistsSequenceCommand(parent, cluster, batchNode, batchPolicy, keys, listener);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, null, sequenceAP, sequenceSC, batch, false, parent);
		}
	}

	//-------------------------------------------------------
	// OperateList
	//-------------------------------------------------------

	public sealed class AsyncBatchOperateListExecutor : AsyncBatchExecutor
	{
		internal readonly BatchOperateListListener listener;
		internal readonly List<BatchRecord> records;

		public AsyncBatchOperateListExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchOperateListListener listener,
			List<BatchRecord> records
		) : base(true)
		{
			this.listener = listener;
			this.records = records;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchOperateListCommand(this, cluster, batchNode, policy, records);
			}
			// Dispatch commands to nodes.
			Execute(tasks);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(records, GetStatus());
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(ae);
		}
	}

	sealed class AsyncBatchOperateListCommand : AsyncBatchCommand
	{
		internal readonly List<BatchRecord> records;

		public AsyncBatchOperateListCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			List<BatchRecord> records
		) : base(parent, cluster, batch, batchPolicy, true)
		{
			this.records = records;
		}

		public AsyncBatchOperateListCommand(AsyncBatchOperateListCommand other) : base(other)
		{
			this.records = other.records;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(batchPolicy, records, batch);
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = ParseRecord();
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = Command.BatchInDoubt(record.hasWrite, commandSentCounter);
					parent.SetRowError();
					return;
				}
			}

			record.SetError(resultCode, Command.BatchInDoubt(record.hasWrite, commandSentCounter));
			parent.SetRowError();
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.SetError(resultCode, record.hasWrite && inDoubt);
				}
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchOperateListCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchOperateListCommand(parent, cluster, batchNode, batchPolicy, records);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, parent);
		}
	}

	//-------------------------------------------------------
	// OperateSequence
	//-------------------------------------------------------

	public sealed class AsyncBatchOperateSequenceExecutor : AsyncBatchExecutor
	{
		internal readonly BatchRecordSequenceListener listener;

		public AsyncBatchOperateSequenceExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchRecordSequenceListener listener,
			List<BatchRecord> records
		) : base(true)
		{
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchOperateSequenceCommand(this, cluster, batchNode, policy, listener, records);
			}
			// Dispatch commands to nodes.
			Execute(tasks);
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

	sealed class AsyncBatchOperateSequenceCommand : AsyncBatchCommand
	{
		internal readonly BatchRecordSequenceListener listener;
		internal readonly List<BatchRecord> records;

		public AsyncBatchOperateSequenceCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			BatchRecordSequenceListener listener,
			List<BatchRecord> records
		) : base(parent, cluster, batch, batchPolicy, true)
		{
			this.listener = listener;
			this.records = records;
		}

		public AsyncBatchOperateSequenceCommand(AsyncBatchOperateSequenceCommand other) : base(other)
		{
			this.listener = other.listener;
			this.records = other.records;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(batchPolicy, records, batch);
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = ParseRecord();
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = Command.BatchInDoubt(record.hasWrite, commandSentCounter);
				}
				else
				{
					record.SetError(resultCode, Command.BatchInDoubt(record.hasWrite, commandSentCounter));
				}
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(record.hasWrite, commandSentCounter));
			}
			listener.OnRecord(record, batchIndex);
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.SetError(resultCode, record.hasWrite && inDoubt);
					listener.OnRecord(record, index);
				}
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchOperateSequenceCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchOperateSequenceCommand(parent, cluster, batchNode, batchPolicy, listener, records);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, parent);
		}
	}

	//-------------------------------------------------------
	// OperateRecordArray
	//-------------------------------------------------------

	public sealed class AsyncBatchOperateRecordArrayExecutor : AsyncBatchExecutor
	{
		internal readonly BatchRecordArrayListener listener;
		internal readonly BatchRecord[] records;

		public AsyncBatchOperateRecordArrayExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchRecordArrayListener listener,
			Key[] keys,
			Operation[] ops,
			BatchAttr attr
		) : base(true)
		{
			this.listener = listener;
			this.records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				this.records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, records, attr.hasWrite, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchOperateRecordArrayCommand(this, cluster, batchNode, policy, keys, ops, records, attr);
			}
			// Dispatch commands to nodes.
			Execute(tasks);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(records, GetStatus());
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(records, ae);
		}
	}

	sealed class AsyncBatchOperateRecordArrayCommand : AsyncBatchCommand
	{
		internal readonly Key[] keys;
		internal readonly Operation[] ops;
		internal readonly BatchRecord[] records;
		internal readonly BatchAttr attr;

		public AsyncBatchOperateRecordArrayCommand
		(
			AsyncBatchExecutor parent, 
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecord[] records,
			BatchAttr attr
		) : base(parent, cluster, batch, batchPolicy, ops != null)
		{
			this.keys = keys;
			this.ops = ops;
			this.records = records;
			this.attr = attr;
		}

		public AsyncBatchOperateRecordArrayCommand(AsyncBatchOperateRecordArrayCommand other) : base(other)
		{
			this.keys = other.keys;
			this.ops = other.ops;
			this.records = other.records;
			this.attr = other.attr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(batchPolicy, keys, batch, null, ops, attr);
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter));
				parent.SetRowError();
			}
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.SetError(resultCode, attr.hasWrite && inDoubt);
				}
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchOperateRecordArrayCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchOperateRecordArrayCommand(parent, cluster, batchNode, batchPolicy, keys, ops, records, attr);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, parent);
		}
	}

	//-------------------------------------------------------
	// OperateRecordSequence
	//-------------------------------------------------------

	public sealed class AsyncBatchOperateRecordSequenceExecutor : AsyncBatchExecutor
	{
		internal readonly BatchRecordSequenceListener listener;
		private readonly bool[] sent;

		public AsyncBatchOperateRecordSequenceExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchRecordSequenceListener listener,
			Key[] keys,
			Operation[] ops,
			BatchAttr attr
		) : base(true)
		{
			this.listener = listener;
			this.sent = new bool[keys.Length];

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, attr.hasWrite, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchOperateRecordSequenceCommand(this, cluster, batchNode, policy, keys, ops, sent, listener, attr);
			}
			// Dispatch commands to nodes.
			Execute(tasks);
		}

		public override void SetInvalidNode(Key key, int index, AerospikeException ae, bool inDoubt, bool hasWrite)
		{
			BatchRecord record = new BatchRecord(key, null, ae.Result, inDoubt, hasWrite);
			sent[index] = true;
			listener.OnRecord(record, index);
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

	sealed class AsyncBatchOperateRecordSequenceCommand : AsyncBatchCommand
	{
		internal readonly Key[] keys;
		internal readonly Operation[] ops;
		internal readonly bool[] sent;
		internal readonly BatchRecordSequenceListener listener;
		internal readonly BatchAttr attr;

		public AsyncBatchOperateRecordSequenceCommand
		(
			AsyncBatchExecutor parent, 
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			bool[] sent,
			BatchRecordSequenceListener listener,
			BatchAttr attr
		) : base(parent, cluster, batch, batchPolicy, ops != null)
		{
			this.keys = keys;
			this.ops = ops;
			this.sent = sent;
			this.listener = listener;
			this.attr = attr;
		}

		public AsyncBatchOperateRecordSequenceCommand(AsyncBatchOperateRecordSequenceCommand other) : base(other)
		{
			this.keys = other.keys;
			this.ops = other.ops;
			this.sent = other.sent;
			this.listener = other.listener;
			this.attr = other.attr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(batchPolicy, keys, batch, null, ops, attr);
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			Key keyOrig = keys[batchIndex];
			BatchRecord record;

			if (resultCode == 0)
			{
				record = new BatchRecord(keyOrig, ParseRecord(), attr.hasWrite);
			}
			else
			{
				record = new BatchRecord(keyOrig, null, resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
			}
			sent[batchIndex] = true;
			listener.OnRecord(record, batchIndex);
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			foreach (int index in batch.offsets)
			{
				if (!sent[index])
				{
					Key key = keys[index];
					BatchRecord record = new BatchRecord(key, null, resultCode, attr.hasWrite && inDoubt, attr.hasWrite);
					sent[index] = true;
					listener.OnRecord(record, index);
				}
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchOperateRecordSequenceCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchOperateRecordSequenceCommand(parent, cluster, batchNode, batchPolicy, keys, ops, sent, listener, attr);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, null, sequenceAP, sequenceSC, batch, attr.hasWrite, parent);
		}
	}

	//-------------------------------------------------------
	// UDFArray
	//-------------------------------------------------------

	public sealed class AsyncBatchUDFArrayExecutor : AsyncBatchExecutor
	{
		internal readonly BatchRecordArrayListener listener;
		internal readonly BatchRecord[] recordArray;

		public AsyncBatchUDFArrayExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchRecordArrayListener listener,
			Key[] keys,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchAttr attr
		) : base(true)
		{
			this.listener = listener;
			this.recordArray = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				this.recordArray[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, recordArray, attr.hasWrite, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchUDFArrayCommand(this, cluster, batchNode, policy, keys, packageName, functionName, argBytes, recordArray, attr);
			}
			// Dispatch commands to nodes.
			Execute(tasks);
		}

		protected internal override void OnSuccess()
		{
			listener.OnSuccess(recordArray, GetStatus());
		}

		protected internal override void OnFailure(AerospikeException ae)
		{
			listener.OnFailure(recordArray, ae);
		}
	}

	public sealed class AsyncBatchUDFArrayCommand : AsyncBatchCommand
	{
		internal readonly Key[] keys;
		internal readonly string packageName;
		internal readonly string functionName;
		internal readonly byte[] argBytes;
		internal readonly BatchRecord[] records;
		internal readonly BatchAttr attr;

		public AsyncBatchUDFArrayCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchRecord[] records,
			BatchAttr attr
		) : base(parent, cluster, batch, batchPolicy, false)
		{
			this.keys = keys;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
			this.records = records;
			this.attr = attr;
		}

		public AsyncBatchUDFArrayCommand(AsyncBatchUDFArrayCommand other) : base(other)
		{
			this.keys = other.keys;
			this.packageName = other.packageName;
			this.functionName = other.functionName;
			this.argBytes = other.argBytes;
			this.records = other.records;
			this.attr = other.attr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchUDF(batchPolicy, keys, batch, packageName, functionName, argBytes, attr);
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = ParseRecord();
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = Command.BatchInDoubt(attr.hasWrite, commandSentCounter);
					parent.SetRowError();
					return;
				}
			}

			record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter));
			parent.SetRowError();
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.SetError(resultCode, attr.hasWrite && inDoubt);
				}
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchUDFArrayCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchUDFArrayCommand(parent, cluster, batchNode, batchPolicy, keys, packageName, functionName, argBytes, records, attr);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, parent);
		}
	}

	//-------------------------------------------------------
	// UDFSequence
	//-------------------------------------------------------

	public sealed class AsyncBatchUDFSequenceExecutor : AsyncBatchExecutor
	{
		internal readonly BatchRecordSequenceListener listener;
		private readonly bool[] sent;

		public AsyncBatchUDFSequenceExecutor
		(
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchRecordSequenceListener listener,
			Key[] keys,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchAttr attr
		) : base(true)
		{
			this.listener = listener;
			this.sent = new bool[keys.Length];

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, attr.hasWrite, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				tasks[count++] = new AsyncBatchUDFSequenceCommand(this, cluster, batchNode, policy, keys, packageName, functionName, argBytes, sent, listener, attr);
			}
			// Dispatch commands to nodes.
			Execute(tasks);
		}

		public override void SetInvalidNode(Key key, int index, AerospikeException ae, bool inDoubt, bool hasWrite)
		{
			BatchRecord record = new BatchRecord(key, null, ae.Result, inDoubt, hasWrite);
			sent[index] = true;
			listener.OnRecord(record, index);
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

	sealed class AsyncBatchUDFSequenceCommand : AsyncBatchCommand
	{
		internal readonly Key[] keys;
		internal readonly string packageName;
		internal readonly string functionName;
		internal readonly byte[] argBytes;
		internal readonly bool[] sent;
		internal readonly BatchRecordSequenceListener listener;
		internal readonly BatchAttr attr;

		public AsyncBatchUDFSequenceCommand
		(
			AsyncBatchExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string packageName,
			string functionName,
			byte[] argBytes,
			bool[] sent,
			BatchRecordSequenceListener listener,
			BatchAttr attr
		) : base(parent, cluster, batch, batchPolicy, false)
		{
			this.keys = keys;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
			this.sent = sent;
			this.listener = listener;
			this.attr = attr;
		}

		public AsyncBatchUDFSequenceCommand(AsyncBatchUDFSequenceCommand other) : base(other)
		{
			this.keys = other.keys;
			this.packageName = other.packageName;
			this.functionName = other.functionName;
			this.argBytes = other.argBytes;
			this.sent = other.sent;
			this.listener = other.listener;
			this.attr = other.attr;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchUDF(batchPolicy, keys, batch, packageName, functionName, argBytes, attr);
		}

		protected internal override void ParseRow()
		{
			SkipKey(fieldCount);

			Key keyOrig = keys[batchIndex];
			BatchRecord record;

			if (resultCode == 0)
			{
				record = new BatchRecord(keyOrig, ParseRecord(), attr.hasWrite);
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = ParseRecord();
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record = new BatchRecord(keyOrig, r, resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
				}
				else
				{
					record = new BatchRecord(keyOrig, null, resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
				}
			}
			else
			{
				record = new BatchRecord(keyOrig, null, resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
			}
			sent[batchIndex] = true;
			listener.OnRecord(record, batchIndex);
		}

		internal override void SetError(int resultCode, bool inDoubt)
		{
			foreach (int index in batch.offsets)
			{
				if (!sent[index])
				{
					Key key = keys[index];
					BatchRecord record = new BatchRecord(key, null, resultCode, attr.hasWrite && inDoubt, attr.hasWrite);
					sent[index] = true;
					listener.OnRecord(record, index);
				}
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchUDFSequenceCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchUDFSequenceCommand(parent, cluster, batchNode, batchPolicy, keys, packageName, functionName, argBytes, sent, listener, attr);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, null, sequenceAP, sequenceSC, batch, attr.hasWrite, parent);
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

		public AsyncBatchExecutor(bool hasResultCode)
		{
			this.hasResultCode = hasResultCode;
		}

		public void Execute(AsyncBatchCommand[] commands)
		{
			max = commands.Length;

			foreach (AsyncBatchCommand command in commands)
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

			foreach (AsyncBatchCommand command in commands)
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

		public virtual void SetInvalidNode(Key key, int index, AerospikeException ae, bool inDoubt, bool hasWrite)
		{
			// Only used in executors with sequence listeners.
			// These executors will override this method.
		}

		public void SetInvalidNode(AerospikeException ae)
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

	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	public abstract class AsyncBatchCommand : AsyncMultiCommand
	{
		internal readonly AsyncBatchExecutor parent;
		internal readonly BatchNode batch;
		internal readonly BatchPolicy batchPolicy;
		internal uint sequenceAP;
		internal uint sequenceSC;

		public AsyncBatchCommand(AsyncBatchExecutor parent, AsyncCluster cluster, BatchNode batch, BatchPolicy batchPolicy, bool isOperation)
			: base(cluster, batchPolicy, (AsyncNode)batch.node, isOperation)
		{
			this.parent = parent;
			this.batch = batch;
			this.batchPolicy = batchPolicy;
		}

		public AsyncBatchCommand(AsyncBatchCommand other) : base(other)
		{
			this.parent = other.parent;
			this.batch = other.batch;
			this.batchPolicy = other.batchPolicy;
			this.sequenceAP = other.sequenceAP;
			this.sequenceSC = other.sequenceSC;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			if (!(policy.replica == Replica.SEQUENCE || policy.replica == Replica.PREFER_RACK))
			{
				// Perform regular retry to same node.
				return true;
			}

			sequenceAP++;

			if (! timeout || policy.readModeSC != ReadModeSC.LINEARIZE) {
				sequenceSC++;
			}
			return false;
		}

		protected internal override bool RetryBatch()
		{
			List<BatchNode> batchNodes = null;

			try
			{
				// Retry requires keys for this node to be split among other nodes.
				// This can cause an exponential number of commands.
				batchNodes = GenerateBatchNodes();
				
				if (batchNodes.Count == 1 && batchNodes[0].node == batch.node)
				{
					// Batch node is the same.  Go through normal retry.
					// Normal retries reuse eventArgs, so PutBackArgsOnError()
					// should not be called here.
					return false;
				}
			}
			catch (Exception)
			{
				// Close original command.
				base.PutBackArgsOnError();
				throw;
			}

			// Close original command.
			base.PutBackArgsOnError();
			
			// Execute new commands.
			AsyncBatchCommand[] cmds = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				AsyncBatchCommand cmd = CreateCommand(batchNode);
				cmd.sequenceAP = sequenceAP;
				cmd.sequenceSC = sequenceSC;
				cmd.SetBatchRetry(this);
				cmds[count++] = cmd;
			}

			// Retry new commands.
			parent.Retry(cmds);

			// Return true so original batch command is stopped.
			return true;
		}

		protected internal override void OnSuccess()
		{
			parent.ChildSuccess(node);
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			SetError(e.Result, e.InDoubt);
			parent.ChildFailure(e);
		}

		internal abstract void SetError(int resultCode, bool inDoubt);
		internal abstract AsyncBatchCommand CreateCommand(BatchNode batchNode);
		internal abstract List<BatchNode> GenerateBatchNodes();
	}
}
