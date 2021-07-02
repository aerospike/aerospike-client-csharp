/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
		)
		{
			this.listener = listener;
			this.records = records;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records);
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
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.records = records;
		}

		public AsyncBatchReadListCommand(AsyncBatchReadListCommand other) : base(other)
		{
			this.records = other.records;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(batchPolicy, records, batch);
		}

		protected internal override void ParseRow(Key key)
		{
			BatchRead record = records[batchIndex];

			if (resultCode == 0)
			{
				record.record = ParseRecord();
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
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch);
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
		)
		{
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records);
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
		) : base(parent, cluster, batch, batchPolicy)
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
			SetBatchRead(batchPolicy, records, batch);
		}

		protected internal override void ParseRow(Key key)
		{
			BatchRead record = records[batchIndex];

			if (resultCode == 0)
			{
				record.record = ParseRecord();
			}
			listener.OnRecord(record);
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
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch);
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
			int readAttr
		)
		{
			this.keys = keys;
			this.records = new Record[keys.Length];
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys);
			AsyncBatchCommand[] commands = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new AsyncBatchGetArrayCommand(this, cluster, batchNode, policy, keys, binNames, records, readAttr);
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
			listener.OnFailure(ae);
		}
	}

	sealed class AsyncBatchGetArrayCommand : AsyncBatchCommand
	{
		private readonly Key[] keys;
		private readonly string[] binNames;
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
			Record[] records,
			int readAttr
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		public AsyncBatchGetArrayCommand(AsyncBatchGetArrayCommand other) : base(other)
		{
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
			if (resultCode == 0)
			{
				records[batchIndex] = ParseRecord();
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchGetArrayCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchGetArrayCommand(parent, cluster, batchNode, batchPolicy, keys, binNames, records, readAttr);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch);
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
		)
		{
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys);
			AsyncBatchCommand[] commands = new AsyncBatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new AsyncBatchGetSequenceCommand(this, cluster, batchNode, policy, keys, binNames, listener, readAttr);
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
			RecordSequenceListener listener,
			int readAttr
		) : base(parent, cluster, batch, batchPolicy)
		{
			this.keys = keys;
			this.binNames = binNames;
			this.listener = listener;
			this.readAttr = readAttr;
		}

		public AsyncBatchGetSequenceCommand(AsyncBatchGetSequenceCommand other) : base(other)
		{
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

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchGetSequenceCommand(this);
		}

		internal override AsyncBatchCommand CreateCommand(BatchNode batchNode)
		{
			return new AsyncBatchGetSequenceCommand(parent, cluster, batchNode, batchPolicy, keys, binNames, listener, readAttr);
		}

		internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch);
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
		)
		{
			this.keys = keys;
			this.existsArray = new bool[keys.Length];
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys);
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
			listener.OnFailure(ae);
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
		) : base(parent, cluster, batch, batchPolicy)
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
			SetBatchRead(batchPolicy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			existsArray[batchIndex] = resultCode == 0;
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
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch);
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
		)
		{
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys);
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
		) : base(parent, cluster, batch, batchPolicy)
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
			SetBatchRead(batchPolicy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			Key keyOrig = keys[batchIndex];
			listener.OnExists(keyOrig, resultCode == 0);
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
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch);
		}
	}

	//-------------------------------------------------------
	// Batch Base Executor
	//-------------------------------------------------------

	public abstract class AsyncBatchExecutor : AsyncExecutor
	{
		private AerospikeException exception;
		private int max;
		private int count;

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

		public AsyncBatchCommand(AsyncBatchExecutor parent, AsyncCluster cluster, BatchNode batch, BatchPolicy batchPolicy)
			: base(parent, cluster, batchPolicy, (AsyncNode)batch.node)
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
			AsyncMultiCommand[] cmds = new AsyncMultiCommand[batchNodes.Count];
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

		internal abstract AsyncBatchCommand CreateCommand(BatchNode batchNode);
		internal abstract List<BatchNode> GenerateBatchNodes();
	}
}
