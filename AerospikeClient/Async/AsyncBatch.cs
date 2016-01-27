/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
				if (!batchNode.node.hasBatchIndex)
				{
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
				}
				tasks[count++] = new AsyncBatchReadListCommand(this, cluster, batchNode, policy, records);
			}
			// Dispatch commands to nodes.
			Execute(tasks, policy.maxConcurrentThreads);
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

	sealed class AsyncBatchReadListCommand : AsyncMultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly List<BatchRead> records;

		public AsyncBatchReadListCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			List<BatchRead> records
		) : base(parent, cluster, (AsyncNode)batch.node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.records = records;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, records, batch);
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
				if (!batchNode.node.hasBatchIndex)
				{
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
				}
				tasks[count++] = new AsyncBatchReadSequenceCommand(this, cluster, batchNode, policy, listener, records);
			}
			// Dispatch commands to nodes.
			Execute(tasks, policy.maxConcurrentThreads);
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

	sealed class AsyncBatchReadSequenceCommand : AsyncMultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly BatchSequenceListener listener;
		private readonly List<BatchRead> records;

		public AsyncBatchReadSequenceCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			BatchSequenceListener listener,
			List<BatchRead> records
		) : base(parent, cluster, (AsyncNode)batch.node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.listener = listener;
			this.records = records;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, records, batch);
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
				if (batchNode.node.UseNewBatch(policy))
				{
					// New batch
					tasks[count++] = new AsyncBatchGetArrayCommand(this, cluster, batchNode, policy, keys, binNames, recordArray, readAttr);
				}
				else
				{
					// Old batch only allows one namespace per call.
					foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
					{
						tasks[count++] = new AsyncBatchGetArrayDirect(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, binNames, recordArray, readAttr);
					}
				}
			}
			// Dispatch commands to nodes.
			Execute(tasks, policy.maxConcurrentThreads);
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

	sealed class AsyncBatchGetArrayCommand : AsyncMultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Record[] records;
		private readonly int readAttr;

		public AsyncBatchGetArrayCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			string[] binNames,
			Record[] records,
			int readAttr
		) : base(parent, cluster, (AsyncNode)batch.node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, keys, batch, binNames, readAttr);
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
	}

	sealed class AsyncBatchGetArrayDirect : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batch;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Record[] records;
		private readonly int readAttr;
		private int index;

		public AsyncBatchGetArrayDirect
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			string[] binNames,
			Record[] records,
			int readAttr
		) : base(parent, cluster, node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchReadDirect(policy, keys, batch, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			int offset = batch.offsets[index++];

			if (Util.ByteArrayEquals(key.digest, keys[offset].digest))
			{
				if (resultCode == 0)
				{
					records[offset] = ParseRecord();
				}
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
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
				if (batchNode.node.UseNewBatch(policy))
				{
					// New batch
					tasks[count++] = new AsyncBatchGetSequenceCommand(this, cluster, batchNode, policy, keys, binNames, listener, readAttr);
				}
				else
				{
					// Old batch only allows one namespace per call.
					foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
					{
						tasks[count++] = new AsyncBatchGetSequenceDirect(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, binNames, listener, readAttr);
					}
				}
			}
			// Dispatch commands to nodes.
			Execute(tasks, policy.maxConcurrentThreads);
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
	
	sealed class AsyncBatchGetSequenceCommand : AsyncMultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly RecordSequenceListener listener;
		private readonly int readAttr;

		public AsyncBatchGetSequenceCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			string[] binNames,
			RecordSequenceListener listener,
			int readAttr
		) : base(parent, cluster, (AsyncNode)batch.node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.listener = listener;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, keys, batch, binNames, readAttr);
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
	}
	
	sealed class AsyncBatchGetSequenceDirect : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batch;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly RecordSequenceListener listener;
		private readonly int readAttr;
		private int index;

		public AsyncBatchGetSequenceDirect
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			string[] binNames,
			RecordSequenceListener listener,
			int readAttr
		) : base(parent, cluster, node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.binNames = binNames;
			this.listener = listener;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchReadDirect(policy, keys, batch, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			int offset = batch.offsets[index++];
			Key keyOrig = keys[offset];

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
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
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
				if (batchNode.node.UseNewBatch(policy))
				{
					// New batch
					tasks[count++] = new AsyncBatchExistsArrayCommand(this, cluster, batchNode, policy, keys, existsArray);
				}
				else
				{
					// Old batch only allows one namespace per call.
					foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
					{
						tasks[count++] = new AsyncBatchExistsArrayDirect(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, existsArray);
					}
				}
			}
			// Dispatch commands to nodes.
			Execute(tasks, policy.maxConcurrentThreads);
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

	sealed class AsyncBatchExistsArrayCommand : AsyncMultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly Key[] keys;
		private readonly bool[] existsArray;

		public AsyncBatchExistsArrayCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			bool[] existsArray
		) : base(parent, cluster, (AsyncNode)batch.node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
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
	}

	sealed class AsyncBatchExistsArrayDirect : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batch;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly bool[] existsArray;
		private int index;

		public AsyncBatchExistsArrayDirect
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			bool[] existsArray
		) : base(parent, cluster, node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.existsArray = existsArray;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchReadDirect(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			int offset = batch.offsets[index++];

			if (Util.ByteArrayEquals(key.digest, keys[offset].digest))
			{
				existsArray[offset] = resultCode == 0;
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
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
				if (batchNode.node.UseNewBatch(policy))
				{
					// New batch
					tasks[count++] = new AsyncBatchExistsSequenceCommand(this, cluster, batchNode, policy, keys, listener);
				}
				else
				{
					// Old batch only allows one namespace per call.
					foreach (BatchNode.BatchNamespace batchNamespace in batchNode.batchNamespaces)
					{
						tasks[count++] = new AsyncBatchExistsSequenceDirect(this, cluster, (AsyncNode)batchNode.node, batchNamespace, policy, keys, listener);
					}
				}
			}
			// Dispatch commands to nodes.
			Execute(tasks, policy.maxConcurrentThreads);
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

	sealed class AsyncBatchExistsSequenceCommand : AsyncMultiCommand
	{
		private readonly BatchNode batch;
		private readonly BatchPolicy policy;
		private readonly Key[] keys;
		private readonly ExistsSequenceListener listener;

		public AsyncBatchExistsSequenceCommand
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			ExistsSequenceListener listener
		) : base(parent, cluster, (AsyncNode)batch.node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.listener = listener;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchRead(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
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
	}

	sealed class AsyncBatchExistsSequenceDirect : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batch;
		private readonly Policy policy;
		private readonly Key[] keys;
		private readonly ExistsSequenceListener listener;
		private int index;

		public AsyncBatchExistsSequenceDirect
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batch,
			Policy policy,
			Key[] keys,
			ExistsSequenceListener listener
		) : base(parent, cluster, node, false)
		{
			this.batch = batch;
			this.policy = policy;
			this.keys = keys;
			this.listener = listener;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchReadDirect(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			int offset = batch.offsets[index++];
			Key keyOrig = keys[offset];

			if (Util.ByteArrayEquals(key.digest, keyOrig.digest))
			{
				listener.OnExists(keyOrig, resultCode == 0);
			}
			else
			{
				throw new AerospikeException.Parse("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest) + ',' + index + ',' + offset);
			}
		}
	}

	//-------------------------------------------------------
	// BaseExecutor
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

			// Count number of asynchronous commands needed.
			int size = 0;
			foreach (BatchNode batchNode in batchNodes)
			{
				if (batchNode.node.UseNewBatch(policy))
				{
					// New batch
					size++;
				}
				else
				{
					// Old batch only allows one namespace per call.
					batchNode.SplitByNamespace(keys);
					size += batchNode.batchNamespaces.Count;
				}
			}
			this.taskSize = size;
		}
	}
}
