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
using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public sealed class BatchReadListCommandNew : BatchCommandNew, ICommand
	{
		private readonly List<BatchRead> records;

		public BatchReadListCommandNew
		(
			ArrayPool<byte> bufferPool,
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			List<BatchRead> records,
			BatchStatus status
		) : base(bufferPool, cluster, batch, policy, status, true)
		{
			this.records = records;
		}

		public new void WriteBuffer()
		{
			if (batch.node != null && batch.node.HasBatchAny)
			{
				this.SetBatchOperate(batchPolicy, records, batch);
			}
			else
			{
				this.SetBatchRead(batchPolicy, records, batch);
			}
		}

		public new BatchRecord ParseRow()
		{
			this.SkipKey(FieldCount);

			BatchRead record = records[BatchIndex];

			if (ResultCode == 0)
			{
				record.SetRecord(this.ParseRecord());
			}
			else
			{
				record.SetError(ResultCode, false);
				status.SetRowError();
			}
			return record;
		}

		public new BatchCommandNew CreateCommand(BatchNode batchNode)
		{
			return new BatchReadListCommandNew(BufferPool, Cluster, batchNode, batchPolicy, records, status);
		}

		public new List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(Cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, status);
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public sealed class BatchGetArrayCommandNew : BatchCommandNew, ICommand
	{
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Operation[] ops;
		private readonly Record[] records;
		private readonly int readAttr;

		public BatchGetArrayCommandNew
		(
			ArrayPool<byte> bufferPool,
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			string[] binNames,
			Operation[] ops,
			Record[] records,
			int readAttr,
			bool isOperation,
			BatchStatus status
		) : base(bufferPool, cluster, batch, policy, status, isOperation)
		{
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.records = records;
			this.readAttr = readAttr;
		}

		public new void WriteBuffer()
		{
			if (batch.node != null && batch.node.HasBatchAny)
			{
				BatchAttr attr = new(Policy, readAttr, ops);
				this.SetBatchOperate(batchPolicy, keys, batch, binNames, ops, attr);
			}
			else
			{
				this.SetBatchRead(batchPolicy, keys, batch, binNames, ops, readAttr);
			}
		}

		public new bool ParseRow()
		{
			this.SkipKey(FieldCount);

			if (ResultCode == 0)
			{
				records[BatchIndex] = this.ParseRecord();
			}
			return true;
		}

		public new BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchGetArrayCommand(Cluster, batchNode, batchPolicy, keys, binNames, ops, records, readAttr, IsOperation, status);
		}

		public new List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(Cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, status);
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public sealed class BatchExistsArrayCommandNew : BatchCommandNew, ICommand
	{
		private readonly Key[] keys;
		private readonly bool[] existsArray;

		public BatchExistsArrayCommandNew
		(
			ArrayPool<byte> bufferPool,
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			bool[] existsArray,
			BatchStatus status
		) : base(bufferPool, cluster, batch, policy, status, false)
		{
			this.keys = keys;
			this.existsArray = existsArray;
		}

		public new void WriteBuffer()
		{
			if (batch.node != null && batch.node.HasBatchAny)
			{
				BatchAttr attr = new(Policy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
				this.SetBatchOperate(batchPolicy, keys, batch, null, null, attr);
			}
			else
			{
				this.SetBatchRead(batchPolicy, keys, batch, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			}
		}

		public new bool ParseRow()
		{
			this.SkipKey(FieldCount);

			if (OpCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			existsArray[BatchIndex] = ResultCode == 0;
			return true;
		}

		public new BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchExistsArrayCommand(Cluster, batchNode, batchPolicy, keys, existsArray, status);
		}

		public new List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(Cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, status);
		}
	}

	//-------------------------------------------------------
	// OperateList
	//-------------------------------------------------------

	public sealed class BatchOperateListCommandNew : BatchCommandNew, ICommand
	{
		private readonly IList<BatchRecord> records;

		public BatchOperateListCommandNew
		(
			ArrayPool<byte> bufferPool,
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			IList<BatchRecord> records,
			BatchStatus status
		) : base(bufferPool, cluster, batch, policy, status, true)
		{
			this.records = records;
		}

		public new bool IsWrite()
		{
			// This method is only called to set inDoubt on node level errors.
			// SetError() will filter out reads when setting record level inDoubt.
			return true;
		}

		public new void WriteBuffer()
		{
			this.SetBatchOperate(batchPolicy, (IList)records, batch);
		}

		public new BatchRecord ParseRow()
		{
			this.SkipKey(FieldCount);

			BatchRecord record = records[BatchIndex];

			if (ResultCode == 0)
			{
				record.SetRecord(this.ParseRecord());
				return record;
			}

			if (ResultCode == Client.ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = this.ParseRecord();
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = ResultCode;
					record.inDoubt = Command.BatchInDoubt(record.hasWrite, CommandSentCounter);
					status.SetRowError();
					return record;
				}
			}

			record.SetError(ResultCode, Command.BatchInDoubt(record.hasWrite, CommandSentCounter));
			status.SetRowError();
			return record;
		}

		public new void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == Client.ResultCode.NO_RESPONSE)
				{
					record.inDoubt = record.hasWrite;
				}
			}
		}

		public new BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchOperateListCommand(Cluster, batchNode, batchPolicy, records, status);
		}

		public new List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(Cluster, batchPolicy, (IList)records, sequenceAP, sequenceSC, batch, status);
		}
	}

	//-------------------------------------------------------
	// OperateArray
	//-------------------------------------------------------

	public sealed class BatchOperateArrayCommandNew : BatchCommandNew, ICommand
	{
		private readonly Key[] keys;
		private readonly Operation[] ops;
		private readonly BatchRecord[] records;
		private readonly BatchAttr attr;

		public BatchOperateArrayCommandNew
		(
			ArrayPool<byte> bufferPool,
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) : base(bufferPool, cluster, batch, batchPolicy, status, ops != null)
		{
			this.keys = keys;
			this.ops = ops;
			this.records = records;
			this.attr = attr;
		}

		public new bool IsWrite()
		{
			return attr.hasWrite;
		}

		public new void WriteBuffer()
		{
			this.SetBatchOperate(batchPolicy, keys, batch, null, ops, attr);
		}

		public new BatchRecord ParseRow()
		{
			this.SkipKey(FieldCount);

			BatchRecord record = records[BatchIndex];

			if (ResultCode == 0)
			{
				record.SetRecord(this.ParseRecord());
			}
			else
			{
				record.SetError(ResultCode, Command.BatchInDoubt(attr.hasWrite, CommandSentCounter));
				status.SetRowError();
			}
			return record;
		}

		public new void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt || !attr.hasWrite)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == Client.ResultCode.NO_RESPONSE)
				{
					record.inDoubt = inDoubt;
				}
			}
		}

		public new BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchOperateArrayCommand(Cluster, batchNode, batchPolicy, keys, ops, records, attr, status);
		}

		public new List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(Cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, status);
		}
	}

	//-------------------------------------------------------
	// UDF
	//-------------------------------------------------------

	public sealed class BatchUDFCommandNew : BatchCommandNew, ICommand
	{
		private readonly Key[] keys;
		private readonly string packageName;
		private readonly string functionName;
		private readonly byte[] argBytes;
		private readonly BatchRecord[] records;
		private readonly BatchAttr attr;

		public BatchUDFCommandNew
		(
			ArrayPool<byte> bufferPool,
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) : base(bufferPool, cluster, batch, batchPolicy, status, false)
		{
			this.keys = keys;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
			this.records = records;
			this.attr = attr;
		}

		public new bool IsWrite()
		{
			return attr.hasWrite;
		}

		public new void WriteBuffer()
		{
			this.SetBatchUDF(batchPolicy, keys, batch, packageName, functionName, argBytes, attr);
		}

		public new BatchRecord ParseRow()
		{
			this.SkipKey(FieldCount);

			BatchRecord record = records[BatchIndex];

			if (ResultCode == 0)
			{
				record.SetRecord(this.ParseRecord());
				return record;
			}

			if (ResultCode == Client.ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = this.ParseRecord();
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = ResultCode;
					record.inDoubt = Command.BatchInDoubt(attr.hasWrite, CommandSentCounter);
					status.SetRowError();
					return record;
				}
			}

			record.SetError(ResultCode, Command.BatchInDoubt(attr.hasWrite, CommandSentCounter));
			status.SetRowError();
			return record;
		}

		public new void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt || !attr.hasWrite)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == Client.ResultCode.NO_RESPONSE)
				{
					record.inDoubt = inDoubt;
				}
			}
		}

		public new BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchUDFCommand(Cluster, batchNode, batchPolicy, keys, packageName, functionName, argBytes, records, attr, status);
		}

		public new List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(Cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, status);
		}
	}

	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	public class BatchCommandNew : ICommand
	{
		public ArrayPool<byte> BufferPool { get; set; }
		public int ServerTimeout { get; set; }
		public int SocketTimeout { get; set; }
		public int TotalTimeout { get; set; }
		public int MaxRetries { get; set; }
		public Cluster Cluster { get; set; }
		public Policy Policy { get; set; }

		public byte[] DataBuffer { get; set; }
		public int DataOffset { get; set; }
		public int Iteration { get; set; }// 1;
		public int CommandSentCounter { get; set; }
		public DateTime Deadline { get; set; }
		public int Info3 { get; set; }
		public int ResultCode { get; set; }
		public int Generation { get; set; }
		public int Expiration { get; set; }
		public int BatchIndex { get; set; }
		public int FieldCount { get; set; }
		public int OpCount { get; set; }
		public bool IsOperation { get; set; }

		private readonly Node node;
		protected internal readonly String ns;
		private readonly ulong clusterKey;
		private readonly bool first;
		protected internal volatile bool valid = true;

		internal readonly BatchNode batch;
		internal readonly BatchPolicy batchPolicy;
		internal readonly BatchStatus status;
		internal BatchExecutor parent;
		internal uint sequenceAP;
		internal uint sequenceSC;
		internal bool splitRetry;

		public BatchCommandNew
		(
			ArrayPool<byte> bufferPool,
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			BatchStatus status,
			bool isOperation
		)
		{
			this.SetCommonProperties(bufferPool, cluster, batchPolicy);
			this.node = batch.node;
			this.IsOperation = isOperation;
			this.ns = null;
			this.clusterKey = 0;
			this.first = false;
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.status = status;
		}

		public Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.BATCH;
		}

		public bool PrepareRetry(bool timeout)
		{
			if (!((batchPolicy.replica == Replica.SEQUENCE || batchPolicy.replica == Replica.PREFER_RACK) &&
				  (parent == null || !parent.IsDone())))
			{
				// Perform regular retry to same node.
				return true;
			}
			sequenceAP++;

			if (!timeout || batchPolicy.readModeSC != ReadModeSC.LINEARIZE)
			{
				sequenceSC++;
			}
			return false;
		}

		public bool IsWrite()
		{
			return false;
		}

		public Node GetNode()
		{
			return node;
		}

		public void WriteBuffer()
		{
			throw new NotImplementedException();
		}

		public async Task ParseResult(IConnection conn, CancellationToken token)
		{
			await CommandHelpers.ParseResult(this, conn, token);
		}

		public KeyRecord ParseGroup(int receiveSize)
		{
			return CommandHelpers.ParseGroup(this, receiveSize);
		}

		public KeyRecord ParseRow()
		{
			throw new NotImplementedException();
		}

		public bool RetryBatch
		(
			Cluster cluster,
			int socketTimeout,
			int totalTimeout,
			DateTime deadline,
			int iteration,
			int commandSentCounter
		)
		{
			// Retry requires keys for this node to be split among other nodes.
			// This is both recursive and exponential.
			List<BatchNode> batchNodes = GenerateBatchNodes();

			if (batchNodes.Count == 1 && batchNodes[0].node == batch.node)
			{
				// Batch node is the same.  Go through normal retry.
				return false;
			}

			splitRetry = true;

			// Run batch requests sequentially in same thread.
			foreach (BatchNode batchNode in batchNodes)
			{
				BatchCommand command = CreateCommand(batchNode);
				command.parent = parent;
				command.sequenceAP = sequenceAP;
				command.sequenceSC = sequenceSC;
				command.socketTimeout = socketTimeout;
				command.totalTimeout = totalTimeout;
				command.iteration = iteration;
				command.commandSentCounter = commandSentCounter;
				command.deadline = deadline;

				try
				{
					cluster.AddRetry();
					command.ExecuteCommand();
				}
				catch (AerospikeException ae)
				{
					if (!command.splitRetry)
					{
						command.SetInDoubt(ae.InDoubt);
					}
					status.SetException(ae);

					if (!batchPolicy.respondAllKeys)
					{
						throw;
					}
				}
				catch (Exception e)
				{
					if (!command.splitRetry)
					{
						command.SetInDoubt(true);
					}
					status.SetException(e);

					if (!batchPolicy.respondAllKeys)
					{
						throw;
					}
				}
			}
			return true;
		}

		public void SetInDoubt(bool inDoubt)
		{
			// Do nothing by default. Batch writes will override this method.
		}

		protected internal BatchCommand CreateCommand(BatchNode batchNode)
		{
			throw new NotImplementedException();
		}
		protected internal List<BatchNode> GenerateBatchNodes()
		{
			throw new NotImplementedException();
		}
	}
}
