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
using System.Collections;

namespace Aerospike.Client
{
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public sealed class BatchReadListCommand : BatchCommand
	{
		private readonly List<BatchRead> records;

		public BatchReadListCommand
		(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			List<BatchRead> records,
			BatchStatus status
		) : base(cluster, batch, policy, status, true)
		{
			this.records = records;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node != null && batch.node.HasBatchAny)
			{
				SetBatchOperate(batchPolicy, null, null, null, records, batch, null);
			}
			else
			{
				SetBatchRead(batchPolicy, records, batch);
			}
		}

		protected internal override bool ParseRow()
		{
			BatchRead record = records[batchIndex];

			ParseFieldsRead(record.key);

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else
			{
				record.SetError(resultCode, false);
				status.SetRowError();
			}
			return true;
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchReadListCommand(cluster, batchNode, batchPolicy, records, status);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, status);
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public sealed class BatchGetArrayCommand : BatchCommand
	{
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Operation[] ops;
		private readonly Record[] records;
		private readonly int readAttr;

		public BatchGetArrayCommand
		(
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
		) : base(cluster, batch, policy, status, isOperation)
		{
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node != null && batch.node.HasBatchAny)
			{
				BatchAttr attr = new(policy, readAttr, ops);
				SetBatchOperate(batchPolicy, keys, batch, binNames, ops, attr);
			}
			else
			{
				SetBatchRead(batchPolicy, keys, batch, binNames, ops, readAttr);
			}
		}

		protected internal override bool ParseRow()
		{
			ParseFieldsRead(keys[batchIndex]);

			if (resultCode == 0)
			{
				records[batchIndex] = ParseRecord();
			}
			return true;
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchGetArrayCommand(cluster, batchNode, batchPolicy, keys, binNames, ops, records, readAttr, isOperation, status);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, status);
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public sealed class BatchExistsArrayCommand : BatchCommand
	{
		private readonly Key[] keys;
		private readonly bool[] existsArray;

		public BatchExistsArrayCommand
		(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			bool[] existsArray,
			BatchStatus status
		) : base(cluster, batch, policy, status, false)
		{
			this.keys = keys;
			this.existsArray = existsArray;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node != null && batch.node.HasBatchAny)
			{
				BatchAttr attr = new(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
				SetBatchOperate(batchPolicy, keys, batch, null, null, attr);
			}
			else
			{
				SetBatchRead(batchPolicy, keys, batch, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			}
		}

		protected internal override bool ParseRow()
		{
			ParseFieldsRead(keys[batchIndex]);
			existsArray[batchIndex] = resultCode == 0;
			return true;
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchExistsArrayCommand(cluster, batchNode, batchPolicy, keys, existsArray, status);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, status);
		}
	}

	//-------------------------------------------------------
	// OperateList
	//-------------------------------------------------------

	public sealed class BatchOperateListCommand : BatchCommand
	{
		private readonly List<BatchRecord> records;
		private readonly IConfigProvider configProvider;

		public BatchOperateListCommand
		(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			List<BatchRecord> records,
			BatchStatus status,
			IConfigProvider configProvider
		) : base(cluster, batch, policy, status, true)
		{
			this.records = records;
			this.configProvider = configProvider;
		}

		protected internal override bool IsWrite()
		{
			// This method is only called to set inDoubt on node level errors.
			// SetError() will filter out reads when setting record level inDoubt.
			return true;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(batchPolicy, cluster.client.BatchWritePolicyDefault, cluster.client.BatchUDFPolicyDefault,
				cluster.client.BatchDeletePolicyDefault, records, batch, configProvider);
		}

		protected internal override bool ParseRow()
		{
			BatchRecord record = records[batchIndex];

			ParseFields(record);

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
				return true;
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
					status.SetRowError();
					return true;
				}
			}

			record.SetError(resultCode, Command.BatchInDoubt(record.hasWrite, commandSentCounter));
			status.SetRowError();
			return true;
		}

		protected internal override void InDoubt()
		{
			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.inDoubt = record.hasWrite;

					if (record.inDoubt && policy.Txn != null)
					{
						policy.Txn.OnWriteInDoubt(record.key);
					}
				}
			}
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchOperateListCommand(cluster, batchNode, batchPolicy, records, status, configProvider);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, (IList)records, sequenceAP, sequenceSC, batch, status);
		}
	}

	//-------------------------------------------------------
	// OperateArray
	//-------------------------------------------------------

	public sealed class BatchOperateArrayCommand : BatchCommand
	{
		private readonly Key[] keys;
		private readonly Operation[] ops;
		private readonly BatchRecord[] records;
		private readonly BatchAttr attr;

		public BatchOperateArrayCommand
		(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) : base(cluster, batch, batchPolicy, status, ops != null)
		{
			this.keys = keys;
			this.ops = ops;
			this.records = records;
			this.attr = attr;
		}

		protected internal override bool IsWrite()
		{
			return attr.hasWrite;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(batchPolicy, keys, batch, null, ops, attr);
		}

		protected internal override bool ParseRow()
		{
			BatchRecord record = records[batchIndex];

			ParseFields(record);

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter));
				status.SetRowError();
			}
			return true;
		}

		protected internal override void InDoubt()
		{
			if (!attr.hasWrite)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.inDoubt = true;

					if (policy.Txn != null)
					{
						policy.Txn.OnWriteInDoubt(record.key);
					}
				}
			}
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchOperateArrayCommand(cluster, batchNode, batchPolicy, keys, ops, records, attr, status);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, status);
		}
	}

	//-------------------------------------------------------
	// UDF
	//-------------------------------------------------------

	public sealed class BatchUDFCommand : BatchCommand
	{
		private readonly Key[] keys;
		private readonly string packageName;
		private readonly string functionName;
		private readonly byte[] argBytes;
		private readonly BatchRecord[] records;
		private readonly BatchAttr attr;

		public BatchUDFCommand
		(
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
		) : base(cluster, batch, batchPolicy, status, false)
		{
			this.keys = keys;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
			this.records = records;
			this.attr = attr;
		}

		protected internal override bool IsWrite()
		{
			return attr.hasWrite;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchUDF(batchPolicy, keys, batch, packageName, functionName, argBytes, attr);
		}

		protected internal override bool ParseRow()
		{
			BatchRecord record = records[batchIndex];

			ParseFields(record);

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
				return true;
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
					status.SetRowError();
					return true;
				}
			}

			record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter));
			status.SetRowError();
			return true;
		}

		protected internal override void InDoubt()
		{
			if (!attr.hasWrite)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.inDoubt = true;

					if (policy.Txn != null)
					{
						policy.Txn.OnWriteInDoubt(record.key);
					}
				}
			}
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchUDFCommand(cluster, batchNode, batchPolicy, keys, packageName, functionName, argBytes, records, attr, status);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, status);
		}
	}

	//-------------------------------------------------------
	// Transaction
	//-------------------------------------------------------

	public sealed class BatchTxnVerify : BatchCommand
	{
		private readonly Key[] keys;
		private readonly long?[] versions;
		private readonly BatchRecord[] records;

		public BatchTxnVerify(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			long?[] versions,
			BatchRecord[] records,
			BatchStatus status
		) : base(cluster, batch, batchPolicy, status, false)
		{
			this.keys = keys;
			this.versions = versions;
			this.records = records;
		}

		protected internal override bool IsWrite()
		{
			return false;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchTxnVerify(batchPolicy, keys, versions, batch);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == ResultCode.OK)
			{
				record.resultCode = resultCode;
			}
			else
			{
				record.SetError(resultCode, false);
				status.SetRowError();
			}
			return true;
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchTxnVerify(cluster, batchNode, batchPolicy, keys, versions, records, status);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, false, status);
		}
	}

	public sealed class BatchTxnRoll : BatchCommand
	{
		private readonly Txn txn;
		private readonly Key[] keys;
		private readonly BatchRecord[] records;
		private readonly BatchAttr attr;

		public BatchTxnRoll(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Txn txn,
			Key[] keys,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) : base(cluster, batch, batchPolicy, status, false)
		{
			this.txn = txn;
			this.keys = keys;
			this.records = records;
			this.attr = attr;
		}

		protected internal override bool IsWrite()
		{
			return attr.hasWrite;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchTxnRoll(batchPolicy, txn, keys, batch, attr);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0)
			{
				record.resultCode = resultCode;
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter));
				status.SetRowError();
			}
			return true;
		}

		protected internal override void InDoubt()
		{
			if (!attr.hasWrite)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.inDoubt = true;
				}
			}
		}

		protected internal override BatchCommand CreateCommand(BatchNode batchNode)
		{
			return new BatchTxnRoll(cluster, batchNode, batchPolicy, txn, keys, records, attr, status);
		}

		protected internal override List<BatchNode> GenerateBatchNodes()
		{
			return BatchNode.GenerateList(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, status);
		}
	}

	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	public abstract class BatchCommand : MultiCommand, IBatchCommand
	{
		public BatchExecutor Parent { get; set; }

		internal readonly BatchNode batch;
		internal readonly BatchPolicy batchPolicy;
		internal readonly BatchStatus status;
		internal uint sequenceAP;
		internal uint sequenceSC;
		internal bool splitRetry;

		public BatchCommand
		(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			BatchStatus status,
			bool isOperation
		) : base(cluster, batchPolicy, batch.node, isOperation)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.status = status;
		}

		public void Run(object obj)
		{
			try
			{
				if (!splitRetry)
				{
					Execute();
				}
				else
				{
					ExecuteCommand();
				}
			}
			catch (AerospikeException ae)
			{
				if (ae.InDoubt)
				{
					SetInDoubt();
				}
				status.SetException(ae);
			}
			catch (Exception e)
			{
				SetInDoubt();
				status.SetException(new AerospikeException(e));
			}
			finally
			{
				Parent.OnComplete();
			}
		}

		protected void ParseFieldsRead(Key key)
		{
			if (policy.Txn != null)
			{
				long? version = ParseVersion(fieldCount);
				policy.Txn.OnRead(key, version);
			}
			else
			{
				SkipKey(fieldCount);
			}
		}

		protected void ParseFields(BatchRecord br)
		{
			if (policy.Txn != null)
			{
				long? version = ParseVersion(fieldCount);

				if (br.hasWrite)
				{
					policy.Txn.OnWrite(br.key, version, resultCode);
				}
				else
				{
					policy.Txn.OnRead(br.key, version);
				}
			}
			else
			{
				SkipKey(fieldCount);
			}
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.BATCH;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			if (!((batchPolicy.replica == Replica.SEQUENCE || batchPolicy.replica == Replica.PREFER_RACK) &&
				  (Parent == null || !Parent.IsDone())))
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

		protected internal override bool RetryBatch
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
				command.Parent = Parent;
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
					if (ae.InDoubt)
					{
						SetInDoubt();
					}
					status.SetException(ae);
				}
				catch (Exception e)
				{
					SetInDoubt();
					status.SetException(new AerospikeException(e));
				}
			}
			return true;
		}

		public void SetInDoubt()
		{
			// Set error/inDoubt for keys associated this batch command when
			// the command was not retried and split. If a split retry occurred,
			// those new subcommands have already set inDoubt on the affected
			// subset of keys.
			if (!splitRetry)
			{
				InDoubt();
			}
		}

		protected internal virtual void InDoubt()
		{
			// Do nothing by default. Batch writes will override this method.
		}

		protected internal abstract BatchCommand CreateCommand(BatchNode batchNode);
		protected internal abstract List<BatchNode> GenerateBatchNodes();
	}

	public interface IBatchCommand
	{
		BatchExecutor Parent { get; set; }

		void Execute();

		void SetInDoubt();

		void Run(object obj);
	}
}
