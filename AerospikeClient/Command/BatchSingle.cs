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
	public sealed class BatchSingleOperateRead : BatchSingleRead
	{
		private readonly Operation[] ops;

		public BatchSingleOperateRead
		(
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			Operation[] ops,
			Record[] records,
			int index,
			BatchStatus status,
			Node node
		) : base(cluster, policy, key, null, records, index, status, node, true)
		{
			this.ops = ops;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, ops);
		}
	}

	public class BatchSingleRead
	(
		Cluster cluster,
		Policy policy,
		Key key,
		string[] binNames,
		Record[] records,
		int index,
		BatchStatus status,
		Node node,
		bool isOperation
	) : BatchSingleCommand(cluster, policy, status, key, node, false)
	{
		private readonly string[] binNames = binNames;
		private readonly Record[] records = records;
		private readonly int index = index;
		private readonly bool isOperation = isOperation;

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, binNames);
		}

		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);
			ParseFields(policy.Txn, key, false);

			if (resultCode == ResultCode.OK)
			{
				records[index] = ParseRecord(isOperation);
			}
		}
	}

	public sealed class BatchSingleReadHeader
	(
		Cluster cluster,
		Policy policy,
		Key key,
		Record[] records,
		int index,
		BatchStatus status,
		Node node
	) : BatchSingleCommand(cluster, policy, status, key, node, false)
	{
		protected internal override void WriteBuffer()
		{
			SetReadHeader(policy, key);
		}
		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);
			ParseFields(policy.Txn, key, false);

			if (resultCode == ResultCode.OK)
			{
				// Create a record with null bins.
				records[index] = new Record(null, generation, expiration);
			}
		}
	}

	public sealed class BatchSingleReadRecord
	(
		Cluster cluster,
		Policy policy,
		BatchRead record,
		BatchStatus status,
		Node node
	) : BatchSingleCommand(cluster, policy, status, record.key, node, false)
	{ 
		private readonly BatchRead record = record;

		protected internal override void WriteBuffer()
		{
			SetRead(policy, record);
		}

		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);
			ParseFields(policy.Txn, key, false);

			if (resultCode == ResultCode.OK)
			{
				record.SetRecord(ParseRecord(true));
			}
			else
			{
				record.SetError(resultCode, false);
				status.SetRowError();
			}
		}
	}

	public sealed class BatchSingleExists
	(
		Cluster cluster,
		Policy policy,
		Key key,
		bool[] existsArray,
		int index,
		BatchStatus status,
		Node node
	) : BatchSingleCommand(cluster, policy, status, key, node, false)
	{
		private readonly bool[] existsArray = existsArray;
		private readonly int index = index;

		protected internal override void WriteBuffer()
		{
			SetExists(policy, key);
		}

		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);
			ParseFields(policy.Txn, key, false);
			existsArray[index] = (resultCode == ResultCode.OK);
		}
	}

	public sealed class BatchSingleOperateBatchRecord : BatchSingleCommand
	{
		private readonly Operation[] ops;
		private readonly BatchAttr attr;
		private readonly BatchRecord record;

		public BatchSingleOperateBatchRecord
		(
			Cluster cluster,
			BatchPolicy policy,
			Operation[] ops,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) : base(cluster, policy, status, record.key, node, attr.hasWrite)
		{
			this.ops = ops;
			this.attr = attr;
			this.record = record;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(policy, attr, record.key, ops);
		}

		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);
			ParseFields(policy.Txn, key, record.hasWrite);

			if (resultCode == ResultCode.OK)
			{
				record.SetRecord(ParseRecord(true));
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter));
				status.SetRowError();
			}
		}
		public override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}
	}

	public sealed class BatchSingleDelete : BatchSingleCommand
	{
		private readonly BatchAttr attr;
		private readonly BatchRecord record;

		public BatchSingleDelete
		(
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) : base(cluster, policy, status, record.key, node, true)
		{
			this.attr = attr;
			this.record = record;
		}

		protected internal override void WriteBuffer()
		{
			SetDelete(policy, record.key, attr);
		}
		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);
			ParseFields(policy.Txn, record.key, true);

			if (resultCode == ResultCode.OK)
			{
				record.SetRecord(new Record(null, generation, expiration));
			}
			else
			{
				// A KEY_NOT_FOUND_ERROR on a delete is benign, but still results in an overall
				// batch status of false to be consistent with the original batch code.
				record.SetError(resultCode, Command.BatchInDoubt(true, commandSentCounter));
				status.SetRowError();
			}
		}
		public override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}
	}

	public sealed class BatchSingleUDF : BatchSingleCommand
	{
		private readonly string packageName;
		private readonly string functionName;
		private readonly Value[] args;
		private readonly BatchAttr attr;
		private readonly BatchRecord record;

		public BatchSingleUDF
		(
			Cluster cluster,
			BatchPolicy policy,
			string packageName,
			string functionName,
			Value[] args,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) : base(cluster, policy, status, record.key, node, true)
		{
			this.packageName = packageName;
			this.functionName = functionName;
			this.args = args;
			this.attr = attr;
			this.record = record;
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(policy, attr, record.key, packageName, functionName, args);
		}

		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);
			ParseFields(policy.Txn, record.key, true);

			if (resultCode == ResultCode.OK)
			{
				record.SetRecord(ParseRecord(false));
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = ParseRecord(false);
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = Command.BatchInDoubt(true, commandSentCounter);
					status.SetRowError();
				}
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(true, commandSentCounter));
				status.SetRowError();
			}
		}

		public override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}
	}

	//-------------------------------------------------------
	// Transaction
	//-------------------------------------------------------

	public sealed class BatchSingleTxnVerify
	(
		Cluster cluster,
		BatchPolicy policy,
		long version,
		BatchRecord record,
		BatchStatus status,
		Node node
		) : BatchSingleCommand(cluster, policy, status, record.key, node, true)
	{
		private readonly long version = version;
		private readonly BatchRecord record = record;

		protected internal override void WriteBuffer()
		{
			SetTxnVerify(record.key, version);
		}

		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);

			if (resultCode == ResultCode.OK)
			{
				record.resultCode = resultCode;
			}
			else
			{
				record.SetError(resultCode, false);
				status.SetRowError();
			}
		}
	}

	public sealed class BatchSingleTxnRoll
	(
		Cluster cluster,
		BatchPolicy policy,
		Txn txn,
		BatchRecord record,
		BatchStatus status,
		Node node,
		int attr
	) : BatchSingleCommand(cluster, policy, status, record.key, node, true)
	{
		protected internal override void WriteBuffer()
		{
			SetTxnRoll(record.key, txn, attr);
		}

		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);

			if (resultCode == ResultCode.OK)
			{
				record.resultCode = resultCode;
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(true, commandSentCounter));
				status.SetRowError();
			}
		}

		public override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}
	}

	//-------------------------------------------------------
	// Batch Single Base Command
	//-------------------------------------------------------

	public abstract class BatchSingleCommand
	(
		Cluster cluster,
		Policy policy,
		BatchStatus status,
		Key key,
		Node node,
		bool hasWrite
	) : SyncCommand(cluster, policy, key.ns), IBatchCommand 
	{
		public BatchExecutor Parent { get; set; }
		internal readonly BatchStatus status = status;
		internal readonly Key key = key;
		internal Node node = node;
		internal uint sequence;
		internal readonly bool hasWrite = hasWrite;

		public void Run(object obj)
		{
			try
			{
				Execute();
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
				status.SetException(e);
			}
		}

		protected internal override bool IsWrite()
		{
			return hasWrite;
		}

		protected internal override Node GetNode()
		{
			return node;
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.BATCH;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			if (hasWrite)
			{
				Partition p = Partition.Write(cluster, policy, key);
				p.sequence = sequence;
				p.prevNode = node;
				p.PrepareRetryWrite(timeout);
				node = (AsyncNode)p.GetNodeWrite(cluster);
				sequence = p.sequence;
			}
			else
			{
				Partition p = Partition.Read(cluster, policy, key);
				p.sequence = sequence;
				p.prevNode = node;
				p.PrepareRetryRead(timeout);
				node = (AsyncNode)p.GetNodeRead(cluster);
				sequence = p.sequence;
			}
			return true;
		}

		protected internal Record ParseRecord(bool isOperation)
		{
			if (opCount <= 0)
			{
				return new Record(null, generation, expiration);
			}

			return policy.recordParser.ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, isOperation);
		}

		public virtual void SetInDoubt()
		{ }
	}
}
