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
	//-------------------------------------------------------
	// Read
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleReadGetSequence : AsyncBatchSingleRead
	{
		private readonly BatchSequenceListener listener;

		public AsyncBatchSingleReadGetSequence
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchRead record,
			Node node,
			BatchSequenceListener listener
		) : base(executor, cluster, policy, record, node)
		{
			this.listener = listener;
		}

		public AsyncBatchSingleReadGetSequence(AsyncBatchSingleReadGetSequence other) : base(other)
		{
			this.listener = other.listener;
		}

		protected internal override void ParseResult()
		{
			base.ParseResult();
			try
			{
				listener.OnRecord(record);
			}
			catch (Exception e)
			{
				Log.Error("Unexpected exception from OnRecord(): " + e.Message);
			}
		}
	}

	public sealed class AsyncBatchSingleReadSequence : AsyncBatchSingleRead
	{
		private readonly BatchRecordSequenceListener listener;
		private readonly int index;

		public AsyncBatchSingleReadSequence
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchRead record,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) : base(executor, cluster, policy, record, node)
		{
			this.listener = listener;
			this.index = index;
		}

		public AsyncBatchSingleReadSequence(AsyncBatchSingleReadSequence other) : base(other)
		{
			this.listener = other.listener;
			this.index = other.index;
		}

		protected internal override void ParseResult()
		{
			base.ParseResult();
			AsyncBatch.OnRecord(cluster, listener, record, index);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleReadSequence(this);
		}
	}

	public class AsyncBatchSingleRead : AsyncBatchSingleCommand
	{
		protected BatchRead record;
		
		public AsyncBatchSingleRead
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchRead record,
			Node node
		) : base(executor, cluster, policy, record.key, node, false)
		{
			this.record = record;
		}

		public AsyncBatchSingleRead(AsyncBatchSingleRead other) : base(other)
		{
			this.record = other.record;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, record);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			if (resultCode == ResultCode.OK)
			{
				record.SetRecord(ParseRecord(record.ops != null));
			}
			else
			{
				record.SetError(resultCode, false);
				executor.SetRowError();
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleRead(this);
		}
	}

	//-------------------------------------------------------
	// Operate/Get
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleOperateGet : AsyncBatchSingleGet
	{
		private readonly Operation[] ops;

		public AsyncBatchSingleOperateGet
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			Operation[] ops,
			Record[] records,
			Node node,
			int index
		) : base(executor, cluster, policy, key, null, records, node, index, true)
		{
			this.ops = ops;
		}

		public AsyncBatchSingleOperateGet(AsyncBatchSingleOperateGet other) : base(other)
		{
			this.ops = other.ops;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, ops);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleOperateGet(this);
		}
	}

	public class AsyncBatchSingleGet : AsyncBatchSingleCommand
	{
		private readonly string[] binNames;
		private readonly Record[] records;
		private readonly int index;
		private readonly bool isOperation;

		public AsyncBatchSingleGet
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			string[] binNames,
			Record[] records,
			Node node,
			int index,
			bool isOperation
		) : base(executor, cluster, policy, key, node, false)
		{
			this.binNames = binNames;
			this.records = records;
			this.index = index;
			this.isOperation = isOperation;
		}

		public AsyncBatchSingleGet(AsyncBatchSingleGet other) : base(other)
		{
			this.binNames = other.binNames;
			this.records = other.records;
			this.index = other.index;
			this.isOperation = other.isOperation;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, binNames);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			if (resultCode == ResultCode.OK)
			{
				records[index] = ParseRecord(isOperation);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleGet(this);
		}
	}
	
	public sealed class AsyncBatchSingleOperateGetSequence : AsyncBatchSingleGetSequence
	{
		private readonly Operation[] ops;

		public AsyncBatchSingleOperateGetSequence
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			RecordSequenceListener listener,
			Key key,
			Operation[] ops,
			Node node
		) : base(executor, cluster, policy, listener, key, null, node, true)
		{
			this.ops = ops;
		}

		public AsyncBatchSingleOperateGetSequence(AsyncBatchSingleOperateGetSequence other) : base(other)
		{
			this.ops = other.ops;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, ops);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleOperateGetSequence(this);
		}
	}
	
	public class AsyncBatchSingleGetSequence : AsyncBatchSingleCommand
	{
		private readonly RecordSequenceListener listener;
		private readonly string[] binNames;
		private readonly bool isOperation;

		public AsyncBatchSingleGetSequence
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			RecordSequenceListener listener,
			Key key,
			string[] binNames,
			Node node,
			bool isOperation
		) : base(executor, cluster, policy, key, node, false)
		{
			this.listener = listener;
			this.binNames = binNames;
			this.isOperation = isOperation;
		}

		public AsyncBatchSingleGetSequence(AsyncBatchSingleGetSequence other) : base(other)
		{
			this.listener = other.listener;
			this.binNames = other.binNames;
			this.isOperation = other.isOperation;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(policy, key, binNames);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			Record record = null;

			if (resultCode == ResultCode.OK)
			{
				record = ParseRecord(isOperation);
			}
			AsyncBatch.OnRecord(cluster, listener, key, record);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleGetSequence(this);
		}
	}

	//-------------------------------------------------------
	// Read Header
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleReadHeaderSequence : AsyncBatchSingleCommand
	{
		private readonly RecordSequenceListener listener;

		public AsyncBatchSingleReadHeaderSequence
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			Node node,
			RecordSequenceListener listener
		) : base(executor, cluster, policy, key, node, false)
		{
			this.listener = listener;
		}

		public AsyncBatchSingleReadHeaderSequence(AsyncBatchSingleReadHeaderSequence other) : base(other)
		{
			this.listener = other.listener;
		}

		protected internal override void WriteBuffer()
		{
			SetReadHeader(policy, key);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			Record record = null;

			if (resultCode == ResultCode.OK)
			{
				record = ParseRecord(false);
			}
			AsyncBatch.OnRecord(cluster, listener, key, record);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleReadHeaderSequence(this);
		}
	}

	public sealed class AsyncBatchSingleReadHeader : AsyncBatchSingleCommand
	{
		private readonly Record[] records;
		private readonly int index;

		public AsyncBatchSingleReadHeader
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			Record[] records,
			Node node,
			int index
		) : base(executor, cluster, policy, key, node, false)
		{
			this.records = records;
			this.index = index;
		}

		public AsyncBatchSingleReadHeader(AsyncBatchSingleReadHeader other) : base(other)
		{
			this.records = other.records;
			this.index = other.index;
		}

		protected internal override void WriteBuffer()
		{
			SetReadHeader(policy, key);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			if (resultCode == ResultCode.OK)
			{
				records[index] = ParseRecord(false);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleReadHeader(this);
		}
	}

	//-------------------------------------------------------
	// Exists
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleExistsSequence : AsyncBatchSingleCommand
	{
		private readonly ExistsSequenceListener listener;

		public AsyncBatchSingleExistsSequence
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			Node node,
			ExistsSequenceListener listener
		) : base(executor, cluster, policy, key, node, false)
		{
			this.listener = listener;
		}

		public AsyncBatchSingleExistsSequence(AsyncBatchSingleExistsSequence other) : base(other)
		{
			this.listener = other.listener;
		}

		protected internal override void WriteBuffer()
		{
			SetExists(policy, key);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			try
			{
				listener.OnExists(key, resultCode == ResultCode.OK);
			}
			catch (Exception e)
			{
				Log.Error("Unexpected exception from OnExists(): " + e.Message);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleExistsSequence(this);
		}
	}

	public sealed class AsyncBatchSingleExists : AsyncBatchSingleCommand
	{
		private readonly bool[] existsArray;
		private readonly int index;

		public AsyncBatchSingleExists
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			Node node,
			bool[] existsArray,
			int index
		) : base(executor, cluster, policy, key, node, false)
		{
			this.existsArray = existsArray;
			this.index = index;
		}

		public AsyncBatchSingleExists(AsyncBatchSingleExists other) : base(other)
		{
			this.existsArray = other.existsArray;
			this.index = other.index;
		}

		protected internal override void WriteBuffer()
		{
			SetExists(policy, key);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			existsArray[index] = resultCode == ResultCode.OK;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleExists(this);
		}
	}

	//-------------------------------------------------------
	// Operate
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleOperateSequence : AsyncBatchSingleCommand
	{
		private readonly AsyncBatchRecordSequenceExecutor parent;
		private readonly BatchRecordSequenceListener listener;
		private readonly BatchAttr attr;
		private readonly Operation[] ops;
		private readonly int index;

		public AsyncBatchSingleOperateSequence
		(
			AsyncBatchRecordSequenceExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			BatchAttr attr,
			Operation[] ops,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) : base(executor, cluster, policy, key, node, attr.hasWrite)
		{
			this.parent = executor;
			this.listener = listener;
			this.attr = attr;
			this.ops = ops;
			this.index = index;
		}

		public AsyncBatchSingleOperateSequence(AsyncBatchSingleOperateSequence other) : base(other)
		{
			this.parent = other.parent;
			this.listener = other.listener;
			this.attr = other.attr;
			this.ops = other.ops;
			this.index = other.index;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(policy, attr, key, ops);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			BatchRecord record;

			if (resultCode == ResultCode.OK)
			{
				record = new BatchRecord(key, ParseRecord(true), attr.hasWrite);
			}
			else
			{
				record = new BatchRecord(key, null, resultCode, 
					Command.BatchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
				executor.SetRowError();
			}

			parent.SetSent(index);
			AsyncBatch.OnRecord(cluster, listener, record, index);
		}

		internal override void SetInDoubt()
		{
			if (!parent.ExchangeSent(index))
			{
				BatchRecord record = new(key, null, ResultCode.NO_RESPONSE, true, attr.hasWrite);
				AsyncBatch.OnRecord(cluster, listener, record, index);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleOperateSequence(this);
		}
	}

	public sealed class AsyncBatchSingleOperate : AsyncBatchSingleCommand
	{
		private readonly BatchAttr attr;
		private readonly BatchRecord record;
		private readonly Operation[] ops;

		public AsyncBatchSingleOperate
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			Operation[] ops,
			Node node
		) : base(executor, cluster, policy, record.key, node, attr.hasWrite)
		{
			this.attr = attr;
			this.record = record;
			this.ops = ops;
		}

		public AsyncBatchSingleOperate(AsyncBatchSingleOperate other) : base(other)
		{
			this.attr = other.attr;
			this.record = other.record;
			this.ops = other.ops;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(policy, attr, record.key, ops);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			if (resultCode == ResultCode.OK)
			{
				record.SetRecord(ParseRecord(true));
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, commandSentCounter));
				executor.SetRowError();
			}
		}

		internal override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleOperate(this);
		}
	}


	//-------------------------------------------------------
	// Write
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleWriteSequence : AsyncBatchSingleWrite
	{
		private readonly BatchRecordSequenceListener listener;
		private readonly int index;

		public AsyncBatchSingleWriteSequence
		(
			AsyncBatchOperateSequenceExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchWrite record,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) : base(executor, cluster, policy, attr, record, node)
		{
			this.listener = listener;
			this.index = index;
		}

		public AsyncBatchSingleWriteSequence(AsyncBatchSingleWriteSequence other) : base(other)
		{
			this.listener = other.listener;
			this.index = other.index;
		}

		protected internal override void ParseResult()
		{
			base.ParseResult();
			AsyncBatch.OnRecord(cluster, listener, record, index);
		}

		// SetInDoubt() is not overridden to call onRecord() because user already has access to full
		// BatchRecord list and can examine each record for inDoubt when the exception occurs.
	}

	public class AsyncBatchSingleWrite : AsyncBatchSingleCommand
	{
		private readonly BatchAttr attr;
		protected BatchWrite record;

		public AsyncBatchSingleWrite
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchWrite record,
			Node node
		) : base(executor, cluster, policy, record.key, node, true)
		{
			this.attr = attr;
			this.record = record;
		}

		public AsyncBatchSingleWrite(AsyncBatchSingleWrite other) : base(other)
		{
			this.attr = other.attr;
			this.record = other.record;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(policy, attr, record.key, record.ops);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			if (resultCode == ResultCode.OK)
			{
				record.SetRecord(ParseRecord(true));
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(true, commandSentCounter));
				executor.SetRowError();
			}
		}

		internal override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleWrite(this);
		}
	}

	//-------------------------------------------------------
	// UDF
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleUDFSequence : AsyncBatchSingleUDF
	{
		private readonly BatchRecordSequenceListener listener;
		private readonly int index;

		public AsyncBatchSingleUDFSequence
		(
			AsyncBatchOperateSequenceExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchUDF record,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) : base(executor, cluster, policy, attr, record, node)
		{
			this.listener = listener;
			this.index = index;
		}

		public AsyncBatchSingleUDFSequence(AsyncBatchSingleUDFSequence other) : base(other)
		{
			this.listener = other.listener;
			this.index = other.index;
		}

		protected internal override void ParseResult()
		{
			base.ParseResult();
			AsyncBatch.OnRecord(cluster, listener, record, index);
		}

		// setInDoubt() is not overridden to call onRecord() because user already has access to full
		// BatchRecord list and can examine each record for inDoubt when the exception occurs.

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleUDFSequence(this);
		}
	}

	public class AsyncBatchSingleUDF : AsyncBatchSingleCommand
	{
		private readonly BatchAttr attr;
		protected readonly BatchUDF record;

		public AsyncBatchSingleUDF
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchUDF record,
			Node node
		) : base(executor, cluster, policy, record.key, node, true)
		{
			this.attr = attr;
			this.record = record;
		}

		public AsyncBatchSingleUDF(AsyncBatchSingleUDF other) : base(other)
		{
			this.attr = other.attr;
			this.record = other.record;
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(policy, attr, key, record.packageName, record.functionName, record.functionArgs);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

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
				}
				record.resultCode = resultCode;
				record.inDoubt = Command.BatchInDoubt(true, commandSentCounter);
				executor.SetRowError();
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(true, commandSentCounter));
				executor.SetRowError();
			}
		}

		internal override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleUDF(this);
		}
	}

	public sealed class AsyncBatchSingleUDFSequenceCommand : AsyncBatchSingleCommand
	{
		private readonly AsyncBatchRecordSequenceExecutor parent;
		private readonly BatchRecordSequenceListener listener;
		private readonly BatchAttr attr;
		private readonly string packageName;
		private readonly string functionName;
		private readonly byte[] argBytes;
		private readonly int index;

		public AsyncBatchSingleUDFSequenceCommand
		(
			AsyncBatchRecordSequenceExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			BatchAttr attr,
			string packageName,
			string functionName,
			byte[] argBytes,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) : base(executor, cluster, policy, key, node, true)
		{
			this.parent = executor;
			this.listener = listener;
			this.attr = attr;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
			this.index = index;
		}

		public AsyncBatchSingleUDFSequenceCommand(AsyncBatchSingleUDFSequenceCommand other) : base(other)
		{
			this.parent = other.parent;
			this.listener = other.listener;
			this.attr = other.attr;
			this.packageName = other.packageName;
			this.functionName = other.functionName;
			this.argBytes = other.argBytes;
			this.index = other.index;
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(policy, attr, key, packageName, functionName, argBytes);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			BatchRecord record;
			if (resultCode == ResultCode.OK)
			{
				record = new BatchRecord(key, ParseRecord(true), attr.hasWrite);
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = ParseRecord(false);
				string m = r.GetString("FAILURE");
				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record = new BatchRecord(key, r, resultCode, Command.BatchInDoubt(true, commandSentCounter), true);
				}
				else
				{
					record = new BatchRecord(key, null, resultCode, Command.BatchInDoubt(true, commandSentCounter), true);
				}
				executor.SetRowError();
			}
			else
			{
				record = new BatchRecord(key, null, resultCode, Command.BatchInDoubt(true, commandSentCounter), true);
				executor.SetRowError();
			}
			parent.SetSent(index);
			AsyncBatch.OnRecord(cluster, listener, record, index);
		}

		internal override void SetInDoubt()
		{
			if (!parent.ExchangeSent(index))
			{
				BatchRecord record = new(key, null, ResultCode.NO_RESPONSE, true, true);
				AsyncBatch.OnRecord(cluster, listener, record, index);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleUDFSequenceCommand(this);
		}
	}

	public sealed class AsyncBatchSingleUDFCommand : AsyncBatchSingleCommand
	{
		private readonly BatchAttr attr;
		private readonly BatchRecord record;
		private readonly string packageName;
		private readonly string functionName;
		private readonly byte[] argBytes;

		public AsyncBatchSingleUDFCommand
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			string packageName,
			string functionName,
			byte[] argBytes,
			Node node
		) : base(executor, cluster, policy, record.key, node, true)
		{
			this.attr = attr;
			this.record = record;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
		}

		public AsyncBatchSingleUDFCommand(AsyncBatchSingleUDFCommand other) : base(other)
		{
			this.attr = other.attr;
			this.record = other.record;
			this.packageName = other.packageName;
			this.functionName = other.functionName;
			this.argBytes = other.argBytes;
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(policy, attr, key, packageName, functionName, argBytes);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

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
				}
				record.resultCode = resultCode;
				record.inDoubt = Command.BatchInDoubt(true, commandSentCounter);
				executor.SetRowError();
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(true, commandSentCounter));
				executor.SetRowError();
			}
		}

		internal override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleUDFCommand(this);
		}
	}

	//-------------------------------------------------------
	// Delete
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleDeleteSequenceSent : AsyncBatchSingleCommand
	{
		private readonly  AsyncBatchRecordSequenceExecutor parent;
		private readonly BatchRecordSequenceListener listener;
		private readonly BatchAttr attr;
		private readonly int index;

		public AsyncBatchSingleDeleteSequenceSent
		(
			AsyncBatchRecordSequenceExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Key key,
			BatchAttr attr,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) : base(executor, cluster, policy, key, node, true)
		{
			this.parent = executor;
			this.listener = listener;
			this.attr = attr;
			this.index = index;
		}

		public AsyncBatchSingleDeleteSequenceSent(AsyncBatchSingleDeleteSequenceSent other) : base(other)
		{
			this.parent = other.parent;
			this.listener = other.listener;
			this.attr = other.attr;
			this.index = other.index;
		}

		protected internal override void WriteBuffer()
		{
			SetDelete(policy, key, attr);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			BatchRecord record;

			if (resultCode == ResultCode.OK)
			{
				record = new BatchRecord(key, new Record(null, generation, expiration), true);
			}
			else
			{
				record = new BatchRecord(key, null, resultCode, Command.BatchInDoubt(true, commandSentCounter), true);
				executor.SetRowError();
			}

			parent.SetSent(index);
			AsyncBatch.OnRecord(cluster, listener, record, index);
		}

		internal override void SetInDoubt()
		{
			if (!parent.ExchangeSent(index))
			{
				BatchRecord record = new(key, null, ResultCode.NO_RESPONSE, true, true);
				AsyncBatch.OnRecord(cluster, listener, record, index);
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleDeleteSequenceSent(this);
		}
	}

	public sealed class AsyncBatchSingleDeleteSequence : AsyncBatchSingleDelete
	{
		private readonly BatchRecordSequenceListener listener;
		private readonly int index;

		public AsyncBatchSingleDeleteSequence
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) : base(executor, cluster, policy, attr, record, node)
		{
			this.listener = listener;
			this.index = index;
		}

		public AsyncBatchSingleDeleteSequence(AsyncBatchSingleDeleteSequence other) : base(other)
		{
			this.listener = other.listener;
			this.index = other.index;
		}

		protected internal override void ParseResult()
		{
			base.ParseResult();
			AsyncBatch.OnRecord(cluster, listener, record, index);
		}

		// SetInDoubt() is not overridden to call OnRecord() because user already has access to full
		// BatchRecord list and can examine each record for inDoubt when the exception occurs.

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleDeleteSequence(this);
		}
	}

	public class AsyncBatchSingleDelete : AsyncBatchSingleCommand
	{
		private readonly BatchAttr attr;
		protected BatchRecord record;

		public AsyncBatchSingleDelete
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			Node node
		) : base(executor, cluster, policy, record.key, node, true)
		{
			this.attr = attr;
			this.record = record;
		}

		public AsyncBatchSingleDelete(AsyncBatchSingleDelete other) : base(other)
		{
			this.attr = other.attr;
			this.record = other.record;
		}

		protected internal override void WriteBuffer()
		{
			SetDelete(policy, record.key, attr);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, key, hasWrite);

			if (resultCode == ResultCode.OK)
			{
				record.SetRecord(new Record(null, generation, expiration));
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(true, commandSentCounter));
				executor.SetRowError();
			}
		}

		internal override void SetInDoubt()
		{
			if (record.resultCode == ResultCode.NO_RESPONSE)
			{
				record.inDoubt = true;
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleDelete(this);
		}
	}

	//-------------------------------------------------------
	// Transaction
	//-------------------------------------------------------

	public sealed class AsyncBatchSingleTxnVerify : AsyncBatchSingleCommand
	{
		private readonly long version;
		private readonly BatchRecord record;

		public AsyncBatchSingleTxnVerify
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			long version,
			BatchRecord record,
			Node node
		) : base(executor, cluster, policy, record.key, node, false)
		{
			this.version = version;
			this.record = record;
		}

		public AsyncBatchSingleTxnVerify(AsyncBatchSingleTxnVerify other) : base(other)
		{
			this.version = other.version;
			this.record = other.record;
		}

		protected internal override void WriteBuffer()
		{
			SetTxnVerify(record.key, version);
		}
		protected internal override void ParseResult()
		{
			ParseHeader();

			if (resultCode == ResultCode.OK)
			{
				record.resultCode = resultCode;
			}
			else
			{
				record.SetError(resultCode, false);
				executor.SetRowError();
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleTxnVerify(this);
		}
	}

	public sealed class AsyncBatchSingleTxnRoll : AsyncBatchSingleCommand
	{
		private readonly Txn txn;
		private readonly BatchRecord record;
		private readonly int attr;

		public AsyncBatchSingleTxnRoll
		(
			AsyncBatchExecutor executor,
			AsyncCluster cluster,
			BatchPolicy policy,
			Txn txn,
			BatchRecord record,
			Node node,
			int attr
		) : base(executor, cluster, policy, record.key, node, true)
		{
			this.txn = txn;
			this.record = record;
			this.attr = attr;
		}

		public AsyncBatchSingleTxnRoll(AsyncBatchSingleTxnRoll other) : base(other)
		{
			this.txn = other.txn;
			this.record = other.record;
			this.attr = other.attr;
		}

		protected internal override void WriteBuffer()
		{
			SetTxnRoll(record.key, txn, attr);
		}

		protected internal override void ParseResult()
		{
			ParseHeader();

			if (resultCode == ResultCode.OK)
			{
				record.resultCode = resultCode;
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(true, commandSentCounter));
				executor.SetRowError();
			}
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncBatchSingleTxnRoll(this);
		}
	}

	//-------------------------------------------------------
	// Async Batch Base Command
	//-------------------------------------------------------

	public abstract class AsyncBatchSingleCommand : AsyncCommand
	{
		internal readonly AsyncBatchExecutor executor;
		internal readonly Key key;
		internal uint sequence;
		internal readonly bool hasWrite;

		public AsyncBatchSingleCommand(AsyncBatchExecutor executor, AsyncCluster cluster, Policy policy, Key key, Node node, bool hasWrite)
			: base(cluster, policy, key.ns)
		{
			this.executor = executor;
			this.key = key;
			this.node = (AsyncNode)node;
			this.hasWrite = hasWrite;
		}

		public AsyncBatchSingleCommand(AsyncBatchSingleCommand other) : base(other)
		{
			this.executor = other.executor;
			this.key = other.key;
			this.node = other.node;
			this.sequence = other.sequence;
			this.hasWrite = other.hasWrite;
		}

		protected internal override bool IsWrite()
		{
			return hasWrite;
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return node;
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.BATCH;
		}

		protected internal sealed override void ParseCommand()
		{
			ParseResult();
			Finish();
		}

		protected internal abstract void ParseResult();

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

		protected internal override void OnSuccess()
		{
			executor.ChildSuccess((AsyncNode)node);
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (e.InDoubt)
			{
				SetInDoubt();
			}
			executor.ChildFailure(e);
		}

		internal virtual void SetInDoubt()
		{
			// Do nothing by default. Batch writes will override this method.
		}

		protected internal Record ParseRecord(bool isOperation)
		{
			if (opCount <= 0)
			{
				return new Record(null, generation, expiration);
			}

			return policy.recordParser.ParseRecord(dataBuffer, ref dataOffset, opCount, generation, expiration, isOperation);
		}
	}
}
