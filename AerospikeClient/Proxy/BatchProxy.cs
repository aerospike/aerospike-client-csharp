/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using Aerospike.Client.KVS;
using Google.Protobuf;
using Grpc.Core;
using System.Collections;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public sealed class BatchReadListCommandProxy : BatchCommandProxy
	{
		private readonly List<BatchRead> records;

		public BatchReadListCommandProxy
		(
			Buffer buffer,
			CallInvoker invoker,
			BatchNode batch,
			BatchPolicy policy,
			List<BatchRead> records,
			BatchStatus status
		) : base(buffer, invoker, batch, policy, status, true)
		{
			this.records = records;
		}

		protected internal override void WriteBuffer()
		{
			if (batch.node != null && batch.node.HasBatchAny)
			{
				SetBatchOperate(batchPolicy, records, batch);
			}
			else
			{
				SetBatchRead(batchPolicy, records, batch);
			}
		}

		protected internal override bool ParseRow()
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
				status.SetRowError();
			}
			return true;
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public sealed class BatchGetArrayCommandProxy : BatchCommandProxy
	{
		private readonly Key[] keys;
		private readonly string[] binNames;
		private readonly Operation[] ops;
		private readonly Record[] records;
		private readonly int readAttr;

		public BatchGetArrayCommandProxy
		(
			Buffer buffer,
			CallInvoker invoker,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			string[] binNames,
			Operation[] ops,
			Record[] records,
			int readAttr,
			bool isOperation,
			BatchStatus status
		) : base(buffer, invoker, batch, policy, status, isOperation)
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
			SkipKey(fieldCount);

			if (resultCode == 0)
			{
				records[batchIndex] = ParseRecord();
			}
			return true;
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public sealed class BatchExistsArrayCommandProxy : BatchCommandProxy
	{
		private readonly Key[] keys;
		private readonly bool[] existsArray;

		public BatchExistsArrayCommandProxy
		(
			Buffer buffer,
			CallInvoker invoker,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			bool[] existsArray,
			BatchStatus status
		) : base(buffer, invoker, batch, policy, status, false)
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
			SkipKey(fieldCount);

			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			existsArray[batchIndex] = resultCode == 0;
			return true;
		}
	}

	//-------------------------------------------------------
	// OperateList
	//-------------------------------------------------------

	public sealed class BatchOperateListCommandProxy : BatchCommandProxy
	{
		private readonly IList<BatchRecord> records;

		public BatchOperateListCommandProxy
		(
			Buffer buffer,
			CallInvoker invoker,
			BatchNode batch,
			BatchPolicy policy,
			IList<BatchRecord> records,
			BatchStatus status
		) : base(buffer, invoker, batch, policy, status, true)
		{
			this.records = records;
		}

		protected internal override bool IsWrite()
		{
			// This method is only called to set inDoubt on node level errors.
			// SetError() will filter out reads when setting record level inDoubt.
			return true;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(batchPolicy, (IList)records, batch);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

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
					record.inDoubt = Command.BatchInDoubt(record.hasWrite, 0);
					status.SetRowError();
					return true;
				}
			}

			record.SetError(resultCode, Command.BatchInDoubt(record.hasWrite, 0));
			status.SetRowError();
			return true;
		}

		protected internal override void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.inDoubt = record.hasWrite;
				}
			}
		}
	}

	//-------------------------------------------------------
	// OperateArray
	//-------------------------------------------------------

	public sealed class BatchOperateArrayCommandProxy : BatchCommandProxy
	{
		private readonly Key[] keys;
		private readonly Operation[] ops;
		private readonly BatchRecord[] records;
		private readonly BatchAttr attr;

		public BatchOperateArrayCommandProxy
		(
			Buffer buffer,
			CallInvoker invoker,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) : base(buffer, invoker, batch, batchPolicy, status, ops != null)
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
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else
			{
				record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, 0));
				status.SetRowError();
			}
			return true;
		}

		protected internal override void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt || !attr.hasWrite)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.inDoubt = inDoubt;
				}
			}
		}
	}

	//-------------------------------------------------------
	// UDF
	//-------------------------------------------------------

	public sealed class BatchUDFCommandProxy : BatchCommandProxy
	{
		private readonly Key[] keys;
		private readonly string packageName;
		private readonly string functionName;
		private readonly byte[] argBytes;
		private readonly BatchRecord[] records;
		private readonly BatchAttr attr;

		public BatchUDFCommandProxy
		(
			Buffer buffer,
			CallInvoker invoker,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) : base(buffer, invoker, batch, batchPolicy, status, false)
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
			SkipKey(fieldCount);

			BatchRecord record = records[batchIndex];

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
					record.inDoubt = Command.BatchInDoubt(attr.hasWrite, 0);
					status.SetRowError();
					return true;
				}
			}

			record.SetError(resultCode, Command.BatchInDoubt(attr.hasWrite, 0));
			status.SetRowError();
			return true;
		}

		protected internal override void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt || !attr.hasWrite)
			{
				return;
			}

			foreach (int index in batch.offsets)
			{
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE)
				{
					record.inDoubt = inDoubt;
				}
			}
		}
	}

	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	public abstract class BatchCommandProxy : GRPCCommand
	{
		internal readonly BatchNode batch;
		internal readonly BatchPolicy batchPolicy;
		internal readonly BatchStatus status;

		public BatchCommandProxy
		(
			Buffer buffer,
			CallInvoker invoker,
			BatchNode batch,
			BatchPolicy batchPolicy,
			BatchStatus status,
			bool isOperation
		) : base(buffer, invoker, batchPolicy, isOperation)
		{
			this.batch = batch;
			this.batchPolicy = batchPolicy;
			this.status = status;
		}

		protected internal virtual void SetInDoubt(bool inDoubt)
		{
			// Do nothing by default. Batch writes will override this method.
		}

		public void Execute()
		{
			CancellationTokenSource source = new();
			Execute(source.Token).Wait();
		}

		public async Task Execute(CancellationToken token)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset)
			};
			GRPCConversions.SetRequestPolicy(batchPolicy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(CallInvoker);
				var deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var stream = client.BatchOperate(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxyStream(stream);
				await ParseResult(conn, token);
			}
			catch (EndOfGRPCStream)
			{
				// continue
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}
	}
}
