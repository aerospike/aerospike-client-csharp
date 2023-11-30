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
using Grpc.Net.Client;
using System.Collections;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{

	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------
	public sealed class BatchReadListCommandProxy : BatchCommandProxy
	{
		public BatchReadListCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			BatchNode batch,
			BatchPolicy policy,
			List<BatchRead> records,
			BatchStatus status
		) : base(buffer, channel, batch, policy, records.ToArray(), status, true)
		{
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(BatchPolicy, (IList)Records, Batch);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(FieldCount);

			BatchRead record = (BatchRead)Records[BatchIndex];

			if (ResultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else
			{
				record.SetError(ResultCode, false);
				Status.SetRowError();
			}
			return true;
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public sealed class BatchGetArrayCommandProxy : BatchCommandProxy
	{
		private string[] BinNames { get; }
		private Operation[] Ops { get; }
		private int ReadAttr { get; }

		public BatchGetArrayCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			BatchNode batch,
			BatchPolicy policy,
			string[] binNames,
			Operation[] ops,
			BatchRecord[] records,
			int readAttr,
			bool isOperation,
			BatchStatus status
		) : base(buffer, channel, batch, policy, records, status, isOperation)
		{
			this.BinNames = binNames;
			this.Ops = ops;
			this.ReadAttr = readAttr;
		}

		protected internal override void WriteBuffer()
		{
			BatchAttr attr = new(BatchPolicy, ReadAttr, Ops);
			SetBatchOperate(BatchPolicy, (BatchRecord[])Records, Batch, BinNames, Ops, attr);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(FieldCount);

			if (ResultCode == 0)
			{
				Records[BatchIndex].record = ParseRecord();
			}
			return true;
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public sealed class BatchExistsArrayCommandProxy : BatchCommandProxy
	{
		private bool[] ExistsArray { get; }

		public BatchExistsArrayCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			BatchNode batch,
			BatchPolicy policy,
			BatchRecord[] records,
			bool[] existsArray,
			BatchStatus status
		) : base(buffer, channel, batch, policy, records, status, false)
		{
			this.ExistsArray = existsArray;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(BatchPolicy, Records.ToArray(), Batch);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(FieldCount);

			if (OpCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			ExistsArray[BatchIndex] = ResultCode == 0;
			return true;
		}
	}

	//-------------------------------------------------------
	// OperateList
	//-------------------------------------------------------

	public sealed class BatchOperateListCommandProxy : BatchCommandProxy
	{
		public BatchOperateListCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			BatchNode batch,
			BatchPolicy policy,
			IList<BatchRecord> records,
			BatchStatus status
		) : base(buffer, channel, batch, policy, records, status, true)
		{
		}

		protected internal override bool IsWrite()
		{
			// This method is only called to set inDoubt on node level errors.
			// SetError() will filter out reads when setting record level inDoubt.
			return true;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(BatchPolicy, (IList)Records, Batch);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(FieldCount);

			BatchRecord record = Records[BatchIndex];

			if (ResultCode == 0)
			{
				record.SetRecord(ParseRecord());
				return true;
			}

			if (ResultCode == Client.ResultCode.UDF_BAD_RESPONSE)
			{
                Record r = ParseRecord();
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = ResultCode;
					record.inDoubt = Command.BatchInDoubt(record.hasWrite, 0);
                    Status.SetRowError();
					return true;
				}
			}

			record.SetError(ResultCode, Command.BatchInDoubt(record.hasWrite, 0));
			Status.SetRowError();
			return true;
		}

		protected internal override void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt)
			{
				return;
			}

			foreach (int index in Batch.offsets)
			{
				BatchRecord record = Records[index];

				if (record.resultCode == Client.ResultCode.NO_RESPONSE)
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
		private Operation[] Ops { get; }
		private BatchAttr Attr { get; }

		public BatchOperateArrayCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Operation[] ops,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) : base(buffer, channel, batch, batchPolicy, records, status, ops != null)
		{
			this.Ops = ops;
			this.Attr = attr;
		}

		protected internal override bool IsWrite()
		{
			return Attr.hasWrite;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(BatchPolicy, (BatchRecord[])Records, Batch, null, Ops, Attr);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(FieldCount);

			BatchRecord record = Records[BatchIndex];

			if (ResultCode == 0)
			{
				record.SetRecord(ParseRecord());
			}
			else
			{
				record.SetError(ResultCode, Command.BatchInDoubt(Attr.hasWrite, 0));
				Status.SetRowError();
			}
			return true;
		}

		protected internal override void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt || !Attr.hasWrite)
			{
				return;
			}

			foreach (int index in Batch.offsets)
			{
				BatchRecord record = Records[index];

				if (record.resultCode == Client.ResultCode.NO_RESPONSE)
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
		private Key[] Keys { get; }
		private string PackageName { get; }
		private string FunctionName { get; }
		private byte[] ArgBytes { get; }
		private BatchAttr Attr { get; }

		public BatchUDFCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) : base(buffer, channel, batch, batchPolicy, records, status, false)
		{
			this.Keys = keys;
			this.PackageName = packageName;
			this.FunctionName = functionName;
			this.ArgBytes = argBytes;
			this.Attr = attr;
		}

		protected internal override bool IsWrite()
		{
			return Attr.hasWrite;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchUDF(BatchPolicy, Keys, Batch, PackageName, FunctionName, ArgBytes, Attr);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(FieldCount);

			BatchRecord record = Records[BatchIndex];

			if (ResultCode == 0)
			{
				record.SetRecord(ParseRecord());
				return true;
			}

			if (ResultCode == Client.ResultCode.UDF_BAD_RESPONSE)
			{
				Record r = ParseRecord();
				string m = r.GetString("FAILURE");

				if (m != null)
				{
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = ResultCode;
					record.inDoubt = Command.BatchInDoubt(Attr.hasWrite, 0);
					Status.SetRowError();
					return true;
				}
			}

			record.SetError(ResultCode, Command.BatchInDoubt(Attr.hasWrite, 0));
			Status.SetRowError();
			return true;
		}

		protected internal override void SetInDoubt(bool inDoubt)
		{
			if (!inDoubt || !Attr.hasWrite)
			{
				return;
			}

			foreach (int index in Batch.offsets)
			{
				BatchRecord record = Records[index];

				if (record.resultCode == Client.ResultCode.NO_RESPONSE)
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
		protected BatchNode Batch { get; }
		protected BatchPolicy BatchPolicy { get; }
		protected BatchStatus Status { get; }

		protected IList<BatchRecord> Records { get; }

		public BatchCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			BatchNode batch,
			BatchPolicy batchPolicy,
			IList<BatchRecord> records,
			BatchStatus status,
			bool isOperation
		) : base(buffer, channel, batchPolicy, isOperation)
		{
			this.Batch = batch;
			this.BatchPolicy = batchPolicy;
			this.Records = records;
			this.Status = status;
		}

		protected internal virtual void SetInDoubt(bool inDoubt)
		{
			// Do nothing by default. Batch writes will override this method.
		}

		public void Execute()
		{
			CancellationTokenSource source = new();
			Execute(source.Token).Wait(totalTimeout);
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
			GRPCConversions.SetRequestPolicy(BatchPolicy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(Channel);
				var deadline = GetDeadline();
				var stream = client.BatchOperate(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxyStream(stream);
				await ParseResult(conn, token);
			}
			catch (EndOfGRPCStream eos)
			{
				// continue
				if (eos.ResultCode != 0)
				{
					// The server returned a fatal error.
					throw new AerospikeException(ResultCode);
				}
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}

			Status.CheckException();
		}
	}
}
