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
	// OperateList
	//-------------------------------------------------------

	public sealed class BatchOperateListCommandProxy : BatchCommandProxy
	{
		private IList<BatchRecord> Records { get; }

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
			this.Records = records;
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
		private Key[] Keys { get; }
		private Operation[] Ops { get; }
		private BatchRecord[] Records { get; }
		private BatchAttr Attr { get; }

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
			this.Keys = keys;
			this.Ops = ops;
			this.Records = records;
			this.Attr = attr;
		}

		protected internal override bool IsWrite()
		{
			return Attr.hasWrite;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchOperate(BatchPolicy, Keys, Batch, null, Ops, Attr);
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
	// Batch Base Command
	//-------------------------------------------------------

	public abstract class BatchCommandProxy : GRPCCommand
	{
		internal BatchNode Batch { get; }
		internal BatchPolicy BatchPolicy { get; }
		internal BatchStatus Status { get; }

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
			this.Batch = batch;
			this.BatchPolicy = batchPolicy;
			this.Status = status;
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
			GRPCConversions.SetRequestPolicy(BatchPolicy, request);

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
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}
	}
}
