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
using Neo.IronLua;

namespace Aerospike.Client
{
	public class ReadCommandProxy : GRPCCommand
	{
		private string[] BinNames { get; }
		public Record Record { get; private set; }

		public ReadCommandProxy(Buffer buffer, CallInvoker invoker, Policy policy, Key key)
			: base(buffer, invoker, policy, key)
		{
			this.BinNames = null;
		}

		public ReadCommandProxy(Buffer buffer, CallInvoker invoker, Policy policy, Key key, String[] binNames)
			: base(buffer, invoker, policy, key)
		{
			this.BinNames = binNames;
		}

		public ReadCommandProxy(Buffer buffer, CallInvoker invoker, Policy policy, Key key, bool isOperation)
			: base(buffer, invoker, policy, key, isOperation)
		{
			this.BinNames = null;
		}

		protected internal override void WriteBuffer()
		{
			SetRead(Policy, Key, BinNames);
		}

		protected internal override bool ParseRow()
		{
			throw new AerospikeException(NotSupported + "ParseRow");
		}

		protected internal override void ParseResult(IConnection conn)
		{
			// Read header.		
			conn.ReadFully(Buffer.DataBuffer, 8);

			long sz = ByteUtil.BytesToLong(Buffer.DataBuffer, 0);
			int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

			if (receiveSize <= 0)
			{
				throw new AerospikeException("Invalid receive size: " + receiveSize);
			}

			SizeBuffer(receiveSize);
			conn.ReadFully(Buffer.DataBuffer, receiveSize);
			conn.UpdateLastUsed();

			ulong type = (ulong)((sz >> 48) & 0xff);

			if (type == Command.AS_MSG_TYPE)
			{
				Buffer.Offset = 5;
			}
			else if (type == Command.MSG_TYPE_COMPRESSED)
			{
				int usize = (int)ByteUtil.BytesToLong(Buffer.DataBuffer, 0);
				byte[] ubuf = new byte[usize];

				ByteUtil.Decompress(Buffer.DataBuffer, 8, receiveSize, ubuf, usize);
				Buffer.DataBuffer = ubuf;
				Buffer.Offset = 13;
			}
			else
			{
				throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
			}

			int resultCode = Buffer.DataBuffer[Buffer.Offset];
			Buffer.Offset++;
			int generation = ByteUtil.BytesToInt(Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			int expiration = ByteUtil.BytesToInt(Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 8;
			int fieldCount = ByteUtil.BytesToShort(Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 2;
			int opCount = ByteUtil.BytesToShort(Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 2;

			if (resultCode == 0)
			{
				if (opCount == 0)
				{
					// Bin data was not returned.
					Record = new Record(null, generation, expiration);
					return;
				}
				SkipKey(fieldCount);
				Record = Policy.recordParser.ParseRecord(Buffer.DataBuffer, ref Buffer.Offset, opCount, generation, expiration, IsOperation);
				return;
			}

			if (resultCode == Client.ResultCode.KEY_NOT_FOUND_ERROR)
			{
                HandleNotFound(resultCode);
				return;
			}

			if (resultCode == Client.ResultCode.FILTERED_OUT)
			{
				if (Policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			if (resultCode == Client.ResultCode.UDF_BAD_RESPONSE)
			{
				base.SkipKey(fieldCount);
                Record = Policy.recordParser.ParseRecord(Buffer.DataBuffer, ref Buffer.Offset, opCount, generation, expiration, IsOperation);
                HandleUdfError(resultCode);
				return;
			}

			throw new AerospikeException(resultCode);
		}

		protected internal virtual void HandleNotFound(int resultCode)
		{
			// Do nothing in default case. Record will be null.
		}

		private void HandleUdfError(int resultCode)
		{
			if (!Record.bins.TryGetValue("FAILURE", out object obj))
			{
				throw new AerospikeException(resultCode);
			}

			string ret = (string)obj;
			string message;
			int code;

			try
			{
				string[] list = ret.Split(':');
				code = Convert.ToInt32(list[2].Trim());
				message = list[0] + ':' + list[1] + ' ' + list[3];
			}
			catch (Exception)
			{
				// Use generic exception if parse error occurs.
				throw new AerospikeException(resultCode, ret);
			}

			throw new AerospikeException(code, message);
		}

		public virtual void Execute()
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset)
			};
			GRPCConversions.SetRequestPolicy(Policy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(CallInvoker);
				var deadline = GetDeadline();
                var response = client.Read(request, deadline: deadline);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}

		public virtual async Task<Record> Execute(CancellationToken token)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset)
			};
			GRPCConversions.SetRequestPolicy(Policy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(CallInvoker);
				var deadline = GetDeadline();

                if (Log.DebugEnabled())
                    Log.Debug($"Execute Read: '{this.Key}': '{deadline}': {token.IsCancellationRequested}");

                var response = await client.ReadAsync(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);

                if (Log.DebugEnabled())
                    Log.Debug($"Execute Read Completed: '{this.Record}'");

                return Record;
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}
	}
}
