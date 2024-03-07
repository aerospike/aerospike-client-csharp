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

namespace Aerospike.Client
{
	public sealed class DeleteCommandProxy : GRPCCommand
	{
		private WritePolicy WritePolicy { get; }
		public bool Existed { get; private set; }

		public DeleteCommandProxy(Buffer buffer, GrpcChannel channel, WritePolicy writePolicy, Key key)
			: base(buffer, channel, writePolicy, key)
		{
			this.WritePolicy = writePolicy;
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override void WriteBuffer()
		{
			SetDelete(WritePolicy, Key);
		}

		protected internal override bool ParseRow()
		{
			throw new AerospikeException(NotSupported + "ParseRow");
		}

		protected internal override void ParseResult(IConnection conn)
		{
			// Read header.
			conn.ReadFully(Buffer.DataBuffer, MSG_TOTAL_HEADER_SIZE);
			conn.UpdateLastUsed();

			int resultCode = Buffer.DataBuffer[13];

			if (resultCode == 0)
			{
				Existed = true;
				return;
			}

			if (Log.DebugEnabled())
			{
				Log.Debug($"DeleteCommandProxy ParseResult resultCode not OK: {resultCode}");
			}

			if (resultCode == Client.ResultCode.KEY_NOT_FOUND_ERROR)
			{
                Existed = false;
				return;
			}

			if (resultCode == Client.ResultCode.FILTERED_OUT)
			{
				if (WritePolicy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
                Existed = true;
				return;
			}

			throw new AerospikeException(resultCode);
		}

		public void Execute()
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset)
			};
			GRPCConversions.SetRequestPolicy(WritePolicy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(Channel);
				var deadline = GetDeadline();
				var response = client.Delete(request, deadline: deadline);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}

		public async Task<bool> Execute(CancellationToken token)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset)
			};
			GRPCConversions.SetRequestPolicy(WritePolicy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(Channel);
				var deadline = GetDeadline();
				var response = await client.DeleteAsync(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
				return Existed;
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}
	}
}
