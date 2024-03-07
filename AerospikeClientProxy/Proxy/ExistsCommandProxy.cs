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
	public sealed class ExistsCommandProxy : GRPCCommand
	{
		public bool Exists { get; private set; }

		public ExistsCommandProxy(Buffer buffer, GrpcChannel channel, Policy policy, Key key)
			: base(buffer, channel, policy, key)
		{
		}

		protected internal override void WriteBuffer()
		{
			SetExists(Policy, Key);
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
				Exists = true;
				return;
			}

			if (Log.DebugEnabled())
			{
				Log.Debug($"ExistsCommandProxy ParseResult resultCode not OK: {resultCode}");
			}

			if (resultCode == Client.ResultCode.KEY_NOT_FOUND_ERROR)
			{
                Exists = false;
				return;
			}

			if (resultCode == Client.ResultCode.FILTERED_OUT)
			{
				if (Policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
                Exists = true;
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
			GRPCConversions.SetRequestPolicy(Policy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(Channel);
				var deadline = GetDeadline();
				var response = client.Exists(request, deadline: deadline);
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
			GRPCConversions.SetRequestPolicy(Policy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(Channel);
				var deadline = GetDeadline();
				var response = await client.ExistsAsync(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
				return Exists;
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}
	}
}
