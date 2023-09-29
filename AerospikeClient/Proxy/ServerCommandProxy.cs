/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	public sealed class ServerCommandProxy : GRPCCommand
	{
		private readonly Statement statement;
		private readonly ulong taskId;

		public ServerCommandProxy(Buffer buffer, CallInvoker invoker, WritePolicy policy, Statement statement, ulong taskId)
			: base(buffer, invoker, policy)
		{
			this.statement = statement;
			this.taskId = taskId;
		}

		protected internal override bool IsWrite()
		{
			return true;
		}
		
		protected internal override void WriteBuffer()
		{
			SetQuery(policy, statement, taskId, true);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(fieldCount);

			// Server commands (Query/Execute UDF) should only send back a return code.
			if (resultCode != 0)
			{
				// Background scans (with null query filter) return KEY_NOT_FOUND_ERROR
				// when the set does not exist on the target node.
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					// Non-fatal error.
					return false;
				}
				throw new AerospikeException(resultCode);
			}

			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Unexpectedly received bins on background query!");
			}

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}
			return true;
		}

		public void Execute()
		{
			CancellationTokenSource source = new();
			Execute(source.Token).Wait();
		}

		public async Task Execute(CancellationToken token)
		{
			WriteBuffer();

			var execRequest = new BackgroundExecuteRequest
			{
				Statement = GRPCConversions.ToGrpc(statement, (long)taskId, statement.maxRecords),
				WritePolicy = GRPCConversions.ToGrpcExec((WritePolicy)policy)
			};
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset),
				BackgroundExecuteRequest = execRequest,
			};
			
			try
			{ 
				var client = new KVS.Query.QueryClient(CallInvoker);
				var deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var stream = client.BackgroundExecute(request, deadline: deadline, cancellationToken: token);
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
