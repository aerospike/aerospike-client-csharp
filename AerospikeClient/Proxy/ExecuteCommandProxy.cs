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

namespace Aerospike.Client
{
	public sealed class ExecuteCommandProxy : ReadCommandProxy
	{
		private readonly WritePolicy writePolicy;
		private readonly string packageName;
		private readonly string functionName;
		private readonly Value[] args;

		public ExecuteCommandProxy
		(
			Buffer buffer,
			CallInvoker invoker,
			WritePolicy writePolicy,
			Key key,
			string packageName,
			string functionName,
			Value[] args
		) : base(buffer, invoker, writePolicy, key)
		{
			this.writePolicy = writePolicy;
			this.packageName = packageName;
			this.functionName = functionName;
			this.args = args;
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(writePolicy, key, packageName, functionName, args);
		}

		protected internal override void HandleNotFound(int resultCode)
		{
			throw new AerospikeException(resultCode);
		}

		protected internal override bool ParseRow()
		{
			throw new AerospikeException(NotSupported + "ParseRow");
		}

		public override void Execute()
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset)
			};
			GRPCConversions.SetRequestPolicy(writePolicy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(CallInvoker);
				var deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = client.Execute(request, deadline: deadline);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}
	}
}
