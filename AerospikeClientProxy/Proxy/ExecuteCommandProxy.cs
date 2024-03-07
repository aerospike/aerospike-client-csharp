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
	public sealed class ExecuteCommandProxy : ReadCommandProxy
	{
		private WritePolicy WritePolicy { get; }
		private string PackageName { get; }
		private string FunctionName { get; }
		private Value[] Args { get; }

		public ExecuteCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			WritePolicy writePolicy,
			Key key,
			string packageName,
			string functionName,
			Value[] args
		) : base(buffer, channel, writePolicy, key)
		{
			this.WritePolicy = writePolicy;
			this.PackageName = packageName;
			this.FunctionName = functionName;
			this.Args = args;
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override void WriteBuffer()
		{
			SetUdf(WritePolicy, Key, PackageName, FunctionName, Args);
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
			GRPCConversions.SetRequestPolicy(WritePolicy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(Channel);
				var deadline = GetDeadline();
				var response = client.Execute(request, deadline: deadline);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}
	}
}
