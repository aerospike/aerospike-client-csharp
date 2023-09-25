/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class OperateCommand : ReadCommand
	{
		private readonly OperateArgs args;

		public OperateCommand(Cluster cluster, Key key, OperateArgs args) 
			: base(cluster, args.writePolicy, key, args.GetPartition(cluster, key), true)
		{
			this.args = args;
		}

		protected internal override bool IsWrite()
		{
			return args.hasWrite;
		}

		protected internal override Node GetNode()
		{
			return args.hasWrite ? partition.GetNodeWrite(cluster) : partition.GetNodeRead(cluster);
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(args.writePolicy, key, args);
		}

		protected internal override void HandleNotFound(int resultCode)
		{
			// Only throw not found exception for command with write operations.
			// Read-only command operations return a null record.
			if (args.hasWrite)
			{
				throw new AerospikeException(resultCode);
			}
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			if (args.hasWrite)
			{
				partition.PrepareRetryWrite(timeout);
			}
			else
			{
				partition.PrepareRetryRead(timeout);
			}
			return true;
		}

		public override void ExecuteGRPC(CallInvoker callInvoker)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset)
			};

			try 
			{ 
				var client = new KVS.KVS.KVSClient(callInvoker);
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = client.Operate(request, deadline: deadline);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}

		public override async Task<Record> ExecuteGRPC(CallInvoker callInvoker, CancellationToken token)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset)
			};

			try
			{
				var client = new KVS.KVS.KVSClient(callInvoker);
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = await client.OperateAsync(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
				return Record;
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}
	}
}
