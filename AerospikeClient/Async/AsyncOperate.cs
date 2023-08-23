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
using Grpc.Net.Client;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncOperate : AsyncRead
	{
		private readonly OperateArgs args;

		public AsyncOperate(AsyncCluster cluster, RecordListener listener, Key key, OperateArgs args)
			: base(cluster, args.writePolicy, listener, key, args.GetPartition(cluster, key), true)
		{
			this.args = args;
		}

		public AsyncOperate(AsyncOperate other)
			: base(other)
		{
			this.args = other.args;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncOperate(this);
		}

		protected internal override bool IsWrite()
		{
			return args.hasWrite;
		}

		protected internal override Node GetNode(Cluster cluster)
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

		public override async Task<Record> ExecuteGRPC(GrpcChannel channel, CancellationToken token)
		{
			segment = new BufferSegment(new BufferPool(1, 128 * 1024), 0);
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataLength)
			};
			GRPCConversions.SetRequestPolicy(policy, request);

			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.OperateAsync(request, cancellationToken: token);
			SetupProxyConnAndBuf(response);
			ReceiveComplete();
			ParseResult();
			return record;
		}
	}
}
