/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
	public sealed class AsyncWrite : AsyncSingleCommand
	{
		private readonly WritePolicy writePolicy;
		private readonly WriteListener listener;
		private readonly Key key;
		private readonly Partition partition;
		private readonly Bin[] bins;
		private readonly Operation.Type operation;

		public AsyncWrite
		(
			AsyncCluster cluster,
			WritePolicy writePolicy,
			WriteListener listener,
			Key key,
			Bin[] bins,
			Operation.Type operation
		) : base(cluster, writePolicy)
		{
			this.writePolicy = writePolicy;
			this.listener = listener;
			this.key = key;
			this.partition = Partition.Write(cluster, policy, key);
			this.bins = bins;
			this.operation = operation;
		}

		public AsyncWrite(AsyncWrite other)
			: base(other)
		{
			this.writePolicy = other.writePolicy;
			this.listener = other.listener;
			this.key = other.key;
			this.partition = other.partition;
			this.bins = other.bins;
			this.operation = other.operation;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncWrite(this);
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return partition.GetNodeWrite(cluster);
		}

		protected internal override void WriteBuffer()
		{
			SizeBuffer();
			WriteBuffer(dataBuffer);
		}

		internal void WriteBuffer(byte[] buffer)
		{
			SetWrite(writePolicy, operation, key, bins, buffer);
		}

		protected internal override void ParseResult()
		{
			int resultCode = dataBuffer[dataOffset + 5];

			if (resultCode == 0)
			{
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryWrite(timeout);
			return true;
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(key);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}

		public async Task ExecuteGRPC(GrpcChannel channel, CancellationToken token)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset)
			};
			GRPCConversions.SetRequestPolicy(writePolicy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(channel);
				var deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = await client.WriteAsync(request, cancellationToken: token, deadline: deadline);
				var conn = new ConnectionProxy(response);
				//ParseResult(conn);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}
	}
}
