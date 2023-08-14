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
using Grpc.Net.Client;

namespace Aerospike.Client
{
	public sealed class AsyncDelete : AsyncSingleCommand
	{
		private readonly WritePolicy writePolicy;
		private readonly DeleteListener listener;
		private readonly Key key;
		private readonly Partition partition;
		private bool existed;

		public AsyncDelete(AsyncCluster cluster, WritePolicy writePolicy, Key key, DeleteListener listener)
			: base(cluster, writePolicy)
		{
			this.writePolicy = writePolicy;
			this.listener = listener;
			this.key = key;
			this.partition = Partition.Write(cluster, policy, key);
		}

		public AsyncDelete(AsyncDelete other)
			: base(other)
		{
			this.writePolicy = other.writePolicy;
			this.listener = other.listener;
			this.key = other.key;
			this.partition = other.partition;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncDelete(this);
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
			SetDelete(writePolicy, key);
		}

		protected internal override void ParseResult()
		{
			int resultCode = dataBuffer[dataOffset + 5];

			if (resultCode == 0)
			{
				existed = true;
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				existed = false;
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				existed = true;
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
				listener.OnSuccess(key, existed);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}

		public async Task<bool> ExecuteGRPC(GrpcChannel channel, CancellationToken token)
		{
			segment = new BufferSegment(new BufferPool(1, 128 * 1024), 0);
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset)
			};
			GRPCConversions.SetRequestPolicy(writePolicy, request);

			var KVS = new KVS.KVS.KVSClient(channel);
			var response = await KVS.DeleteAsync(request, cancellationToken: token);
			conn = new AsyncConnectionProxy(response.Payload.ToByteArray());
			ParseResult();
			return existed;
		}
	}
}
