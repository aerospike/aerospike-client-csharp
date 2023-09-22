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
using Google.Protobuf;
using Grpc.Net.Client;
using Aerospike.Client.KVS;
using System.Diagnostics;
using Grpc.Core;

namespace Aerospike.Client
{
	public sealed class WriteCommand : SyncCommand
	{
		private readonly WritePolicy writePolicy;
		private readonly Key key;
		private readonly Partition partition;
		private readonly Bin[] bins;
		private readonly Operation.Type operation;

		public WriteCommand(Cluster cluster, WritePolicy writePolicy, Key key, Bin[] bins, Operation.Type operation)
			: base(cluster, writePolicy)
		{
			this.writePolicy = writePolicy;
			this.key = key;
			this.partition = Partition.Write(cluster, writePolicy, key);
			this.bins = bins;
			this.operation = operation;
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override Node GetNode()
		{
			return partition.GetNodeWrite(cluster);
		}

		protected internal override void WriteBuffer(byte[] buffer)
		{
			SetWrite(writePolicy, operation, key, bins);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			// Read header.		
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);
			conn.UpdateLastUsed();

			int resultCode = dataBuffer[13];

			if (resultCode == 0)
			{
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (writePolicy.failOnFilteredOut)
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

		public void ExecuteGRPC(GrpcChannel channel)
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
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = client.Write(request, deadline: deadline);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
			}
			catch (RpcException e) 
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
			
		}

		public async Task ExecuteGRPC(GrpcChannel channel, byte[] buffer, CancellationToken token)
		{
			//Debugger.Launch();
			WriteBuffer(buffer);
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
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = await client.WriteAsync(request, cancellationToken: token, deadline: deadline);
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
