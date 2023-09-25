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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class ReadHeaderCommand : SyncCommand
	{
		private readonly Key key;
		private readonly Partition partition;
		private Record record;

		public ReadHeaderCommand(Cluster cluster, Policy policy, Key key)
			: base(cluster, policy)
		{
			this.key = key;
			this.partition = Partition.Read(cluster, policy, key);
		}

		protected internal override Node GetNode()
		{
			return partition.GetNodeRead(cluster);
		}

		protected internal override void WriteBuffer()
		{
			SetReadHeader(policy, key);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			// Read header.		
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);
			conn.UpdateLastUsed();

			int resultCode = dataBuffer[13];

			if (resultCode == 0)
			{
				int generation = ByteUtil.BytesToInt(dataBuffer, 14);
				int expiration = ByteUtil.BytesToInt(dataBuffer, 18);
				record = new Record(null, generation, expiration);
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
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
			partition.PrepareRetryRead(timeout);
			return true;
		}

		public Record Record
		{
			get
			{
				return record;
			}
		}

		public void ExecuteGRPC(CallInvoker callInvoker)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset)
			};
			GRPCConversions.SetRequestPolicy(policy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(callInvoker);
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = client.GetHeader(request, deadline: deadline);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}

		public async Task<Record> ExecuteGRPC(CallInvoker callInvoker, CancellationToken token)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset)
			};
			GRPCConversions.SetRequestPolicy(policy, request);

			try
			{
				var client = new KVS.KVS.KVSClient(callInvoker);
				deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var response = await client.GetHeaderAsync(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxy(response);
				ParseResult(conn);
				return record;
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}
	}
}
