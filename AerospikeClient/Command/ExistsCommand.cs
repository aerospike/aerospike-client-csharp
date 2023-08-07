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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class ExistsCommand : SyncCommand
	{
		private readonly Key key;
		private readonly Partition partition;
		private bool exists;

		public ExistsCommand(Cluster cluster, Policy policy, Key key)
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
			SetExists(policy, key);
		}

		protected internal override void ParseResult(IConnection conn)
		{
			// Read header.
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);
			conn.UpdateLastUsed();

			int resultCode = dataBuffer[13];

			if (resultCode == 0)
			{
				exists = true;
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				exists = false;
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				exists = true;
				return;
			}

			throw new AerospikeException(resultCode);
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryRead(timeout);
			return true;
		}

		public bool Exists()
		{
			return exists;
		}

		public void ExecuteGRPC(GrpcChannel channel)
		{
			WriteBuffer();
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
			};
			GRPCConversions.SetRequestPolicy(policy, request);

			var KVS = new KVS.KVS.KVSClient(channel);
			var response = KVS.Exists(request);
			var conn = new ConnectionProxy(response);
			ParseResult(conn);
		}
	}
}
