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
using Grpc.Net.Client;
using System;
using System.Threading.Tasks;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	public abstract class QueryPartitionCommandProxy : MultiCommand
	{
		private QueryPolicy policy;
		private WritePolicy writePolicy;
		private Statement statement;
		private PartitionFilter partitionFilter;
		private Operation[] operations;

		public QueryPartitionCommandProxy
		(
			QueryPolicy policy,
			WritePolicy writePolicy,
			Statement statement,
			Operation[] operations,
			PartitionTracker partitionTracker,
			PartitionFilter partitionFilter,
			RecordSet recordset
		) : base(null, policy, null, true)
		{
			this.policy = policy;
			this.writePolicy = writePolicy;
			this.statement = statement;
			this.operations = operations;
			this.partitionFilter = partitionFilter;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(null, policy, statement, statement.taskId, false, null);
		}

		public void ExecuteGRPC(GrpcChannel channel)
		{
			WriteBuffer();
			var queryRequest = new QueryRequest
			{
				Statement = GRPCConversions.ToGrpc(statement, (long)statement.taskId, statement.maxRecords),
				PartitionFilter = GRPCConversions.ToGrpc(partitionFilter),
				QueryPolicy = GRPCConversions.ToGrpc(policy)
			};
			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(dataBuffer),
				QueryRequest = queryRequest
			};

			var KVS = new KVS.Query.QueryClient(channel);
			var stream = KVS.Query(request);//, cancellationToken: token);
			var conn = new ConnectionProxyStream(stream);

			try
			{
				ParseResult(conn);
			}
			catch (EndOfGRPCStream eogs)
			{
				// continue
			}
			catch (Exception e)
			{

			}
		}
	}
}
