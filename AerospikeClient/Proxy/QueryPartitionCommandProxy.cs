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
	public class QueryPartitionCommandProxy : MultiCommand
	{
		private QueryPolicy policy;
		private WritePolicy writePolicy;
		private Statement statement;
		private PartitionFilter partitionFilter;
		private Operation[] operations;
		private readonly PartitionTracker tracker;
		private readonly RecordSet recordSet;

		public QueryPartitionCommandProxy
		(
			QueryPolicy policy,
			WritePolicy writePolicy,
			Statement statement,
			Operation[] operations,
			PartitionTracker partitionTracker,
			PartitionFilter partitionFilter,
			RecordSet recordSet
		) : base(null, policy, null, true)
		{
			this.policy = policy;
			this.writePolicy = writePolicy;
			this.statement = statement;
			this.operations = operations;
			this.partitionFilter = partitionFilter;
			this.tracker = partitionTracker;
			this.recordSet = recordSet;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(null, policy, statement, statement.taskId, false, null);
		}

		protected internal override bool ParseRow()
		{
			ulong bval;
			Key key = ParseKey(fieldCount, out bval);

			if ((info3 & Command.INFO3_PARTITION_DONE) != 0)
			{
				// When an error code is received, mark partition as unavailable
				// for the current round. Unavailable partitions will be retried
				// in the next round. Generation is overloaded as partitionId.
				if (resultCode != 0)
				{
					tracker.PartitionUnavailable(null, generation);
				}
				return true;
			}

			if (resultCode != 0)
			{
				throw new AerospikeException(resultCode);
			}

			Record record = ParseRecord();

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}

			if (!recordSet.Put(new KeyRecord(key, record)))
			{
				Stop();
				throw new AerospikeException.QueryTerminated();
			}

			tracker.SetLast(null, key, bval);
			return true;
		}

		public void ExecuteGRPC(GrpcChannel channel)
		{
			CancellationToken token = new();
			ExecuteGRPC(channel, token).Wait();
		}

		public async Task ExecuteGRPC(GrpcChannel channel, CancellationToken token)
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
				Payload = ByteString.CopyFrom(dataBuffer, 0, dataOffset),
				QueryRequest = queryRequest
			};

			var KVS = new KVS.Query.QueryClient(channel);
			var stream = KVS.Query(request, cancellationToken: token);

			try
			{
				var conn = new ConnectionProxyStream(stream);
				await ParseResult(conn);
			}
			catch (EndOfGRPCStream eogs)
			{
				//if (tracker.IsComplete(cluster, policy))
				//{
				// All partitions received.
				recordSet.Put(RecordSet.END);
				//}
			}
			catch (Exception e)
			{

			}
		}
	}
}
