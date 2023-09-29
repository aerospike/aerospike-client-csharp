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
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Threading.Tasks;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	public class QueryPartitionCommandProxy : GRPCCommand
	{
		private new readonly QueryPolicy policy;
		private readonly Statement statement;
		private readonly PartitionFilter partitionFilter;
		private readonly PartitionTracker tracker;
		private readonly RecordSet recordSet;

		public QueryPartitionCommandProxy
		(
			Buffer buffer, 
			CallInvoker invoker,
			QueryPolicy policy,
			Statement statement,
			PartitionTracker partitionTracker,
			PartitionFilter partitionFilter,
			RecordSet recordSet
		) : base(buffer, invoker, policy, true)
		{
			this.policy = policy;
			this.statement = statement;
			this.partitionFilter = partitionFilter;
			this.tracker = partitionTracker;
			this.recordSet = recordSet;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(policy, statement, statement.taskId, false);
		}

		protected internal override bool ParseRow()
		{
			Key key = ParseKey(fieldCount, out ulong bval);

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

		public void Execute()
		{
			CancellationTokenSource source = new();
			Execute(source.Token).Wait();
		}

		public async Task Execute(CancellationToken token)
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
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset),
				QueryRequest = queryRequest
			};

			try
			{ 
				var client = new KVS.Query.QueryClient(CallInvoker);
				var deadline = DateTime.UtcNow.AddMilliseconds(totalTimeout);
				var stream = client.Query(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxyStream(stream);
				await ParseResult(conn, token);
			}
			catch (EndOfGRPCStream)
			{
				recordSet.Put(RecordSet.END);
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, true);
			}
		}
	}
}
