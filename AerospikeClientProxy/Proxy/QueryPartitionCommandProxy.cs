/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using Aerospike.Client.Proxy.KVS;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client.Proxy
{
	public class QueryPartitionCommandProxy : GRPCCommand
	{
		private new QueryPolicy Policy { get; }
		private Statement Statement { get; }
		private PartitionFilter PartitionFilter { get; }
		private PartitionTracker Tracker { get; }
		private RecordSet RecordSet { get; }

		public QueryPartitionCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			QueryPolicy policy,
			Statement statement,
			PartitionTracker partitionTracker,
			PartitionFilter partitionFilter,
			RecordSet recordSet
		) : base(buffer, channel, policy, true)
		{
			this.Policy = policy;
			this.Statement = statement;
			this.PartitionFilter = partitionFilter;
			this.Tracker = partitionTracker;
			this.RecordSet = recordSet;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(Policy, Statement, Statement.taskId, false);
		}

		protected internal override bool ParseRow()
		{
			Key key = ParseKey(FieldCount, out ulong bval);

			if ((Info3 & Command.INFO3_PARTITION_DONE) != 0)
			{
				// When an error code is received, mark partition as unavailable
				// for the current round. Unavailable partitions will be retried
				// in the next round. Generation is overloaded as partitionId.
				if (ResultCode != 0)
				{
					Tracker.PartitionUnavailable(null, Generation);
				}
				return true;
			}

			if (ResultCode != 0)
			{
				throw new AerospikeException(ResultCode);
			}

			Record record = ParseRecord();

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}

			if (!RecordSet.Put(new KeyRecord(key, record)))
			{
				Stop();
				throw new AerospikeException.QueryTerminated();
			}

			Tracker.SetLast(null, key, bval);
			return true;
		}

		public async Task Execute(CancellationToken token)
		{
			WriteBuffer();
			var queryRequest = new QueryRequest
			{
				Statement = GRPCConversions.ToGrpc(Statement, (long)Statement.taskId, Statement.maxRecords),
				PartitionFilter = GRPCConversions.ToGrpc(PartitionFilter),
				QueryPolicy = GRPCConversions.ToGrpc(Policy)
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
				var client = new KVS.Query.QueryClient(Channel);
				var deadline = GetDeadline();

				if (Log.DebugEnabled())
				{
					Log.Debug($"Execute Query: '{request.QueryRequest.Statement}': '{deadline}': {token.IsCancellationRequested}");
				}

				var stream = client.Query(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxyStream(stream);
				await ParseResult(conn, token);
			}
			catch (EndOfGRPCStream eos)
			{
				RecordSet.Put(RecordSet.END);
				if (eos.ResultCode != 0)
				{
					if (Log.DebugEnabled())
					{
						Log.Debug($"QueryParitionCommandProxy EndOfGRPCStream Exception: {eos.ResultCode}: Exception: {eos.GetType()} Message: '{eos.Message}': '{eos}'");
					}

					// The server returned a fatal error.
					throw new AerospikeException(eos.ResultCode);
				}

				if (Log.DebugEnabled())
				{
					Log.Debug($"Execute Query: Result Code: {eos.ResultCode}: Completed: '{this.OpCount}'");
				}
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}
	}
}
