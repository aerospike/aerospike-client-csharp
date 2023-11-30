/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	public sealed class ScanPartitionCommandProxy : GRPCCommand
	{
		private ScanPolicy ScanPolicy { get; }
		private string SetName { get; }
		private string[] BinNames { get; }
		private PartitionTracker Tracker { get; }
		private PartitionFilter PartitionFilter { get; }
		private RecordSet RecordSet { get; }

		public ScanPartitionCommandProxy
		(
			Buffer buffer,
			GrpcChannel channel,
			ScanPolicy scanPolicy,
			string ns,
			string setName,
			string[] binNames,
			PartitionTracker tracker,
			PartitionFilter filter,
			RecordSet recordSet
		) : base(buffer, channel, scanPolicy, tracker.socketTimeout, tracker.totalTimeout)
		{
			this.ScanPolicy = scanPolicy;
			this.SetName = setName;
			this.BinNames = binNames;
			this.Tracker = tracker;
			this.PartitionFilter = filter;
			this.RecordSet = recordSet;
			this.Ns = ns;
		}

		protected internal override void WriteBuffer()
		{
			SetScan(ScanPolicy, Ns, SetName, BinNames, RandomShift.ThreadLocalInstance.NextLong());
		}

		protected internal override bool ParseRow()
		{
			Key key = ParseKey(FieldCount, out _);

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
				throw new AerospikeException.ScanTerminated();
			}

			if (!RecordSet.Put(new KeyRecord(key, record)))
			{
				Stop();
				throw new AerospikeException.QueryTerminated();
			}

			Tracker.SetDigest(null, key);
			return true;
		}

		public void Execute()
		{
			CancellationTokenSource source = new();
			Execute(source.Token).Wait(totalTimeout);
		}

		public async Task Execute(CancellationToken token)
		{
			WriteBuffer();
			var scanRequest = new ScanRequest
			{
				Namespace = Ns,
				SetName = SetName,
				PartitionFilter = GRPCConversions.ToGrpc(PartitionFilter),
				ScanPolicy = GRPCConversions.ToGrpc(ScanPolicy)
			};
			if (BinNames != null)
			{
				foreach (string binName in BinNames)
				{
					scanRequest.BinNames.Add(binName);
				}
			}

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(Buffer.DataBuffer, 0, Buffer.Offset),
				ScanRequest = scanRequest
			};

			try
			{
				var client = new Scan.ScanClient(Channel);
				var deadline = GetDeadline();
				var stream = client.Scan(request, deadline: deadline, cancellationToken: token);
				var conn = new ConnectionProxyStream(stream);
				await ParseResult(conn, token);
			}
			catch (EndOfGRPCStream eos)
			{
				RecordSet.Put(RecordSet.END);
				if (eos.ResultCode != 0)
				{
					// The server returned a fatal error.
					throw new AerospikeException(eos.ResultCode);
				}
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, totalTimeout, IsWrite());
			}
		}
	}
}
