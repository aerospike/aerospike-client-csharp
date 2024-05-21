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

using System.Buffers;

namespace Aerospike.Client
{
	internal class ReadHeaderCommandNew : ICommand
	{
		public ArrayPool<byte> BufferPool { get; set; }
		public int ServerTimeout { get; set; }
		public int SocketTimeout { get; set; }
		public int TotalTimeout { get; set; }
		public int MaxRetries { get; set; }
		public Cluster Cluster { get; set; }
		public Policy Policy { get; set; }

		public byte[] DataBuffer { get; set; }
		public int DataOffset { get; set; }
		public int Iteration { get; set; }// 1;
		public int CommandSentCounter { get; set; }
		public DateTime Deadline { get; set; }
		
		private readonly Key key;
		private readonly Partition partition;
		public Record Record { get; private set; }

		public ReadHeaderCommandNew(ArrayPool<byte> bufferPool, Cluster cluster, Policy policy, Key key)
		{
			this.SetCommonProperties(bufferPool, cluster, policy);
			this.key = key;
			this.partition = Partition.Read(cluster, policy, key);
			cluster.AddTran();
		}

		public bool IsWrite()
		{
			return false;
		}

		public Node GetNode()
		{
			return partition.GetNodeRead(Cluster);
		}

		public Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.READ;
		}

		public void WriteBuffer()
		{
			this.SetReadHeader(Policy, key);
		}

		public async Task ParseResult(IConnection conn, CancellationToken token)
		{
			token.ThrowIfCancellationRequested();

			// Read header.		
			await conn.ReadFully(DataBuffer, CommandHelpers.MSG_TOTAL_HEADER_SIZE, token);
			conn.UpdateLastUsed();

			int resultCode = DataBuffer[13];

			if (resultCode == 0)
			{
				int generation = ByteUtil.BytesToInt(DataBuffer, 14);
				int expiration = ByteUtil.BytesToInt(DataBuffer, 18);
				Record = new Record(null, generation, expiration);
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (Policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);
		}

		public bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryRead(timeout);
			return true;
		}

		public bool RetryBatch
		(
			Cluster cluster,
			int socketTimeout,
			int totalTimeout,
			DateTime deadline,
			int iteration,
			int commandSentCounter
		)
		{
			// Override this method in batch to regenerate node assignments.
			return false;
		}
	}
}
