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
	internal class DeleteCommandNew : ICommand
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

		private readonly WritePolicy writePolicy;
		private readonly Key key;
		private readonly Partition partition;
		public bool Existed { get; private set; }

		public DeleteCommandNew(ArrayPool<byte> bufferPool, Cluster cluster, WritePolicy writePolicy, Key key)
		{
			this.SetCommonProperties(bufferPool, cluster, writePolicy);
			this.writePolicy = writePolicy;
			this.key = key;
			this.partition = Partition.Write(cluster, writePolicy, key);
			cluster.AddTran();
		}

		public bool IsWrite()
		{
			return true;
		}

		public Node GetNode()
		{
			return partition.GetNodeWrite(Cluster);
		}

		public Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.WRITE;
		}

		public void WriteBuffer()
		{
			this.SetDelete(writePolicy, key);
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
				Existed = true;
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				Existed = false;
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (writePolicy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				Existed = true;
				return;
			}

			throw new AerospikeException(resultCode);
		}

		public bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryWrite(timeout);
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
