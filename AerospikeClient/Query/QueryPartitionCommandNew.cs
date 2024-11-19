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
using System.Xml.Linq;

namespace Aerospike.Client
{
	internal class QueryPartitionCommandNew : ICommand
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

		public int Info3 { get; set; }
		public int ResultCode { get; set; }
		public int Generation { get; set; }
		public int Expiration { get; set; }
		public int BatchIndex { get; set; }
		public int FieldCount { get; set; }
		public int OpCount { get; set; }
		public bool IsOperation { get; set; }

		public Statement Statement { get; set; }
		private readonly ulong taskId;
		//private readonly RecordSetNew recordSet;
		public PartitionTracker Tracker { get; set; }
		public NodePartitions NodePartitions { get; set; }

		public QueryPartitionCommandNew
		(
			ArrayPool<byte> bufferPool, 
			Cluster cluster, 
			Policy policy,
			Statement statement,
			ulong taskId,
			//RecordSetNew recordSet,
			PartitionTracker tracker,
			NodePartitions nodePartitions
		)
		{
			this.SetCommonProperties(bufferPool, cluster, policy);
			this.Statement = statement;
			this.taskId = taskId;
			//this.recordSet = recordSet;
			this.Tracker = tracker;
			this.NodePartitions = nodePartitions;
		}

		public Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.QUERY;
		}

		public bool PrepareRetry(bool timeout)
		{
			return false; // TODO
		}

		public bool IsWrite()
		{
			return false;
		}

		public Node GetNode()
		{
			return null; // TODO
		}

		public void WriteBuffer()
		{
			this.SetQuery(Cluster, Policy, Statement, taskId, false, NodePartitions);
		}

		public async Task ParseResult(IConnection conn, CancellationToken token)
		{
			keyRecords = CommandHelpers.ParseMultipleResult(this, conn, token);
		}

		public async IAsyncEnumerable<KeyRecord> ParseMultipleResult(IConnection conn, CancellationToken token)
		{
			return await CommandHelpers.ParseMultipleResult(this, conn, token);
		}

		public KeyRecord ParseGroup(int receiveSize)
		{
			return CommandHelpers.ParseGroup(this, receiveSize);
		}

		public KeyRecord ParseRow()
		{
			ulong bval;
			Key key = this.ParseKey(FieldCount, out bval);

			if ((Info3 & Command.INFO3_PARTITION_DONE) != 0)
			{
				// When an error code is received, mark partition as unavailable
				// for the current round. Unavailable partitions will be retried
				// in the next round. Generation is overloaded as partitionId.
				if (ResultCode != 0)
				{
					Tracker.PartitionUnavailable(NodePartitions, Generation);
				}
				return null;
			}

			if (ResultCode != 0)
			{
				throw new AerospikeException(ResultCode);
			}

			Record record = this.ParseRecord();

			KeyRecord keyRecord = null;

			if (Tracker.AllowRecord())
			{
				keyRecord = new(key, record);

				Tracker.SetLast(NodePartitions, key, bval);
			}
			return keyRecord;
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
