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
	internal class ReadCommandNew : ICommand
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

		protected readonly Key key;
		protected readonly Partition partition;
		private readonly string[] binNames;
		private readonly bool isOperation;
		public Record Record { get; private set; }

		public ReadCommandNew(ArrayPool<byte> bufferPool, Cluster cluster, Policy policy, Key key)
		{
			this.SetCommonProperties(bufferPool, cluster, policy);
			this.key = key;
			this.binNames = null;
			this.partition = Partition.Read(cluster, policy, key);
			this.isOperation = false;
			cluster.AddTran();
		}

		public ReadCommandNew(ArrayPool<byte> bufferPool, Cluster cluster, Policy policy, Key key, String[] binNames)
		{
			this.SetCommonProperties(bufferPool, cluster, policy);
			this.key = key;
			this.binNames = binNames;
			this.partition = Partition.Read(cluster, policy, key);
			this.isOperation = false;
			cluster.AddTran();
		}

		public ReadCommandNew(ArrayPool<byte> bufferPool, Cluster cluster, Policy policy, Key key, Partition partition, bool isOperation)
		{
			this.SetCommonProperties(bufferPool, cluster, policy);
			this.key = key;
			this.binNames = null;
			this.partition = partition;
			this.isOperation = isOperation;
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
			this.SetRead(Policy, key, binNames);
		}

		public async Task ParseResult(IConnection conn, CancellationToken token)
		{
			token.ThrowIfCancellationRequested();

			// Read header.		
			await conn.ReadFully(DataBuffer, 8, token);

			long sz = ByteUtil.BytesToLong(DataBuffer, 0);
			int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

			if (receiveSize <= 0)
			{
				throw new AerospikeException("Invalid receive size: " + receiveSize);
			}

			this.SizeBuffer(receiveSize);
			await conn.ReadFully(DataBuffer, receiveSize, token);
			conn.UpdateLastUsed();

			ulong type = (ulong)((sz >> 48) & 0xff);

			if (type == Command.AS_MSG_TYPE)
			{
				DataOffset = 5;
			}
			else if (type == Command.MSG_TYPE_COMPRESSED)
			{
				int usize = (int)ByteUtil.BytesToLong(DataBuffer, 0);
				byte[] ubuf = new byte[usize];

				ByteUtil.Decompress(DataBuffer, 8, receiveSize, ubuf, usize);
				DataBuffer = ubuf;
				DataOffset = 13;
			}
			else
			{
				throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
			}

			int resultCode = DataBuffer[DataOffset];
			DataOffset++;
			int generation = ByteUtil.BytesToInt(DataBuffer, DataOffset);
			DataOffset += 4;
			int expiration = ByteUtil.BytesToInt(DataBuffer, DataOffset);
			DataOffset += 8;
			int fieldCount = ByteUtil.BytesToShort(DataBuffer, DataOffset);
			DataOffset += 2;
			int opCount = ByteUtil.BytesToShort(DataBuffer, DataOffset);
			DataOffset += 2;

			if (resultCode == 0)
			{
				if (opCount == 0)
				{
					// Bin data was not returned.
					Record = new Record(null, generation, expiration);
					return;
				}
				this.SkipKey(fieldCount);
				(Record, DataOffset) = Policy.recordParser.ParseRecord(DataBuffer, DataOffset, opCount, generation, expiration, isOperation);
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				HandleNotFound(resultCode);
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

			if (resultCode == ResultCode.UDF_BAD_RESPONSE)
			{
				this.SkipKey(fieldCount);
				(Record, DataOffset) = Policy.recordParser.ParseRecord(DataBuffer, DataOffset, opCount, generation, expiration, isOperation);
				HandleUdfError(resultCode);
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

		public void HandleNotFound(int resultCode)
		{
			// Do nothing in default case. Record will be null.
		}

		private void HandleUdfError(int resultCode)
		{
			object obj;

			if (!Record.bins.TryGetValue("FAILURE", out obj))
			{
				throw new AerospikeException(resultCode);
			}

			string ret = (string)obj;
			string message;
			int code;

			try
			{
				string[] list = ret.Split(':');
				code = Convert.ToInt32(list[2].Trim());
				message = list[0] + ':' + list[1] + ' ' + list[3];
			}
			catch (Exception e)
			{
				// Use generic exception if parse error occurs.
				throw new AerospikeException(resultCode, ret, e);
			}

			throw new AerospikeException(code, message);
		}
	}
}
