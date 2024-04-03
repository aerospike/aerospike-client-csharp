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

namespace Aerospike.Client
{
	public sealed class AsyncReadHeader : AsyncSingleCommand
	{
		private readonly RecordListener listener;
		private readonly Key key;
		private readonly Partition partition;
		private Record record;

		public AsyncReadHeader(AsyncCluster cluster, Policy policy, RecordListener listener, Key key) 
			: base(cluster, policy)
		{
			this.listener = listener;
			this.key = key;
			this.partition = Partition.Read(cluster, policy, key);
		}

		public AsyncReadHeader(AsyncReadHeader other)
			: base(other)
		{
			this.listener = other.listener;
			this.key = other.key;
			this.partition = other.partition;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncReadHeader(this);
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return partition.GetNodeRead(cluster);
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.READ;
		}

		protected internal override void WriteBuffer()
		{
			SetReadHeader(policy, key);
		}

		protected internal override void ParseResult()
		{
			int resultCode = dataBuffer[dataOffset + 5];

			if (resultCode == 0)
			{
				int generation = ByteUtil.BytesToInt(dataBuffer, dataOffset + 6);
				int expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset + 10);

				record = new Record(null, generation, expiration);
				return;
			}

			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);			
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryRead(timeout);
			return true;
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(key, record);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}
