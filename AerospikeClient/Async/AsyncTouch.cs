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

namespace Aerospike.Client
{
	public sealed class AsyncTouch : AsyncSingleCommand
	{
		private readonly WritePolicy writePolicy;
		private readonly WriteListener listener;
		private readonly Key key;
		private readonly Partition partition;

		public AsyncTouch(AsyncCluster cluster, WritePolicy writePolicy, WriteListener listener, Key key)
			: base(cluster, writePolicy)
		{
			this.writePolicy = writePolicy;
			this.listener = listener;
			this.key = key;
			this.partition = Partition.Write(cluster, policy, key);
			cluster.AddTran();
		}

		public AsyncTouch(AsyncTouch other)
			: base(other)
		{
			this.writePolicy = other.writePolicy;
			this.listener = other.listener;
			this.key = other.key;
			this.partition = other.partition;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncTouch(this);
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return partition.GetNodeWrite(cluster);
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.WRITE;
		}

		protected internal override void WriteBuffer()
		{
			SetTouch(writePolicy, key);
		}

		protected internal override void ParseResult()
		{
			int resultCode = dataBuffer[dataOffset + 5];

			if (resultCode == 0)
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
			partition.PrepareRetryWrite(timeout);
			return true;
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(key);
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
