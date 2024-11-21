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
		private readonly ExistsListener existsListener;
		private readonly Key key;
		private readonly Partition partition;
		private readonly bool throwsKeyNotFoundError;
		private bool touched;

		public AsyncTouch(AsyncCluster cluster, WritePolicy writePolicy, WriteListener listener, Key key)
			: base(cluster, writePolicy)
		{
			this.writePolicy = writePolicy;
			this.listener = listener;
			this.existsListener = null;
			this.key = key;
			this.partition = Partition.Write(cluster, policy, key);
			this.throwsKeyNotFoundError = true;
			cluster.AddTran();
		}

		public AsyncTouch(AsyncCluster cluster, WritePolicy writePolicy, ExistsListener listener, Key key, bool throwsKeyNotFoundError)
			: base(cluster, writePolicy)
		{
			this.writePolicy = writePolicy;
			this.listener = null;
			this.existsListener = listener;
			this.key = key;
			this.partition = Partition.Write(cluster, policy, key);
			this.throwsKeyNotFoundError = throwsKeyNotFoundError;
			cluster.AddTran();
		}

		public AsyncTouch(AsyncTouch other)
			: base(other)
		{
			this.writePolicy = other.writePolicy;
			this.listener = other.listener;
			this.existsListener = other.existsListener;
			this.key = other.key;
			this.partition = other.partition;
			this.throwsKeyNotFoundError = other.throwsKeyNotFoundError;
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
				touched = true;
				return;
			}

			touched = false;
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				if (throwsKeyNotFoundError)
				{
					throw new AerospikeException(resultCode);
				}
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
			else if (existsListener != null)
			{
				existsListener.OnSuccess(key, touched);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null || existsListener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}
