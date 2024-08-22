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
	public abstract class AsyncReadBase : AsyncSingleCommand
	{
		protected internal readonly Key key;
		protected readonly Partition partition;

		public AsyncReadBase(AsyncCluster cluster, Policy policy, Key key) 
			: base(cluster, policy)
		{
			this.key = key;
			this.partition = Partition.Read(cluster, policy, key);
			cluster.AddCommand();
		}

		public AsyncReadBase(AsyncReadBase other)
			: base(other)
		{
			this.key = other.key;
			this.partition = other.partition;
		}

		protected internal override bool IsWrite()
		{
			return false;
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return partition.GetNodeRead(cluster);
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.READ;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryRead(timeout);
			return true;
		}

		protected internal abstract override void WriteBuffer();

		protected internal abstract override bool ParseResult();
	}
}
