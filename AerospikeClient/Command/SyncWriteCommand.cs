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

using System.Runtime.InteropServices;

namespace Aerospike.Client
{
	public abstract class SyncWriteCommand : SyncCommand
	{
		protected readonly WritePolicy writePolicy;
		protected readonly Key key;
		private readonly Partition partition;

		public SyncWriteCommand(Cluster cluster, WritePolicy writePolicy, Key key)
			: base(cluster, writePolicy)
		{
			this.writePolicy = writePolicy;
			this.key = key;
			this.partition = Partition.Write(cluster, writePolicy, key);
			cluster.AddCommandCount();
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override Node GetNode()
		{
			return partition.GetNodeWrite(cluster);
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.WRITE;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			partition.PrepareRetryWrite(timeout);
			return true;
		}

		protected internal override void OnInDoubt()
		{
			if (writePolicy.Txn != null)
			{
				writePolicy.Txn.OnWriteInDoubt(key);
			}
		}

		protected internal abstract override void WriteBuffer();

		protected internal abstract override void ParseResult(IConnection conn);
	}
}
