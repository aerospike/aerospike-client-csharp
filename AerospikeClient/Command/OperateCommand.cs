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
	public sealed class OperateCommand : ReadCommand
	{
		private readonly OperateArgs args;

		public OperateCommand(Cluster cluster, Key key, OperateArgs args)
			: base(cluster, args.writePolicy, key, args.GetPartition(cluster, key), true)
		{
			this.args = args;
		}

		protected internal override bool IsWrite()
		{
			return args.hasWrite;
		}

		protected internal override Node GetNode()
		{
			return args.hasWrite ? partition.GetNodeWrite(cluster) : partition.GetNodeRead(cluster);
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return args.hasWrite ? Latency.LatencyType.WRITE : Latency.LatencyType.READ;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(args.writePolicy, key, args);
		}

		protected internal override void HandleNotFound(int resultCode)
		{
			// Only throw not found exception for command with write operations.
			// Read-only command operations return a null record.
			if (args.hasWrite)
			{
				throw new AerospikeException(resultCode);
			}
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			if (args.hasWrite)
			{
				partition.PrepareRetryWrite(timeout);
			}
			else
			{
				partition.PrepareRetryRead(timeout);
			}
			return true;
		}
	}
}
