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
	internal class OperateCommandNew : ReadCommandNew
	{
		private readonly OperateArgs args;

		public OperateCommandNew(ArrayPool<byte> bufferPool, Cluster cluster, Key key, OperateArgs args)
			: base(bufferPool, cluster, args.writePolicy, key, args.GetPartition(cluster, key), true)
		{
			this.args = args;
		}

		public new bool IsWrite()
		{
			return args.hasWrite;
		}

		public new Node GetNode()
		{
			return args.hasWrite ? partition.GetNodeWrite(Cluster) : partition.GetNodeRead(Cluster);
		}

		public new Latency.LatencyType GetLatencyType()
		{
			return args.hasWrite ? Latency.LatencyType.WRITE : Latency.LatencyType.READ;
		}

		public new void WriteBuffer()
		{
			this.SetOperate(args.writePolicy, key, args);
		}

		public new void HandleNotFound(int resultCode)
		{
			// Only throw not found exception for command with write operations.
			// Read-only command operations return a null record.
			if (args.hasWrite)
			{
				throw new AerospikeException(resultCode);
			}
		}

		public new bool PrepareRetry(bool timeout)
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
