/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System;
using System.Threading;
using System.Collections.Concurrent;

namespace Aerospike.Client
{
	public sealed class QueryAggregateCommand : MultiCommand
	{
		private readonly Statement statement;
		private readonly ulong taskId;
		private readonly BlockingCollection<object> inputQueue;
		private readonly CancellationToken cancelToken;

		public QueryAggregateCommand
		(
			Cluster cluster,
			Node node,
			QueryPolicy policy,
			Statement statement,
			ulong taskId,
			BlockingCollection<object> inputQueue,
			CancellationToken cancelToken,
			ulong clusterKey,
			bool first
		) : base(cluster, policy, node, statement.ns, clusterKey, first)
		{
			this.statement = statement;
			this.taskId = taskId;
			this.inputQueue = inputQueue;
			this.cancelToken = cancelToken;
		}

		protected internal override void WriteBuffer()
		{
			SetQuery(cluster, policy, statement, taskId, false, null);
		}

		protected internal override bool ParseRow()
		{
			SkipKey(fieldCount);

			if (resultCode != 0)
			{
				// Aggregation scans (with null query filter) will return KEY_NOT_FOUND_ERROR
				// when the set does not exist on the target node.
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					// Non-fatal error.
					return false;
				}
				throw new AerospikeException(resultCode);
			}

			if (opCount != 1)
			{
				throw new AerospikeException("Query aggregate expected exactly one bin.  Received " + opCount);
			}

			// Parse aggregateValue.
			int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
			dataOffset += 5;
			byte particleType = dataBuffer[dataOffset];
			dataOffset += 2;
			byte nameSize = dataBuffer[dataOffset++];
			string name = ByteUtil.Utf8ToString(dataBuffer, dataOffset, nameSize);
			dataOffset += nameSize;

			int particleBytesSize = (int)(opSize - (4 + nameSize));

			if (! name.Equals("SUCCESS"))
			{
				if (name.Equals("FAILURE"))
				{
					object value = ByteUtil.BytesToParticle((ParticleType)particleType, dataBuffer, dataOffset, particleBytesSize);
					throw new AerospikeException(ResultCode.QUERY_GENERIC, value.ToString());
				}
				else
				{
					throw new AerospikeException(ResultCode.PARSE_ERROR, "Query aggregate expected bin name SUCCESS.  Received " + name);
				}
			}

			object aggregateValue = LuaInstance.BytesToLua((ParticleType)particleType, dataBuffer, dataOffset, particleBytesSize);
			dataOffset += particleBytesSize;

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}

			if (aggregateValue != null)
			{
				try
				{
					inputQueue.Add(aggregateValue, cancelToken);
				}
				catch (OperationCanceledException)
				{
				}
			}
			return true;
		}
	}
}
