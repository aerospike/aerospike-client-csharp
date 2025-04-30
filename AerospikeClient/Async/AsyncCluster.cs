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
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Net.Sockets;

namespace Aerospike.Client
{
	public sealed class AsyncCluster : Cluster
	{
		// Command scheduler.
		private readonly AsyncScheduler scheduler;

		// Contiguous pool of byte buffers.
		private readonly BufferPool bufferPool;

		// Maximum number of concurrent asynchronous commands.
		internal readonly int maxCommands;

		// Minimum async connections per node.
		internal readonly int asyncMinConnsPerNode;

		// Maximum async connections per node.
		internal readonly int asyncMaxConnsPerNode;

		public AsyncCluster(AsyncClient client, AsyncClientPolicy policy, Host[] hosts)
			: base(client, policy, hosts)
		{
			maxCommands = policy.asyncMaxCommands;
			asyncMinConnsPerNode = policy.asyncMinConnsPerNode;
			asyncMaxConnsPerNode = (policy.asyncMaxConnsPerNode >= 0) ? policy.asyncMaxConnsPerNode : policy.maxConnsPerNode;

			if (asyncMinConnsPerNode > asyncMaxConnsPerNode)
			{
				throw new AerospikeException("Invalid async connection range: " + asyncMinConnsPerNode + " - " + asyncMaxConnsPerNode);
			}

			bufferPool = new BufferPool(maxCommands, policy.asyncBufferSize);

			switch (policy.asyncMaxCommandAction)
			{
				case MaxCommandAction.REJECT:
					scheduler = new RejectScheduler(policy, bufferPool);
					break;

				case MaxCommandAction.BLOCK:
					scheduler = new BlockScheduler(policy, bufferPool);
					break;

				case MaxCommandAction.DELAY:
					scheduler = new DelayScheduler(policy, bufferPool);
					break;

				default:
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Unsupported MaxCommandAction value: " + policy.asyncMaxCommandAction.ToString());
			}

			StartTendThread(policy);
		}

		protected internal override Node CreateNode(NodeValidator nv, bool createMinConn)
		{
			AsyncNode node = new AsyncNode(this, nv);

			if (createMinConn)
			{
				node.CreateMinConnections();
			}
			return node;
		}

		public void ScheduleCommandExecution(AsyncCommand command)
		{
			scheduler.Schedule(command);
		}

		public void ReleaseBuffer(BufferSegment segment)
		{
			scheduler.Release(segment);
		}
	}
}
