/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
		// Pool used in asynchronous SocketChannel communications.
		private readonly AsyncCommandQueueBase commandQueue;

		// Contiguous pool of byte buffers.
		private BufferPool bufferPool;

		// Maximum number of concurrent asynchronous commands.
		internal readonly int maxCommands;

		// Minimum async connections per node.
		internal readonly int asyncMinConnsPerNode;

		// Maximum async connections per node.
		internal readonly int asyncMaxConnsPerNode;

		public AsyncCluster(AsyncClientPolicy policy, Host[] hosts)
			: base(policy, hosts)
		{
			maxCommands = policy.asyncMaxCommands;
			asyncMinConnsPerNode = policy.asyncMinConnsPerNode;
			asyncMaxConnsPerNode = (policy.asyncMaxConnsPerNode >= 0) ? policy.asyncMaxConnsPerNode : policy.maxConnsPerNode;

			if (asyncMinConnsPerNode > asyncMaxConnsPerNode)
			{
				throw new AerospikeException("Invalid async connection range: " + asyncMinConnsPerNode + " - " + asyncMaxConnsPerNode);
			}

			switch (policy.asyncMaxCommandAction)
			{
				case MaxCommandAction.REJECT: commandQueue = new AsyncCommandRejectingQueue(); break;
				case MaxCommandAction.BLOCK: commandQueue = new AsyncCommandBlockingQueue(); break;
				case MaxCommandAction.DELAY: commandQueue = new AsyncCommandDelayingQueue(policy); break;
				default: throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Unsupported MaxCommandAction value: " + policy.asyncMaxCommandAction.ToString() + ".");
			}

			for (int i = 0; i < maxCommands; i++)
			{
				SocketAsyncEventArgs eventArgs = new SocketAsyncEventArgs();
				eventArgs.UserToken = new BufferSegment();
				eventArgs.Completed += AsyncCommand.SocketListener;
				commandQueue.ReleaseArgs(eventArgs);
			}

			bufferPool = new BufferPool();
			InitTendThread(policy.failIfNotConnected);
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
			commandQueue.ScheduleCommand(command);
		}

		public void PutEventArgs(SocketAsyncEventArgs args)
		{
			commandQueue.ReleaseArgs(args);
		}

		public void GetNextBuffer(int size, BufferSegment segment)
		{
			lock (this)
			{
				if (size > bufferPool.bufferSize)
				{
					bufferPool = new BufferPool(maxCommands, size);
				}
				bufferPool.GetNextBuffer(segment);
			}
		}

		public bool HasBufferChanged(BufferSegment segment)
		{
			return bufferPool.bufferSize != segment.size;
		}
	}
}
