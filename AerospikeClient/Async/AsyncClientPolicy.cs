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
	/// <summary>
	/// Asynchronous client policy configuration.
	/// </summary>
	public sealed class AsyncClientPolicy : ClientPolicy
	{
		/// <summary>
		/// How to handle cases when the asynchronous maximum number of concurrent connections 
		/// have been reached.  
		/// </summary>
		public MaxCommandAction asyncMaxCommandAction = MaxCommandAction.BLOCK;

		/// <summary>
		/// Maximum number of concurrent asynchronous commands that can be active at any point in time.
		/// Concurrent commands can target different nodes of the Aerospike cluster. Each command will 
		/// use one concurrent connection. The number of concurrent open connections is therefore
		/// limited by:
		/// <para>
		/// max open connections = asyncMaxCommands
		/// </para>
		/// The actual number of open connections to each node of the Aerospike cluster depends on how
		/// balanced the commands are between nodes and are limited to asyncMaxConnsPerNode for any
		/// given node. For an extreme case where all commands may be destined to the same node of the
		/// cluster, asyncMaxCommands should not be set greater than asyncMaxConnsPerNode to avoid
		/// running out of connections to the node.
		/// <para>
		/// Further, this maximum number of open connections across all nodes should not exceed the
		/// total socket file descriptors available on the client machine. The socket file descriptors
		/// available can be determined by the following command:
		/// </para>
		/// <para>ulimit -n</para>
		/// <para>Default: 100</para>
		/// </summary>
		public int asyncMaxCommands = 100;

		/// <summary>
		/// Maximum number of async commands that can be stored in the delay queue when
		/// <see cref="asyncMaxCommandAction"/> is <see cref="Aerospike.Client.MaxCommandAction.DELAY"/>
		/// and <see cref="asyncMaxCommands"/> is reached.
		/// Queued commands consume memory, but they do not consume connections.
		/// <para>
		/// If this limit is reached, the next async command will be rejected with exception
		/// <see cref="Aerospike.Client.AerospikeException.CommandRejected"/>.
		/// If this limit is zero, all async commands will be accepted into the delay queue.
		/// </para>
		/// <para>
		/// The optimal value will depend on your application's magnitude of command bursts and the
		/// amount of memory available to store commands.
		/// </para>
		/// <para>
		/// Default: 0 (no delay queue limit)
		/// </para>
		/// </summary>
		public int asyncMaxCommandsInQueue;

		/// <summary>
		/// Minimum number of asynchronous connections allowed per server node.  Preallocate min connections
		/// on client node creation.  The client will periodically allocate new connections if count falls
		/// below min connections.
		/// <para>
		/// Server proto-fd-idle-ms and client <see cref="Aerospike.Client.ClientPolicy.maxSocketIdle"/>
		/// should be set to zero (no reap) if asyncMinConnsPerNode is greater than zero.  Reaping connections
		/// can defeat the purpose of keeping connections in reserve for a future burst of activity.
		/// </para>
		/// <para>
		/// Default: 0
		/// </para>
		/// </summary>
		public int asyncMinConnsPerNode;

		/// <summary>
		/// Maximum number of asynchronous connections allowed per server node. Commands will go
		/// through retry logic and potentially fail with "ResultCode.NO_MORE_CONNECTIONS" if the maximum
		/// number of connections would be exceeded.
		/// <para>
		/// The number of connections used per node depends on concurrent commands in progress
		/// plus sub-commands used for parallel multi-node commands (batch, scan, and query).
		/// One connection will be used for each command.
		/// </para>
		/// <para>
		/// If the value is -1, the value will be set to <see cref="Aerospike.Client.ClientPolicy.maxConnsPerNode"/>.
		/// </para>
		/// <para>
		/// Default: -1 (Use maxConnsPerNode)
		/// </para>
		/// </summary>
		public int asyncMaxConnsPerNode = -1;

		/// <summary>
		/// Size of buffer allocated for each async command. The size should be a multiple of 8 KB.
		/// If not, the size is rounded up to the nearest 8 KB increment.
		/// <para>
		/// If an async command requires a buffer size less than or equal to asyncBufferSize, the
		/// buffer pool will be used. If an async command requires a buffer size greater than
		/// asyncBufferSize, a new single-use buffer will be created on the heap.
		/// </para>
		/// <para>
		/// This field is also used to size the buffer pool for all async commands:
		/// </para>
		/// <code>
		/// buffer pool size = asyncBufferSize * asyncMaxCommands
		/// </code> 
		/// <para>
		/// Default: 128 * 1024 (128 KB)
		/// </para>
		/// </summary>
		public int asyncBufferSize = 128 * 1024;

		/// <summary>
		/// Copy async client policy from another async client policy.
		/// </summary>
		public AsyncClientPolicy(AsyncClientPolicy other) : base(other)
		{
			this.asyncMaxCommandAction = other.asyncMaxCommandAction;
			this.asyncMaxCommands = other.asyncMaxCommands;
			this.asyncMaxCommandsInQueue = other.asyncMaxCommandsInQueue;
			this.asyncMinConnsPerNode = other.asyncMinConnsPerNode;
			this.asyncMaxConnsPerNode = other.asyncMaxConnsPerNode;
			this.asyncBufferSize = other.asyncBufferSize;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public AsyncClientPolicy()
		{
		}

        public override void ApplyConfigOverrides()
        {
			base.ApplyConfigOverrides();
			var staticClient = ConfigProvider.ConfigurationData.staticProperties.client;

            if (staticClient.async_max_connections_per_node.HasValue)
            {
                this.asyncMaxConnsPerNode = staticClient.async_max_connections_per_node.Value;
            }
            if (staticClient.async_min_connections_per_node.HasValue)
			{
				this.asyncMinConnsPerNode = staticClient.async_min_connections_per_node.Value;
			}
		}
    }
}
