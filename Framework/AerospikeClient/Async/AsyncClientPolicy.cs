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
		/// Maximum number of concurrent asynchronous commands that are active at any point in time.
		/// Concurrent commands can be used to estimate concurrent connections.
		/// The number of concurrent open connections is limited by:
		/// <para>
		/// max open connections = asyncMaxCommands * &lt;number of nodes in cluster&gt;
		/// </para>
		/// The actual number of open connections consumed depends on how balanced the commands are 
		/// between nodes.
		/// <para>
		/// The maximum open connections should not exceed the total socket file descriptors available
		/// on the client machine.  The socket file descriptors available can be determined by the
		/// following command:
		/// </para>
		/// <para>
		/// ulimit -n
		/// </para>
		/// </summary>
		public int asyncMaxCommands = 200;

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
		/// Maximum number of asynchronous connections allowed per server node.  Transactions will go
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
		/// Size of buffer allocated for each async command. If the async command requires a larger size,
		/// the entire async buffer pool will be resized up to <see cref="BufferPool.BUFFER_CUTOFF"/>.
		/// If the async command requires a larger size than <see cref="BufferPool.BUFFER_CUTOFF"/>,
		/// a new buffer is created specifically for that command.
		/// </summary>
		public int asyncBufferSize;

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
	}
}
