/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
		/// between nodes and if asyncMaxConnAction is ACCEPT.
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
		/// Minimum number of asynchronous connections allowed per server node.  Preallocate min connections
		/// on client node creation.  The client will periodically allocate new connections if count falls
		/// below min connections.
		/// <para>
		/// Server proto-fd-idle-ms may also need to be increased substantially if min connections are defined.
		/// The proto-fd-idle-ms default directs the server to close connections that are idle for 60 seconds
		/// which can defeat the purpose of keeping connections in reserve for a future burst of activity.
		/// </para>
		/// <para>
		/// If server proto-fd-idle-ms is changed, client <see cref="Aerospike.Client.ClientPolicy.maxSocketIdle"/>
		/// should also be changed to be a few seconds less than proto-fd-idle-ms.
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
		/// Copy async client policy from another async client policy.
		/// </summary>
		public AsyncClientPolicy(AsyncClientPolicy other) : base(other)
		{
			this.asyncMaxCommandAction = other.asyncMaxCommandAction;
			this.asyncMaxCommands = other.asyncMaxCommands;
			this.asyncMinConnsPerNode = other.asyncMinConnsPerNode;
			this.asyncMaxConnsPerNode = other.asyncMaxConnsPerNode;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public AsyncClientPolicy()
		{
		}
	}
}
