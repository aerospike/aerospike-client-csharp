/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
	/// Container object for client policy Command.
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
	}
}
