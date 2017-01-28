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
	/// Transaction policy attributes used in all database commands.
	/// </summary>
	public class Policy
	{
		/// <summary>
		/// Priority of request relative to other transactions.
		/// Currently, only used for scans.
		/// </summary>
		public Priority priority = Priority.DEFAULT;

		/// <summary>
		/// How replicas should be consulted in a read operation to provide the desired
		/// consistency guarantee.
		/// <para>
		/// Default:  <see cref="Aerospike.Client.ConsistencyLevel.CONSISTENCY_ONE"/>
		/// </para>
		/// </summary>
		public ConsistencyLevel consistencyLevel = ConsistencyLevel.CONSISTENCY_ONE;

		/// <summary>
		/// Send read commands to the node containing the key's partition replica type.
		/// Write commands are not affected by this setting, because all writes are directed 
		/// to the node containing the key's master partition.
		/// <para>
		/// Default:  <see cref="Aerospike.Client.Replica.MASTER"/>
		/// </para>
		/// </summary>
		public Replica replica = Replica.MASTER;

		/// <summary>
		/// Total transaction timeout in milliseconds for both client and server.
		/// The timeout is tracked on the client and also sent to the server along 
		/// with the transaction in the wire protocol.  The client will most likely
		/// timeout first, but the server has the capability to timeout the transaction
		/// as well.
		/// <para>
		/// The timeout is also used as a socket timeout.
		/// Default: 0 (no timeout).
		/// </para>
		/// </summary>
		public int timeout;

		/// <summary>
		/// Maximum number of retries before aborting the current transaction.
		/// A retry may be attempted when there is a network error.  
		/// If maxRetries is exceeded, the abort will occur even if the timeout 
		/// has not yet been exceeded.
		/// <para>
		/// Default: 1
		/// </para>
		/// </summary>
		public int maxRetries = 1;

		/// <summary>
		/// Milliseconds to sleep between retries.  Do not sleep at all if zero.
		/// Used by synchronous commands only.
		/// <para>
		/// Default: 500ms
		/// </para>
		/// </summary>
		public int sleepBetweenRetries = 500;

		/// <summary>
		/// Should the client retry a command if the timeout is reached.
		/// <para>
		/// If false, throw timeout exception when the timeout has been reached.  Note that
		/// retries can still occur if a command fails on a network error before the timeout
		/// has been reached.
		/// </para>
		/// <para>
		/// If true, retry command with same timeout when the timeout has been reached.
		/// The maximum number of retries is defined by maxRetries.
		/// </para>
		/// Default: false
		/// </summary>
		public bool retryOnTimeout;

		/// <summary>
		/// Send user defined key in addition to hash digest on both reads and writes.
		/// <para>
		/// Default: false (do not send the user defined key)
		/// </para>
		/// </summary>
		public bool sendKey;

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public Policy(Policy other)
		{
			this.priority = other.priority;
			this.consistencyLevel = other.consistencyLevel;
			this.replica = other.replica;
			this.timeout = other.timeout;
			this.maxRetries = other.maxRetries;
			this.sleepBetweenRetries = other.sleepBetweenRetries;
			this.retryOnTimeout = other.retryOnTimeout;
			this.sendKey = other.sendKey;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public Policy()
		{
		}
	}
}
