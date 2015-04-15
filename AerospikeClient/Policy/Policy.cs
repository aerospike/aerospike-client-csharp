/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
	/// Container object for transaction policy attributes used in all database
	/// operation calls.
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
		/// consistency guarantee. Default to allowing one replica to be used in the
		/// read operation.
		/// </summary>
		public ConsistencyLevel consistencyLevel = ConsistencyLevel.CONSISTENCY_ONE;

		/// <summary>
		/// Send read commands to the node containing the key's partition replica type.
		/// Write commands are not affected by this setting, because all writes are directed 
		/// to the node containing the key's master partition.
		/// <para>
		/// Default to sending read commands to the node containing the key's master partition.
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
		/// The timeout is also used as a socket timeout.  Retries will not occur
		/// if the timeout limit has been reached.
		/// Default to no timeout (0).
		/// </para>
		/// </summary>
		public int timeout;

		/// <summary>
		/// Maximum number of retries before aborting the current transaction.
		/// A retry is attempted when there is a network error other than timeout.  
		/// If maxRetries is exceeded, the abort will occur even if the timeout 
		/// has not yet been exceeded. The default number of retries is 1.
		/// </summary>
		public int maxRetries = 1;

		/// <summary>
		/// Milliseconds to sleep between retries if a transaction fails and the 
		/// timeout was not exceeded. The default sleep between retries is 500 ms.
		/// </summary>
		public int sleepBetweenRetries = 500;

		/// <summary>
		/// Allow read operations to use replicated data partitions instead of master
		/// partition. By default, both read and write operations are directed to the
		/// master partition.
		/// <para>
		/// This variable is currently only used in batch read/exists operations. For 
		/// batch, this variable should only be set to true when the replication factor
		/// is greater than or equal to the number of nodes in the cluster.
		/// </para>
		/// </summary>
		public bool allowProleReads;

		/// <summary>
		/// Send user defined key in addition to hash digest on both reads and writes.
		/// The default is to not send the user defined key.
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
			this.allowProleReads = other.allowProleReads;
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
