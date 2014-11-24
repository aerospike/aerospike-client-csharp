/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
		/// Transaction timeout in milliseconds.
		/// This timeout is used to set the socket timeout and is also sent to the 
		/// server along with the transaction in the wire protocol.
		/// Default to no timeout (0).
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
		/// Copy constructor.
		/// </summary>
		public Policy(Policy other)
		{
			this.priority = other.priority;
			this.consistencyLevel = other.consistencyLevel;
			this.timeout = other.timeout;
			this.maxRetries = other.maxRetries;
			this.sleepBetweenRetries = other.sleepBetweenRetries;
			this.allowProleReads = other.allowProleReads;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public Policy()
		{
		}
	}
}
