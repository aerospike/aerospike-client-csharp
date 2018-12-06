/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
		/// Replica algorithm used to determine the target node for a single record command.
		/// Batch, scan and query are not affected by replica algorithms.
		/// <para>
		/// Default:  <see cref="Aerospike.Client.Replica.SEQUENCE"/>
		/// </para>
		/// </summary>
		public Replica replica = Replica.SEQUENCE;

		/// <summary>
		/// Socket idle timeout in milliseconds when processing a database command.
		/// <para>
		/// If socketTimeout is zero and totalTimeout is non-zero, then socketTimeout will be set
		/// to totalTimeout.  If both socketTimeout and totalTimeout are non-zero and
		/// socketTimeout > totalTimeout, then socketTimeout will be set to totalTimeout. If both
		/// socketTimeout and totalTimeout are zero, then there will be no socket idle limit.
		/// </para>
		/// <para>
		/// If socketTimeout is not zero and the socket has been idle for at least socketTimeout,
		/// both maxRetries and totalTimeout are checked.  If maxRetries and totalTimeout are not
		/// exceeded, the transaction is retried.
		/// </para>
		/// <para>
		/// For synchronous methods, socketTimeout is the socket SendTimeout and ReceiveTimeout.
		/// For asynchronous methods, the socketTimeout is implemented using the AsyncTimeoutQueue
		/// and socketTimeout is only used if totalTimeout is not defined.
		/// </para>
		/// <para>
		/// Default: 30000ms
		/// </para>
		/// </summary>
		public int socketTimeout = 30000;

		/// <summary>
		/// Total transaction timeout in milliseconds.
		/// <para>
		/// The totalTimeout is tracked on the client and sent to the server along with 
		/// the transaction in the wire protocol.  The client will most likely timeout
		/// first, but the server also has the capability to timeout the transaction.
		/// </para>
		/// <para>
		/// If totalTimeout is not zero and totalTimeout is reached before the transaction
		/// completes, the transaction will abort with
		/// <see cref="Aerospike.Client.AerospikeException.Timeout"/>.
		/// </para>
		/// <para>
		/// If totalTimeout is zero, there will be no total time limit.
		/// </para>
		/// <para>
		/// Default: 0 (no time limit)
		/// </para>
		/// </summary>
		public int totalTimeout;

		/// <summary>
		/// Maximum number of retries before aborting the current transaction.
		/// The initial attempt is not counted as a retry.
		/// <para>
		/// If maxRetries is exceeded, the transaction will abort with
		/// <see cref="Aerospike.Client.AerospikeException.Timeout"/>.
		/// </para>
		/// <para>
		/// WARNING: Database writes that are not idempotent (such as Add()) 
		/// should not be retried because the write operation may be performed 
		/// multiple times if the client timed out previous transaction attempts.
		/// It's important to use a distinct WritePolicy for non-idempotent 
		/// writes which sets maxRetries = 0;
		/// </para>
		/// <para>
		/// Default for read: 2 (initial attempt + 2 retries = 3 attempts)
		/// </para>
		/// <para>
		/// Default for write/query/scan: 0 (no retries)
		/// </para>
		/// </summary>
		public int maxRetries = 2;

		/// <summary>
		/// Milliseconds to sleep between retries.  Enter zero to skip sleep.
		/// This field is ignored when maxRetries is zero.  
		/// This field is also ignored in async mode.
		/// <para>
		/// The sleep only occurs on connection errors and server timeouts
		/// which suggest a node is down and the cluster is reforming.
		/// The sleep does not occur when the client's socketTimeout expires.
		/// </para>
		/// <para>
		/// Reads do not have to sleep when a node goes down because the cluster
		/// does not shut out reads during cluster reformation.  The default for
		/// reads is zero.
		/// </para>
		/// <para>
		/// The default for writes is also zero because writes are not retried by default.
		/// Writes need to wait for the cluster to reform when a node goes down.
		/// Immediate write retries on node failure have been shown to consistently
		/// result in errors.  If maxRetries is greater than zero on a write, then
		/// sleepBetweenRetries should be set high enough to allow the cluster to
		/// reform (>= 500ms).
		/// </para>
		/// <para>
		/// Default: 0 (do not sleep between retries)
		/// </para>
		/// </summary>
		public int sleepBetweenRetries;

		/// <summary>
		/// Send user defined key in addition to hash digest on both reads and writes.
		/// If the key is sent on a write, the key will be stored with the record on 
		/// the server.
		/// <para>
		/// Default: false (do not send the user defined key)
		/// </para>
		/// </summary>
		public bool sendKey;

		/// <summary>
		/// Force reads to be linearized for server namespaces that support strong consistency mode.
		/// <para>
		/// Default: false
		/// </para>
		/// </summary>
		public bool linearizeRead;

		/// <summary>
		/// Copy constructor.
		/// </summary>
		public Policy(Policy other)
		{
			this.priority = other.priority;
			this.consistencyLevel = other.consistencyLevel;
			this.replica = other.replica;
			this.socketTimeout = other.socketTimeout;
			this.totalTimeout = other.totalTimeout;
			this.maxRetries = other.maxRetries;
			this.sleepBetweenRetries = other.sleepBetweenRetries;
			this.sendKey = other.sendKey;
			this.linearizeRead = other.linearizeRead;
		}

		/// <summary>
		/// Default constructor.
		/// </summary>
		public Policy()
		{
		}

		/// <summary>
		/// Create a single timeout by setting socketTimeout and totalTimeout
		/// to the same value.
		/// </summary>
		public void SetTimeout(int timeout)
		{
			this.socketTimeout = timeout;
			this.totalTimeout = timeout;
		}

		/// <summary>
		/// Set socketTimeout and totalTimeout.  If totalTimeout defined and
		/// socketTimeout greater than totalTimeout, set socketTimeout to
		/// totalTimeout.
		/// </summary>
		public void SetTimeouts(int socketTimeout, int totalTimeout)
		{
			this.socketTimeout = socketTimeout;
			this.totalTimeout = totalTimeout;

			if (totalTimeout > 0 && (socketTimeout == 0 || socketTimeout > totalTimeout))
			{
				this.socketTimeout = totalTimeout;
			}
		}
	}
}
