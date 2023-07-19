/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aerospike.Client
{
	/// <summary>
	/// Asynchronous Aerospike client.
	/// <para>
	/// Your application uses this class to perform asynchronous database operations 
	/// such as writing and reading records, and selecting sets of records. Write 
	/// operations include specialized functionality such as append/prepend and arithmetic
	/// addition.
	/// </para>
	/// <para>
	/// This client is thread-safe. One client instance should be used per cluster.
	/// Multiple threads should share this cluster instance.
	/// </para>
	/// <para>
	/// Each record may have multiple bins, unless the Aerospike server nodes are
	/// configured as "single-bin". In "multi-bin" mode, partial records may be
	/// written or read by specifying the relevant subset of bins.
	/// </para>
	/// </summary>
	public class AsyncClient : AerospikeClient, IAsyncClient
	{
		//-------------------------------------------------------
		// Member variables.
		//-------------------------------------------------------

		private readonly new AsyncCluster cluster;

		//-------------------------------------------------------
		// Constructors
		//-------------------------------------------------------

		/// <summary>
		/// Initialize asynchronous client.
		/// If the host connection succeeds, the client will:
		/// <list type="bullet">
		/// <item>Add host to the cluster map</item>
		/// <item>Request host's list of other nodes in cluster</item>
		/// <item>Add these nodes to cluster map</item>
		/// </list>
		/// <para>
		/// If the connection succeeds, the client is ready to process database requests.
		/// If the connection fails, the cluster will remain in a disconnected state
		/// until the server is activated.
		/// </para>
		/// </summary>
		/// <param name="hostname">host name</param>
		/// <param name="port">host port</param>
		/// <exception cref="AerospikeException">if host connection fails</exception>
		public AsyncClient(string hostname, int port) 
			: this(new AsyncClientPolicy(), new Host(hostname, port))
		{
		}

		/// <summary>
		/// Initialize asynchronous client.
		/// The client policy is used to set defaults and size internal data structures.
		/// If the host connection succeeds, the client will:
		/// <list type="bullet">
		/// <item>Add host to the cluster map</item>
		/// <item>Request host's list of other nodes in cluster</item>
		/// <item>Add these nodes to cluster map</item>
		/// </list>
		/// <para>
		/// If the connection succeeds, the client is ready to process database requests.
		/// If the connection fails and the policy's failOnInvalidHosts is true, a connection 
		/// exception will be thrown. Otherwise, the cluster will remain in a disconnected state
		/// until the server is activated.
		/// </para>
		/// </summary>
		/// <param name="policy">client configuration parameters, pass in null for defaults</param>
		/// <param name="hostname">host name</param>
		/// <param name="port">host port</param>
		/// <exception cref="AerospikeException">if host connection fails</exception>
		public AsyncClient(AsyncClientPolicy policy, string hostname, int port) 
			: this(policy, new Host(hostname, port))
		{
		}

		/// <summary>
		/// Initialize asynchronous client with suitable hosts to seed the cluster map.
		/// The client policy is used to set defaults and size internal data structures.
		/// For each host connection that succeeds, the client will:
		/// <list type="bullet">
		/// <item>Add host to the cluster map</item>
		/// <item>Request host's list of other nodes in cluster</item>
		/// <item>Add these nodes to cluster map</item>
		/// </list>
		/// <para>
		/// In most cases, only one host is necessary to seed the cluster. The remaining hosts 
		/// are added as future seeds in case of a complete network failure.
		/// </para>
		/// <para>
		/// If one connection succeeds, the client is ready to process database requests.
		/// If all connections fail and the policy's failIfNotConnected is true, a connection 
		/// exception will be thrown. Otherwise, the cluster will remain in a disconnected state
		/// until the server is activated.
		/// </para>
		/// </summary>
		/// <param name="policy">client configuration parameters, pass in null for defaults</param>
		/// <param name="hosts">array of potential hosts to seed the cluster</param>
		/// <exception cref="AerospikeException">if all host connections fail</exception>
		public AsyncClient(AsyncClientPolicy policy, params Host[] hosts)
			: base (policy)
		{
			if (policy == null)
			{
				policy = new AsyncClientPolicy();
			}
			this.cluster = new AsyncCluster(policy, hosts);
			base.cluster = this.cluster;
		}

		//-------------------------------------------------------
		// Write Record Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously write record bin(s). 
		/// Create listener, call asynchronous put and return task monitor.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task Put(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins)
		{
			WriteListenerAdapter listener = new WriteListenerAdapter(token);
			Put(policy, listener, key, bins);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously write record bin(s). 
		/// Schedules the put command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Put(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			AsyncWrite async = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.WRITE);
			async.Execute();
		}

		//-------------------------------------------------------
		// String Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously append bin string values to existing record bin values.
		/// Create listener, call asynchronous append and return task monitor.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call only works for string values. 
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task Append(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins)
		{
			WriteListenerAdapter listener = new WriteListenerAdapter(token);
			Append(policy, listener, key, bins);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously append bin string values to existing record bin values.
		/// Schedule the append command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call only works for string values. 
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Append(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			AsyncWrite async = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.APPEND);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously prepend bin string values to existing record bin values.
		/// Create listener, call asynchronous prepend and return task monitor.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call works only for string values. 
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task Prepend(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins)
		{
			WriteListenerAdapter listener = new WriteListenerAdapter(token);
			Prepend(policy, listener, key, bins);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously prepend bin string values to existing record bin values.
		/// Schedule the prepend command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call works only for string values. 
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Prepend(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			AsyncWrite async = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.PREPEND);
			async.Execute();
		}

		//-------------------------------------------------------
		// Arithmetic Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously add integer/double bin values to existing record bin values.
		/// Create listener, call asynchronous add and return task monitor.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task Add(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins)
		{
			WriteListenerAdapter listener = new WriteListenerAdapter(token);
			Add(policy, listener, key, bins);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously add integer/double bin values to existing record bin values.
		/// Schedule the add command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Add(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			AsyncWrite async = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.ADD);
			async.Execute();
		}

		//-------------------------------------------------------
		// Delete Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously delete record for specified key.
		/// Create listener, call asynchronous delete and return task monitor.
		/// </summary>
		/// <param name="policy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<bool> Delete(WritePolicy policy, CancellationToken token, Key key)
		{
			DeleteListenerAdapter listener = new DeleteListenerAdapter(token);
			Delete(policy, listener, key);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously delete record for specified key.
		/// Schedule the delete command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Delete(WritePolicy policy, DeleteListener listener, Key key)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			AsyncDelete async = new AsyncDelete(cluster, policy, key, listener);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously delete records for specified keys.
		/// Create listener, call asynchronous delete and return task monitor.
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="deletePolicy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<BatchResults> Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, CancellationToken token, Key[] keys)
		{
			BatchRecordArrayListenerAdapter listener = new BatchRecordArrayListenerAdapter(token);
			Delete(batchPolicy, deletePolicy, listener, keys);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously delete records for specified keys.
		/// Schedule the delete command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// If a key is not found, the corresponding result <see cref="Aerospike.Client.BatchRecord.resultCode"/> will be
		/// <see cref="Aerospike.Client.ResultCode.KEY_NOT_FOUND_ERROR"/>.
		/// </para>
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="deletePolicy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results </param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, BatchRecordArrayListener listener, Key[] keys)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess(new BatchRecord[0], true);
				return;
			}

			if (batchPolicy == null)
			{
				batchPolicy = batchParentPolicyWriteDefault;
			}

			if (deletePolicy == null)
			{
				deletePolicy = batchDeletePolicyDefault;
			}

			BatchAttr attr = new BatchAttr();
			attr.SetDelete(deletePolicy);

			new AsyncBatchOperateRecordArrayExecutor(cluster, batchPolicy, listener, keys, null, attr);
		}

		/// <summary>
		/// Asynchronously delete records for specified keys.
		/// Schedule the delete command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// Each record result is returned in separate OnRecord() calls.
		/// If a key is not found, the corresponding result <see cref="Aerospike.Client.BatchRecord.resultCode"/> will be
		/// <see cref="Aerospike.Client.ResultCode.KEY_NOT_FOUND_ERROR"/>.
		/// </para>
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="deletePolicy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, BatchRecordSequenceListener listener, Key[] keys)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess();
				return;
			}

			if (batchPolicy == null)
			{
				batchPolicy = batchParentPolicyWriteDefault;
			}

			if (deletePolicy == null)
			{
				deletePolicy = batchDeletePolicyDefault;
			}

			BatchAttr attr = new BatchAttr();
			attr.SetDelete(deletePolicy);

			new AsyncBatchOperateRecordSequenceExecutor(cluster, batchPolicy, listener, keys, null, attr);
		}
	
		//-------------------------------------------------------
		// Touch Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously reset record's time to expiration using the policy's expiration.
		/// Create listener, call asynchronous touch and return task monitor.
		/// Fail if the record does not exist.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task Touch(WritePolicy policy, CancellationToken token, Key key)
		{
			WriteListenerAdapter listener = new WriteListenerAdapter(token);
			Touch(policy, listener, key);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously reset record's time to expiration using the policy's expiration.
		/// Schedule the touch command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// Fail if the record does not exist.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Touch(WritePolicy policy, WriteListener listener, Key key)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			AsyncTouch async = new AsyncTouch(cluster, policy, listener, key);
			async.Execute();
		}

		//-------------------------------------------------------
		// Existence-Check Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously determine if a record key exists.
		/// Create listener, call asynchronous exists and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<bool> Exists(Policy policy, CancellationToken token, Key key)
		{
			ExistsListenerAdapter listener = new ExistsListenerAdapter(token);
			Exists(policy, listener, key);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously determine if a record key exists.
		/// Schedule the exists command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Exists(Policy policy, ExistsListener listener, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			AsyncExists async = new AsyncExists(cluster, policy, key, listener);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// Create listener, call asynchronous array exists and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<bool[]> Exists(BatchPolicy policy, CancellationToken token, Key[] keys)
		{
			ExistsArrayListenerAdapter listener = new ExistsArrayListenerAdapter(token);
			Exists(policy, listener, keys);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// Schedule the array exists command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Exists(BatchPolicy policy, ExistsArrayListener listener, Key[] keys)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess(keys, new bool[0]);
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchExistsArrayExecutor(cluster, policy, keys, listener);
		}

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// Schedule the exists command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Exists(BatchPolicy policy, ExistsSequenceListener listener, Key[] keys)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess();
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchExistsSequenceExecutor(cluster, policy, keys, listener);
		}
		
		//-------------------------------------------------------
		// Read Record Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously read entire record for specified key.
		/// Create listener, call asynchronous get and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record> Get(Policy policy, CancellationToken token, Key key)
		{
			RecordListenerAdapter listener = new RecordListenerAdapter(token);
			Get(policy, listener, key);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read entire record for specified key.
		/// Schedule the get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(Policy policy, RecordListener listener, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			AsyncRead async = new AsyncRead(cluster, policy, listener, key, (string[])null);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously read record header and bins for specified key.
		/// Create listener, call asynchronous get and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record> Get(Policy policy, CancellationToken token, Key key, params string[] binNames)
		{
			RecordListenerAdapter listener = new RecordListenerAdapter(token);
			Get(policy, listener, key, binNames);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read record header and bins for specified key.
		/// Schedule the get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(Policy policy, RecordListener listener, Key key, params string[] binNames)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			AsyncRead async = new AsyncRead(cluster, policy, listener, key, binNames);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
		/// Create listener, call asynchronous get header and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record> GetHeader(Policy policy, CancellationToken token, Key key)
		{
			RecordListenerAdapter listener = new RecordListenerAdapter(token);
			GetHeader(policy, listener, key);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
		/// Schedule the get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void GetHeader(Policy policy, RecordListener listener, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			AsyncReadHeader async = new AsyncReadHeader(cluster, policy, listener, key);
			async.Execute();
		}

		//-------------------------------------------------------
		// Batch Read Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously read multiple records for specified batch keys in one batch call.
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// The returned records are located in the same list.
		/// <para>
		/// Create listener, call asynchronous batch get and return task monitor.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="records">list of unique record identifiers and the bins to retrieve.</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public Task<List<BatchRead>> Get(BatchPolicy policy, CancellationToken token, List<BatchRead> records)
		{
			BatchListListenerAdapter listener = new BatchListListenerAdapter(token);
			Get(policy, listener, records);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read multiple records for specified batch keys in one batch call.
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// The returned records are located in the same list.
		/// If the BatchRecord key field is not found, the corresponding record field will be null.
		/// <para>
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="records">list of unique record identifiers and the bins to retrieve.</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public void Get(BatchPolicy policy, BatchListListener listener, List<BatchRead> records)
		{
			if (records.Count == 0)
			{
				listener.OnSuccess(records);
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchReadListExecutor(cluster, policy, listener, records);
		}

		/// <summary>
		/// Asynchronously read multiple records for specified batch keys in one batch call.
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// The returned records are located in the same list.
		/// If the BatchRecord key field is not found, the corresponding record field will be null.
		/// <para>
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="records">list of unique record identifiers and the bins to retrieve.</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public void Get(BatchPolicy policy, BatchSequenceListener listener, List<BatchRead> records)
		{
			if (records.Count == 0)
			{
				listener.OnSuccess();
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchReadSequenceExecutor(cluster, policy, listener, records);
		}

		/// <summary>
		/// Asynchronously read multiple records for specified keys in one batch call.
		/// Create listener, call asynchronous batch get and return task monitor.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys)
		{
			RecordArrayListenerAdapter listener = new RecordArrayListenerAdapter(token);
			Get(policy, listener, keys);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read multiple records for specified keys in one batch call.
		/// Schedule the batch get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess(keys, new Record[0]);
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
		}

		/// <summary>
		/// Asynchronously read multiple records for specified keys in one batch call.
		/// Schedule the get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess();
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
		}

		/// <summary>
		/// Asynchronously read multiple record headers and bins for specified keys in one batch call.
		/// Create listener, call asynchronous batch get and return task monitor.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys, params string[] binNames)
		{
			RecordArrayListenerAdapter listener = new RecordArrayListenerAdapter(token);
			Get(policy, listener, keys, binNames);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read multiple record headers and bins for specified keys in one batch call.
		/// Schedule the batch get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys, params string[] binNames)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess(keys, new Record[0]);
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, binNames, null, Command.INFO1_READ, false);
		}

		/// <summary>
		/// Asynchronously read multiple record headers and bins for specified keys in one batch call.
		/// Schedule the batch get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys, params string[] binNames)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess();
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, binNames, null, Command.INFO1_READ, false);
		}

		/// <summary>
		/// Asynchronously read multiple record headers and bins for specified keys using read operations
		/// in one batch call. Create listener, call asynchronous batch get and return task monitor.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read operations on record</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys, params Operation[] ops)
		{
			RecordArrayListenerAdapter listener = new RecordArrayListenerAdapter(token);
			Get(policy, listener, keys, ops);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read multiple records for specified keys using read operations in one batch call.
		/// Schedule the batch get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read operations on record</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess(keys, new Record[0]);
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, null, ops, Command.INFO1_READ, true);
		}

		/// <summary>
		/// Asynchronously read multiple records for specified keys using read operations in one batch call.
		/// Schedule the batch get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// <para>
		/// Each record result is returned in separate OnRecord() calls.
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read operations on record</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess();
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, null, ops, Command.INFO1_READ, true);
		}

		/// <summary>
		/// Asynchronously read multiple record header data for specified keys in one batch call.
		/// Create listener, call asynchronous batch header get and return task monitor.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record[]> GetHeader(BatchPolicy policy, CancellationToken token, Key[] keys)
		{
			RecordArrayListenerAdapter listener = new RecordArrayListenerAdapter(token);
			GetHeader(policy, listener, keys);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read multiple record header data for specified keys in one batch call.
		/// Schedule the batch get header command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void GetHeader(BatchPolicy policy, RecordArrayListener listener, Key[] keys)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess(keys, new Record[0]);
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA, false);
		}

		/// <summary>
		/// Asynchronously read multiple record header data for specified keys in one batch call.
		/// Schedule the batch get header command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void GetHeader(BatchPolicy policy, RecordSequenceListener listener, Key[] keys)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess();
				return;
			}
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA, false);
		}
		
		//-------------------------------------------------------
		// Generic Database Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously perform multiple read/write operations on a single key in one batch call.
		/// Create listener, call asynchronous operate and return task monitor.
		/// <para>
		/// An example would be to add an integer value to an existing record and then
		/// read the result, all in one database call.
		/// </para>
		/// <para>
		/// The server executes operations in the same order as the operations array.  Both scalar
		/// bin operations (Operation) and CDT bin operations (ListOperation, MapOperation) can be
		/// performed in same call.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="ops">database operations to perform</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record> Operate(WritePolicy policy, CancellationToken token, Key key, params Operation[] ops)
		{
			RecordListenerAdapter listener = new RecordListenerAdapter(token);
			Operate(policy, listener, key, ops);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously perform multiple read/write operations on a single key in one batch call.
		/// Schedule the operate command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// An example would be to add an integer value to an existing record and then
		/// read the result, all in one database call.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="ops">database operations to perform</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Operate(WritePolicy policy, RecordListener listener, Key key, params Operation[] ops)
		{
			OperateArgs args = new OperateArgs(policy, writePolicyDefault, operatePolicyReadDefault, key, ops);
			AsyncOperate async = new AsyncOperate(cluster, listener, key, args);
			async.Execute();
		}

		//-------------------------------------------------------
		// Batch Read/Write Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously read/write multiple records for specified batch keys in one batch call.
		/// Create listener, call asynchronous delete and return task monitor.
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="records">list of unique record identifiers and read/write operations</param>
		/// <returns>Task with completion status: true if all batch sub-transactions were successful.</returns>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<bool> Operate(BatchPolicy policy, CancellationToken token, List<BatchRecord> records)
		{
			BatchOperateListListenerAdapter listener = new BatchOperateListListenerAdapter(token);
			Operate(policy, listener, records);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously read/write multiple records for specified batch keys in one batch call.
		/// Schedule command with a channel selector and return. Another thread will process the 
		/// command and send the results to the listener in a single call.
		/// <para>
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// The returned records are located in the same list.
		/// </para>
		/// <para>
		/// <see cref="BatchRecord"/> can be <see cref="BatchRead"/>, <see cref="BatchWrite"/>, <see cref="BatchDelete"/> or
		/// <see cref="BatchUDF"/>.
		/// </para>
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="records">list of unique record identifiers and read/write operations</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Operate(BatchPolicy policy, BatchOperateListListener listener, List<BatchRecord> records)
		{
			if (records.Count == 0)
			{
				listener.OnSuccess(records, false);
				return;
			}

			if (policy == null)
			{
				policy = batchParentPolicyWriteDefault;
			}
			new AsyncBatchOperateListExecutor(cluster, policy, listener, records);
		}

		/// <summary>
		/// Asynchronously read/write multiple records for specified batch keys in one batch call.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// Each record result is returned in separate OnRecord() calls.
		/// The returned records are located in the same list.
		/// </para>
		/// <para>
		/// <see cref="BatchRecord"/> can be <see cref="BatchRead"/>, <see cref="BatchWrite"/>, <see cref="BatchDelete"/> or
		/// <see cref="BatchUDF"/>.
		/// </para>
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="records">list of unique record identifiers and read/write operations</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Operate(BatchPolicy policy, BatchRecordSequenceListener listener, List<BatchRecord> records)
		{
			if (records.Count == 0)
			{
				listener.OnSuccess();
				return;
			}

			if (policy == null)
			{
				policy = batchParentPolicyWriteDefault;
			}
			new AsyncBatchOperateSequenceExecutor(cluster, policy, listener, records);
		}

		/// <summary>
		/// Asynchronously perform read/write operations on multiple keys.
		/// Create listener, call asynchronous delete and return task monitor.
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="writePolicy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">
		/// read/write operations to perform. <see cref="Operation.Get()"/> is not allowed because it returns a
		/// variable number of bins and makes it difficult (sometimes impossible) to lineup operations with 
		/// results. Instead, use <see cref="Operation.Get(string)"/> for each bin name.
		/// </param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<BatchResults> Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, CancellationToken token, Key[] keys, params Operation[] ops)
		{
			BatchRecordArrayListenerAdapter listener = new BatchRecordArrayListenerAdapter(token);
			Operate(batchPolicy, writePolicy, listener, keys, ops);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously perform read/write operations on multiple keys.
		/// Schedule command with a channel selector and return. Another thread will process the 
		/// command and send the results to the listener in a single call.
		/// <para>
		/// If a key is not found, the corresponding result <see cref="BatchRecord.resultCode"/> will be
		/// <see cref="ResultCode.KEY_NOT_FOUND_ERROR"/>.
		/// </para>
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="writePolicy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">
		/// read/write operations to perform. <see cref="Operation.Get()"/> is not allowed because it returns a
		/// variable number of bins and makes it difficult (sometimes impossible) to lineup operations with 
		/// results. Instead, use <see cref="Operation.Get(string)"/> for each bin name.
		/// </param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, BatchRecordArrayListener listener, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess(new BatchRecord[0], true);
				return;
			}

			if (batchPolicy == null)
			{
				batchPolicy = batchParentPolicyWriteDefault;
			}

			if (writePolicy == null)
			{
				writePolicy = batchWritePolicyDefault;
			}

			BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
			new AsyncBatchOperateRecordArrayExecutor(cluster, batchPolicy, listener, keys, ops, attr);
		}

		/// <summary>
		/// Asynchronously perform read/write operations on multiple keys.
		/// Schedule command with a channel selector and return. Another thread will process the 
		/// command and send the results to the listener.
		/// <para>
		/// Each record result is returned in separate OnRecord() calls.
		/// If a key is not found, the corresponding result <see cref="BatchRecord.resultCode"/> will be
		/// <see cref="ResultCode.KEY_NOT_FOUND_ERROR"/>.
		/// </para>
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="writePolicy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">
		/// read/write operations to perform. <see cref="Operation.Get()"/> is not allowed because it returns a
		/// variable number of bins and makes it difficult (sometimes impossible) to lineup operations with 
		/// results. Instead, use <see cref="Operation.Get(string)"/> for each bin name.
		/// </param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, BatchRecordSequenceListener listener, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess();
				return;
			}

			if (batchPolicy == null)
			{
				batchPolicy = batchParentPolicyWriteDefault;
			}

			if (writePolicy == null)
			{
				writePolicy = batchWritePolicyDefault;
			}

			BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
			new AsyncBatchOperateRecordSequenceExecutor(cluster, batchPolicy, listener, keys, ops, attr);
		}

		//-------------------------------------------------------
		// Scan Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously read all records in specified namespace and set.  If the policy's 
		/// concurrentNodes is specified, each server node will be read in
		/// parallel.  Otherwise, server nodes are read in series.
		/// <para>
		/// This method schedules the scan command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// </para>
		/// </summary>
		/// <param name="policy">scan configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="binNames">optional bin to retrieve. All bins will be returned if not specified.</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void ScanAll(ScanPolicy policy, RecordSequenceListener listener, string ns, string setName, params string[] binNames)
		{
			if (policy == null)
			{
				policy = scanPolicyDefault;
			}

			Node[] nodes = cluster.ValidateNodes();
			PartitionTracker tracker = new PartitionTracker(policy, nodes);
			new AsyncScanPartitionExecutor(cluster, policy, listener, ns, setName, binNames, tracker);
		}

		/// <summary>
		/// Asynchronously read records in specified namespace, set and partition filter.
		/// If the policy's concurrentNodes is specified, each server node will be read in
		/// parallel.  Otherwise, server nodes are read in series.
		/// <para>
		/// This method schedules the scan command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// </para>
		/// </summary>
		/// <param name="policy">scan configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="partitionFilter">filter on a subset of data partitions</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="binNames">optional bin to retrieve. All bins will be returned if not specified.</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void ScanPartitions(ScanPolicy policy, RecordSequenceListener listener, PartitionFilter partitionFilter, string ns, string setName, params string[] binNames)
		{
			if (policy == null)
			{
				policy = scanPolicyDefault;
			}

			Node[] nodes = cluster.ValidateNodes();
			PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
			new AsyncScanPartitionExecutor(cluster, policy, listener, ns, setName, binNames, tracker);
		}
	
		//---------------------------------------------------------------
		// User defined functions
		//---------------------------------------------------------------

		/// <summary>
		/// Asynchronously execute user defined function on server for a single record and return result.
		/// Create listener, call asynchronous execute and return task monitor.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="functionArgs">arguments passed in to user defined function</param>
		/// <returns>task monitor</returns>
		public Task<object> Execute(WritePolicy policy, CancellationToken token, Key key, string packageName, string functionName, params Value[] functionArgs)
		{
			ExecuteListenerAdapter listener = new ExecuteListenerAdapter(token);
			Execute(policy, listener, key, packageName, functionName, functionArgs);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously execute user defined function on server and return result.
		/// The function operates on a single record.
		/// The package name is used to locate the udf file location on the server:
		/// <para>
		/// udf file = &lt;server udf dir&gt;/&lt;package name&gt;.lua
		/// </para>
		/// <para>
		/// This method schedules the execute command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="functionArgs">arguments passed in to user defined function</param>
		/// <exception cref="AerospikeException">if transaction fails</exception>
		public void Execute(WritePolicy policy, ExecuteListener listener, Key key, string packageName, string functionName, params Value[] functionArgs)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			AsyncExecute command = new AsyncExecute(cluster, policy, listener, key, packageName, functionName, functionArgs);
			command.Execute();
		}

		/// <summary>
		/// Asynchronously execute user defined function on server for each key.
		/// Create listener, call asynchronous delete and return task monitor.
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="udfPolicy">udf configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="functionArgs">arguments passed in to user defined function</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<BatchResults> Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, CancellationToken token, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			BatchRecordArrayListenerAdapter listener = new BatchRecordArrayListenerAdapter(token);
			Execute(batchPolicy, udfPolicy, listener, keys, packageName, functionName, functionArgs);
			return listener.Task;
		}

		/// <summary>
		/// Asynchronously execute user defined function on server for each key.
		/// This method schedules the execute command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The package name is used to locate the udf file location:
		/// </para>
		/// <para>
		/// udf file = &lt;server udf dir&gt;/&lt;package name&gt;.lua
		/// </para>
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="udfPolicy">udf configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="functionArgs">arguments passed in to user defined function</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, BatchRecordArrayListener listener, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess(new BatchRecord[0], true);
				return;
			}

			if (batchPolicy == null)
			{
				batchPolicy = batchParentPolicyWriteDefault;
			}

			if (udfPolicy == null)
			{
				udfPolicy = batchUDFPolicyDefault;
			}

			byte[] argBytes = Packer.Pack(functionArgs);

			BatchAttr attr = new BatchAttr();
			attr.SetUDF(udfPolicy);

			new AsyncBatchUDFArrayExecutor(cluster, batchPolicy, listener, keys, packageName, functionName, argBytes, attr);
		}

		/// <summary>
		/// Asynchronously execute user defined function on server for each key.
		/// This method schedules the execute command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// Each record result is returned in separate OnRecord() calls.
		/// <para>
		/// The package name is used to locate the udf file location:
		/// </para>
		/// <para>
		/// udf file = &lt;server udf dir&gt;/&lt;package name&gt;.lua
		/// </para>
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="udfPolicy">udf configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="functionArgs">arguments passed in to user defined function</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, BatchRecordSequenceListener listener, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			if (keys.Length == 0)
			{
				listener.OnSuccess();
				return;
			}

			if (batchPolicy == null)
			{
				batchPolicy = batchParentPolicyWriteDefault;
			}

			if (udfPolicy == null)
			{
				udfPolicy = batchUDFPolicyDefault;
			}

			byte[] argBytes = Packer.Pack(functionArgs);

			BatchAttr attr = new BatchAttr();
			attr.SetUDF(udfPolicy);

			new AsyncBatchUDFSequenceExecutor(cluster, batchPolicy, listener, keys, packageName, functionName, argBytes, attr);
		}

		//-------------------------------------------------------
		// Query Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously execute query on all server nodes.  The query policy's 
		/// <code>maxConcurrentNodes</code> dictate how many nodes can be queried in parallel.
		/// The default is to query all nodes in parallel.
		/// <para>
		/// This method schedules the node's query commands with channel selectors and returns.
		/// Selector threads will process the commands and send the results to the listener.
		/// </para>
		/// <para>
		/// Requires server version 6.0+ if using a secondary index query.
		/// </para>
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="statement">query definition</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public void Query(QueryPolicy policy, RecordSequenceListener listener, Statement statement)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}

			Node[] nodes = cluster.ValidateNodes();

			if (cluster.hasPartitionQuery || statement.filter == null)
			{
				PartitionTracker tracker = new PartitionTracker(policy, statement, nodes);
				new AsyncQueryPartitionExecutor(cluster, policy, listener, statement, tracker);
			}
			else
			{
				new AsyncQueryExecutor(cluster, policy, listener, statement, nodes);
			}
		}

		/// <summary>
		/// Asynchronously execute query for specified partitions. The query policy's 
		/// <code>maxConcurrentNodes</code> dictate how many nodes can be queried in parallel.
		/// The default is to query all nodes in parallel.
		/// <para>
		/// This method schedules the node's query commands with channel selectors and returns.
		/// Selector threads will process the commands and send the results to the listener.
		/// </para>
		/// <para>
		/// Each record result is returned in separate OnRecord() calls. 
		/// </para>
		/// <para>
		/// Requires server version 6.0+ if using a secondary index query.
		/// </para>
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="statement">query definition</param>
		/// <param name="partitionFilter">filter on a subset of data partitions</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public void QueryPartitions
		(
			QueryPolicy policy,
			RecordSequenceListener listener,
			Statement statement,
			PartitionFilter partitionFilter
		)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}

			Node[] nodes = cluster.ValidateNodes();

			if (cluster.hasPartitionQuery || statement.filter == null)
			{
				PartitionTracker tracker = new PartitionTracker(policy, statement, nodes, partitionFilter);
				new AsyncQueryPartitionExecutor(cluster, policy, listener, statement, tracker);
			}
			else
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "QueryPartitions() not supported");
			}
		}
	}
}
