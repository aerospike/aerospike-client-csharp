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
	public class AsyncClientProxy : AerospikeClientProxy, IAsyncClient
	{
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
		public AsyncClientProxy(string hostname, int port)
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
		public AsyncClientProxy(AsyncClientPolicy policy, string hostname, int port)
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
		public AsyncClientProxy(AsyncClientPolicy policy, params Host[] hosts)
			: base(policy, hosts)
		{
		}

		//-------------------------------------------------------
		// Write Record Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously write record bin(s). 
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
		public async Task Put(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins)
		{
			policy ??= writePolicyDefault;
			var buffer = new Buffer();
			WriteCommandProxy command = new(buffer, callInvoker, policy, key, bins, Operation.Type.WRITE);
			await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <param name="bins"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Put(WritePolicy, CancellationToken, Key, Bin[])"/>
		[Obsolete("Method not supported in proxy client: Put")]
		public void Put(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			throw new AerospikeException(NotSupported + "Put");
		}

		//-------------------------------------------------------
		// String Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously append bin string values to existing record bin values.
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
		public async Task Append(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins)
		{
			policy ??= writePolicyDefault;
			var buffer = new Buffer();
			WriteCommandProxy command = new(buffer, callInvoker, policy, key, bins, Operation.Type.APPEND);
			await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <param name="bins"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Append(WritePolicy, CancellationToken, Key, Bin[])"/>
		[Obsolete("Method not supported in proxy client: Append")]
		public void Append(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			throw new AerospikeException(NotSupported + "Append");
		}

		/// <summary>
		/// Asynchronously prepend bin string values to existing record bin values.
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
		public async Task Prepend(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins)
		{
			policy ??= writePolicyDefault;
			var buffer = new Buffer();
			WriteCommandProxy command = new(buffer, callInvoker, policy, key, bins, Operation.Type.PREPEND);
			await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <param name="bins"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Prepend(WritePolicy, CancellationToken, Key, Bin[])"/>
		[Obsolete("Method not supported in proxy client: Prepend")]
		public void Prepend(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			throw new AerospikeException(NotSupported + "Prepend");
		}

		//-------------------------------------------------------
		// Arithmetic Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously add integer/double bin values to existing record bin values.
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
		public async Task Add(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins)
		{
			policy ??= writePolicyDefault;
			var buffer = new Buffer();
			WriteCommandProxy command = new(buffer, callInvoker, policy, key, bins, Operation.Type.ADD);
			await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <param name="bins"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Add(WritePolicy, CancellationToken, Key, Bin[])"/>
		[Obsolete("Method not supported in proxy client: Add")]
		public void Add(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			throw new AerospikeException(NotSupported + "Add");
		}

		//-------------------------------------------------------
		// Delete Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously delete record for specified key.
		/// </summary>
		/// <param name="policy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<bool> Delete(WritePolicy policy, CancellationToken token, Key key)
		{
			policy ??= writePolicyDefault;
			Buffer buffer = new();
			DeleteCommandProxy command = new(buffer, callInvoker, policy, key);
			return await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Delete(WritePolicy, CancellationToken, Key)"/>
		[Obsolete("Method not supported in proxy client: Delete")]
		public void Delete(WritePolicy policy, DeleteListener listener, Key key)
		{
			throw new AerospikeException(NotSupported + "Delete");
		}

		/// <summary>
		/// Asynchronously delete records for specified keys.
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="deletePolicy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<BatchResults> Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, CancellationToken token, Key[] keys)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(Array.Empty<BatchRecord>(), true);
			}

			batchPolicy ??= batchParentPolicyWriteDefault;
			deletePolicy ??= batchDeletePolicyDefault;

			BatchAttr attr = new();
			attr.SetDelete(deletePolicy);

			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchDelete(deletePolicy, keys[i]);
			}

			try
			{
				BatchStatus status = new(true);
				await Operate(batchPolicy, records, status, token);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				// Batch terminated on fatal error.
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="batchPolicy"></param>
		/// <param name="deletePolicy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Delete(BatchPolicy, BatchDeletePolicy, CancellationToken, Key[])"/>
		[Obsolete("Method not supported in proxy client: Delete")]
		public void Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, BatchRecordArrayListener listener, Key[] keys)
		{
			throw new AerospikeException(NotSupported + "Delete");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="batchPolicy"></param>
		/// <param name="deletePolicy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Delete(BatchPolicy, BatchDeletePolicy, CancellationToken, Key[])"/>
		[Obsolete("Method not supported in proxy client: Delete")]
		public void Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, BatchRecordSequenceListener listener, Key[] keys)
		{
			throw new AerospikeException(NotSupported + "Delete");
		}

		//-------------------------------------------------------
		// Touch Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously reset record's time to expiration using the policy's expiration.
		/// Fail if the record does not exist.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task Touch(WritePolicy policy, CancellationToken token, Key key)
		{
			policy ??= writePolicyDefault;
			Buffer buffer = new();
			TouchCommandProxy command = new(buffer, callInvoker, policy, key);
			await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Touch(WritePolicy, CancellationToken, Key)"/>
		[Obsolete("Method not supported in proxy client: Touch")]
		public void Touch(WritePolicy policy, WriteListener listener, Key key)
		{
			throw new AerospikeException(NotSupported + "Touch");
		}

		//-------------------------------------------------------
		// Existence-Check Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously determine if a record key exists.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<bool> Exists(Policy policy, CancellationToken token, Key key)
		{
			policy ??= readPolicyDefault;
			Buffer buffer = new();
			ExistsCommandProxy command = new(buffer, callInvoker, policy, key);
			return await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Exists(Policy, CancellationToken, Key)"/>
		[Obsolete("Method not supported in proxy client: Exists")]
		public void Exists(Policy policy, ExistsListener listener, Key key)
		{
			throw new AerospikeException(NotSupported + "Exists");
		}

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// Create listener, call asynchronous array exists and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<bool[]> Exists(BatchPolicy policy, CancellationToken token, Key[] keys)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<bool>();
			}

			policy ??= batchPolicyDefault;

			bool[] existsArray = new bool[keys.Length];

			BatchRecord[] records = new BatchRecord[keys.Length];
			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRead(keys[i], false);
			}

			try
			{
				BatchStatus status = new(false);
				await Operate(policy, records, status, token);
				for (int i = 0; i < keys.Length; i++)
				{
					existsArray[i] = records[i].record != null;
				}
				return existsArray;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchExists(existsArray, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Exists(BatchPolicy, CancellationToken, Key[])"/>
		[Obsolete("Method not supported in proxy client: Exists")]
		public void Exists(BatchPolicy policy, ExistsArrayListener listener, Key[] keys)
		{
			throw new AerospikeException(NotSupported + "Exists");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Exists(BatchPolicy, CancellationToken, Key[])"/>
		[Obsolete("Method not supported in proxy client: Exists")]
		public void Exists(BatchPolicy policy, ExistsSequenceListener listener, Key[] keys)
		{
			throw new AerospikeException(NotSupported + "Exists");
		}

		//-------------------------------------------------------
		// Read Record Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously read entire record for specified key.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<Record> Get(Policy policy, CancellationToken token, Key key)
		{
			policy ??= readPolicyDefault;
			Buffer buffer = new();
			ReadCommandProxy command = new(buffer, callInvoker, policy, key);
			return await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(Policy, CancellationToken, Key)"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(Policy policy, RecordListener listener, Key key)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Asynchronously read record header and bins for specified key.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<Record> Get(Policy policy, CancellationToken token, Key key, params string[] binNames)
		{
			policy ??= readPolicyDefault;
			Buffer buffer = new();
			ReadCommandProxy command = new(buffer, callInvoker, policy, key, binNames);
			return await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <param name="binNames"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(Policy, CancellationToken, Key, string[])"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(Policy policy, RecordListener listener, Key key, params string[] binNames)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<Record> GetHeader(Policy policy, CancellationToken token, Key key)
		{
			policy ??= readPolicyDefault;
			Buffer buffer = new();
			ReadHeaderCommandProxy command = new(buffer, callInvoker, policy, key);
			return await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="GetHeader(Policy, CancellationToken, Key)"/>
		[Obsolete("Method not supported in proxy client: GetHeader")]
		public void GetHeader(Policy policy, RecordListener listener, Key key)
		{
			throw new AerospikeException(NotSupported + "GetHeader");
		}

		//-------------------------------------------------------
		// Batch Read Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously read multiple records for specified batch keys in one batch call.
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// The returned records are located in the same list.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="records">list of unique record identifiers and the bins to retrieve.</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public async Task<List<BatchRead>> Get(BatchPolicy policy, CancellationToken token, List<BatchRead> records)
		{
			if (records.Count == 0)
			{
				return new List<BatchRead>();
			}

			policy ??= batchPolicyDefault;

			BatchRecord[] batchRecords = new BatchRecord[records.Count];
			for (int i = 0; i < records.Count; i++)
			{
				batchRecords[i] = records[i];
			}

			try
			{
				BatchStatus status = new(false);
				await Operate(policy, batchRecords, status, token);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecordArray(batchRecords, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="records"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(BatchPolicy, CancellationToken, List{BatchRead})"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(BatchPolicy policy, BatchListListener listener, List<BatchRead> records)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="records"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(BatchPolicy, CancellationToken, List{BatchRead})"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(BatchPolicy policy, BatchSequenceListener listener, List<BatchRead> records)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Asynchronously read multiple records for specified keys in one batch call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<Record>();
			}

			policy ??= batchPolicyDefault;

			Record[] records = new Record[keys.Length];
			BatchRecord[] batchRecords = new BatchRecord[keys.Length];
			for (int i = 0; i < keys.Length; i++)
			{
				batchRecords[i] = new BatchRead(keys[i], true);
			}

			try
			{
				BatchStatus status = new(false);
				await Operate(policy, batchRecords, status, token);
				for (int i = 0; i < keys.Length; i++)
				{
					records[i] = batchRecords[i].record;
				}
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(BatchPolicy, CancellationToken, Key[])"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(BatchPolicy, CancellationToken, Key[])"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Asynchronously read multiple record headers and bins for specified keys in one batch call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys, params string[] binNames)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<Record>();
			}

			policy ??= batchPolicyDefault;

			Record[] records = new Record[keys.Length];
			BatchRecord[] batchRecords = new BatchRecord[keys.Length];
			for (int i = 0; i < keys.Length; i++)
			{
				batchRecords[i] = new BatchRead(keys[i], binNames);
			}

			try
			{
				BatchStatus status = new(false);
				await Operate(policy, batchRecords, status, token);
				for (int i = 0; i < keys.Length; i++)
				{
					records[i] = batchRecords[i].record;
				}
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <param name="binNames"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(BatchPolicy, CancellationToken, Key[], string[])"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys, params string[] binNames)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <param name="binNames"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(BatchPolicy, CancellationToken, Key[], string[])"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys, params string[] binNames)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Asynchronously read multiple record headers and bins for specified keys using read operations
		/// in one batch call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read operations on record</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<Record>();
			}

			policy ??= batchPolicyDefault;

			Record[] records = new Record[keys.Length];
			BatchRecord[] batchRecords = new BatchRecord[keys.Length];
			for (int i = 0; i < keys.Length; i++)
			{
				batchRecords[i] = new BatchRead(keys[i], ops);
			}

			try
			{
				BatchStatus status = new(false);
				await Operate(policy, batchRecords, status, token);
				for (int i = 0; i < batchRecords.Length; i++)
				{
					records[i] = batchRecords[i].record;
				}
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <param name="ops"></param>
		/// <exception cref="AerospikeException"></exception>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys, params Operation[] ops)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <param name="ops"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Get(BatchPolicy, CancellationToken, Key[], Operation[])"/>
		[Obsolete("Method not supported in proxy client: Get")]
		public void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys, params Operation[] ops)
		{
			throw new AerospikeException(NotSupported + "Get");
		}

		/// <summary>
		/// Asynchronously read multiple record header data for specified keys in one batch call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<Record[]> GetHeader(BatchPolicy policy, CancellationToken token, Key[] keys)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<Record>();
			}

			policy ??= batchPolicyDefault;

			Record[] records = new Record[keys.Length];
			BatchRecord[] batchRecords = new BatchRecord[keys.Length];
			for (int i = 0; i < keys.Length; i++)
			{
				batchRecords[i] = new BatchRead(keys[i], false);
			}

			try
			{
				BatchStatus status = new(false);
				await Operate(policy, batchRecords, status, token);
				for (int i = 0; i < batchRecords.Length; i++)
				{
					records[i] = batchRecords[i].record;
				}
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="GetHeader(BatchPolicy, CancellationToken, Key[])"/>
		[Obsolete("Method not supported in proxy client: GetHeader")]
		public void GetHeader(BatchPolicy policy, RecordArrayListener listener, Key[] keys)
		{
			throw new AerospikeException(NotSupported + "GetHeader");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="GetHeader(BatchPolicy, CancellationToken, Key[])"/>
		[Obsolete("Method not supported in proxy client: GetHeader")]
		public void GetHeader(BatchPolicy policy, RecordSequenceListener listener, Key[] keys)
		{
			throw new AerospikeException(NotSupported + "GetHeader");
		}

		//-------------------------------------------------------
		// Generic Database Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously perform multiple read/write operations on a single key in one batch call.
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
		public async Task<Record> Operate(WritePolicy policy, CancellationToken token, Key key, params Operation[] ops)
		{
			OperateArgs args = new(policy, writePolicyDefault, operatePolicyReadDefault, key, ops);
			Buffer buffer = new();
			OperateCommandProxy command = new(buffer, callInvoker, key, args);
			return await command.Execute(token);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <param name="ops"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Operate(WritePolicy, CancellationToken, Key, Operation[])"/>
		[Obsolete("Method not supported in proxy client: Operate")]
		public void Operate(WritePolicy policy, RecordListener listener, Key key, params Operation[] ops)
		{
			throw new AerospikeException(NotSupported + "Operate");
		}

		//-------------------------------------------------------
		// Batch Read/Write Operations
		//-------------------------------------------------------

		private async Task Operate(BatchPolicy policy, BatchRecord[] records, BatchStatus status, CancellationToken token)
		{
			policy ??= batchParentPolicyWriteDefault;
			Buffer buffer = new();
			BatchNode batch = new(records);
			BatchOperateListCommandProxy command = new(buffer, callInvoker, batch, policy, records, status);
			await command.Execute(token);
		}

		/// <summary>
		/// Asynchronously read/write multiple records for specified batch keys in one batch call.
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="records">list of unique record identifiers and read/write operations</param>
		/// <returns>Task with completion status: true if all batch sub-transactions were successful.</returns>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task<bool> Operate(BatchPolicy policy, CancellationToken token, List<BatchRecord> records)
		{
			policy ??= batchParentPolicyWriteDefault;
			BatchStatus status = new(true);
			await Operate(policy, records.ToArray(), status, token);
			return status.GetStatus();
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="records"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Operate(BatchPolicy, CancellationToken, List{BatchRecord})"/>
		[Obsolete("Method not supported in proxy client: Operate")]
		public void Operate(BatchPolicy policy, BatchOperateListListener listener, List<BatchRecord> records)
		{
			throw new AerospikeException(NotSupported + "Operate");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="records"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Operate(BatchPolicy, CancellationToken, List{BatchRecord})"/>
		[Obsolete("Method not supported in proxy client: Operate")]
		public void Operate(BatchPolicy policy, BatchRecordSequenceListener listener, List<BatchRecord> records)
		{
			throw new AerospikeException(NotSupported + "Operate");
		}

		/// <summary>
		/// Asynchronously perform read/write operations on multiple keys.
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
		public async Task<BatchResults> Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, CancellationToken token, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(Array.Empty<BatchRecord>(), true);
			}

			batchPolicy ??= batchParentPolicyWriteDefault;
			writePolicy ??= batchWritePolicyDefault;

			BatchAttr attr = new(batchPolicy, writePolicy, ops);
			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			Buffer buffer = new();
			BatchStatus status = new(true);
			BatchNode batchNode = new(records);
			BatchOperateArrayCommandProxy command = new(buffer, callInvoker, batchNode, batchPolicy, keys, ops, records, attr, status);

			try
			{
				await command.Execute(token);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="batchPolicy"></param>
		/// <param name="writePolicy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <param name="ops"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Operate(BatchPolicy, BatchWritePolicy, CancellationToken, Key[], Operation[])"/>
		[Obsolete("Method not supported in proxy client: Operate")]
		public void Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, BatchRecordArrayListener listener, Key[] keys, params Operation[] ops)
		{
			throw new AerospikeException(NotSupported + "Operate");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="batchPolicy"></param>
		/// <param name="writePolicy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <param name="ops"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Operate(BatchPolicy, BatchWritePolicy, CancellationToken, Key[], Operation[])"/>
		[Obsolete("Method not supported in proxy client: Operate")]
		public void Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, BatchRecordSequenceListener listener, Key[] keys, params Operation[] ops)
		{
			throw new AerospikeException(NotSupported + "Operate");
		}

		//-------------------------------------------------------
		// Scan Operations
		//-------------------------------------------------------

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="ns"></param>
		/// <param name="setName"></param>
		/// <param name="binNames"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="ScanAll(ScanPolicy, CancellationToken, string, string, string[])"/>
		[Obsolete("Method not supported in proxy client: ScanAll")]
		public void ScanAll(ScanPolicy policy, RecordSequenceListener listener, string ns, string setName, params string[] binNames)
		{
			throw new AerospikeException(NotSupported + "ScanAll");
		}

		/// <summary>
		/// Asynchronously read all records in specified namespace and set.
		/// </summary>
		/// <param name="policy">scan configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="binNames">
		/// optional bin to retrieve. All bins will be returned if not specified.
		/// </param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		public async Task<RecordSet> ScanAll(ScanPolicy policy, CancellationToken token, string ns, string setName, params string[] binNames)
		{
			return await ScanPartitions(policy, token, PartitionFilter.All(), ns, setName, binNames);
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="partitionFilter"></param>
		/// <param name="ns"></param>
		/// <param name="setName"></param>
		/// <param name="binNames"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="ScanPartitions(ScanPolicy, CancellationToken, PartitionFilter, string, string, string[])"/>
		[Obsolete("Method not supported in proxy client: ScanPartitions")]
		public void ScanPartitions(ScanPolicy policy, RecordSequenceListener listener, PartitionFilter partitionFilter, string ns, string setName, params string[] binNames)
		{
			throw new AerospikeException(NotSupported + "ScanPartitions");
		}

		/// <summary>
		/// Asynchronously read records in specified namespace, set and partition filter.
		/// </summary>
		/// <param name="policy">scan configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="partitionFilter">filter on a subset of data partitions</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="binNames">optional bin to retrieve. All bins will be returned if not specified.</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		public async Task<RecordSet> ScanPartitions(ScanPolicy policy, CancellationToken token, PartitionFilter partitionFilter, string ns, string setName, params string[] binNames)
		{
			policy ??= scanPolicyDefault;
			Buffer buffer = new();
			PartitionTracker tracker = new(policy, null, partitionFilter);
			RecordSet recordSet = new(null, policy.recordQueueSize, token);
			ScanPartitionCommandProxy command = new(buffer, callInvoker, policy, ns, setName, binNames, tracker, partitionFilter, recordSet);
			await command.Execute(token);
			return recordSet;
		}

		//---------------------------------------------------------------
		// User defined functions
		//---------------------------------------------------------------

		/// <summary>
		/// Asynchronously execute user defined function on server for a single record and return result.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="functionArgs">arguments passed in to user defined function</param>
		/// <returns>task monitor</returns>
		public async Task<object> Execute(WritePolicy policy, CancellationToken token, Key key, string packageName, string functionName, params Value[] functionArgs)
		{
			policy ??= writePolicyDefault;
			Buffer buffer = new();
			ExecuteCommandProxy command = new(buffer, callInvoker, policy, key, packageName, functionName, functionArgs);
			await command.Execute(token);

			var record = command.Record;

			IDictionary<string, object> map = record.bins;
			if (map.TryGetValue("SUCCESS", out object obj))
			{
				return obj;
			}

			if (map.TryGetValue("FAILURE", out obj))
			{
				throw new AerospikeException(obj.ToString());
			}
			throw new AerospikeException("Invalid UDF return value");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="key"></param>
		/// <param name="packageName"></param>
		/// <param name="functionName"></param>
		/// <param name="functionArgs"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Execute(WritePolicy, CancellationToken, Key, string, string, Value[])"/>
		[Obsolete("Method not supported in proxy client: Execute")]
		public void Execute(WritePolicy policy, ExecuteListener listener, Key key, string packageName, string functionName, params Value[] functionArgs)
		{
			throw new AerospikeException(NotSupported + "Execute");
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
		public async Task<BatchResults> Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, CancellationToken token, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(Array.Empty<BatchRecord>(), true);
			}

			batchPolicy ??= batchParentPolicyWriteDefault;
			udfPolicy ??= batchUDFPolicyDefault;

			BatchAttr attr = new();
			attr.SetUDF(udfPolicy);

			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchUDF(udfPolicy, keys[i], packageName, functionName, functionArgs);
			}

			try
			{
				BatchStatus status = new(true);
				await Operate(batchPolicy, records, status, token);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				// Batch terminated on fatal error.
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="batchPolicy"></param>
		/// <param name="udfPolicy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <param name="packageName"></param>
		/// <param name="functionName"></param>
		/// <param name="functionArgs"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Execute(BatchPolicy, BatchUDFPolicy, CancellationToken, Key[], string, string, Value[])"/>
		[Obsolete("Method not supported in proxy client: Execute")]
		public void Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, BatchRecordArrayListener listener, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			throw new AerospikeException(NotSupported + "Execute");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="batchPolicy"></param>
		/// <param name="udfPolicy"></param>
		/// <param name="listener"></param>
		/// <param name="keys"></param>
		/// <param name="packageName"></param>
		/// <param name="functionName"></param>
		/// <param name="functionArgs"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Execute(BatchPolicy, BatchUDFPolicy, CancellationToken, Key[], string, string, Value[])"/>
		[Obsolete("Method not supported in proxy client: Execute")]
		public void Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, BatchRecordSequenceListener listener, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			throw new AerospikeException(NotSupported + "Execute");
		}

		//-------------------------------------------------------
		// Query Operations
		//-------------------------------------------------------

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="statement"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="Query(QueryPolicy, CancellationToken, Statement)"/>
		[Obsolete("Method not supported in proxy client: Query")]
		public void Query(QueryPolicy policy, RecordSequenceListener listener, Statement statement)
		{
			throw new AerospikeException(NotSupported + "Query");
		}

		/// <summary>
		/// Not supported in proxy client
		/// </summary>
		/// <param name="policy"></param>
		/// <param name="listener"></param>
		/// <param name="statement"></param>
		/// <param name="partitionFilter"></param>
		/// <exception cref="AerospikeException"></exception>
		/// <seealso cref="QueryPartitions(QueryPolicy, CancellationToken, Statement, PartitionFilter)"/>
		[Obsolete("Method not supported in proxy client: Query")]
		public void QueryPartitions
		(
			QueryPolicy policy,
			RecordSequenceListener listener,
			Statement statement,
			PartitionFilter partitionFilter
		)
		{
			throw new AerospikeException(NotSupported + "Query");
		}

		/// <summary>
		/// Execute query and call action for each record returned from server.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="statement">query definition</param>
		/// <param name="action">action methods to be called for each record</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public async Task Query(QueryPolicy policy, CancellationToken token, Statement statement, Action<Key, Record> action)
		{
			using RecordSet rs = await Query(policy, token, statement);
			while (rs.Next())
			{
				action(rs.Key, rs.Record);
			}
		}

		/// <summary>
		/// Execute query on all server nodes and return records via the listener. This method will
		/// block until the query is complete. Listener callbacks are made within the scope of this call.
		/// <para>
		/// If <see cref="QueryPolicy.maxConcurrentNodes"/> is not 1, the supplied listener must handle
		/// shared data in a thread-safe manner, because the listener will be called by multiple query
		/// threads (one thread per node) in parallel.
		/// </para>
		/// <para>
		/// Requires server version 6.0+ if using a secondary index query.
		/// </para>
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="statement">query definition</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public async Task<RecordSet> Query(QueryPolicy policy, CancellationToken token, Statement statement)
		{
			return await QueryPartitions(policy, token, statement, PartitionFilter.All());
		}

		/// <summary>
		/// Execute query for specified partitions and return record iterator.  The query executor puts
		/// records on a queue in separate threads.  The calling thread concurrently pops records off
		/// the queue through the record iterator.
		/// <para>
		/// Requires server version 6.0+ if using a secondary index query.
		/// </para>
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		///<param name="token">cancellation token</param>
		/// <param name="statement">query definition</param>
		/// <param name="partitionFilter">filter on a subset of data partitions</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public async Task<RecordSet> QueryPartitions
		(
			QueryPolicy policy,
			CancellationToken token,
			Statement statement,
			PartitionFilter partitionFilter
		)
		{
			policy ??= queryPolicyDefault;
			Buffer buffer = new();
			PartitionTracker tracker = new(policy, statement, (Node[])null, partitionFilter);
			RecordSet recordSet = new(null, policy.recordQueueSize, token);
			QueryPartitionCommandProxy command = new(buffer, callInvoker, policy, statement, tracker, partitionFilter, recordSet);
			await command.Execute(token);
			return recordSet;
		}
	}
}
