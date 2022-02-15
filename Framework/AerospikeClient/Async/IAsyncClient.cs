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
    /// Your application uses this interface to perform asynchronous database operations 
    /// such as writing and reading records, and selecting sets of records. Write 
    /// operations include specialized functionality such as append/prepend and arithmetic
    /// addition.
    /// </para>
    /// <para>
    /// Clients implementing this interface must be thread-safe. One client instance should be used per cluster.
    /// Multiple threads should share same cluster instance.
    /// </para>
    /// <para>
    /// Each record may have multiple bins, unless the Aerospike server nodes are
    /// configured as "single-bin". In "multi-bin" mode, partial records may be
    /// written or read by specifying the relevant subset of bins.
    /// </para>
    /// </summary>
    public interface IAsyncClient : IAerospikeClient
    {
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
		Task Put(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins);

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
		void Put(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins);

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
		Task Append(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins);

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
		void Append(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins);

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
		Task Prepend(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins);

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
		void Prepend(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins);

		//-------------------------------------------------------
		// Arithmetic Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously add integer bin values to existing record bin values.
		/// Create listener, call asynchronous add and return task monitor.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call only works for integer values. 
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		Task Add(WritePolicy policy, CancellationToken token, Key key, params Bin[] bins);

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
		void Add(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins);
		
		//-------------------------------------------------------
		// Delete Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously delete record for specified key.
		/// Create listener, call asynchronous delete and return task monitor.
		/// <para>
		/// The policy specifies the transaction timeout.
		/// </para>
		/// </summary>
		/// <param name="policy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		Task<bool> Delete(WritePolicy policy, CancellationToken token, Key key);

		/// <summary>
		/// Asynchronously delete record for specified key.
		/// Schedule the delete command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void Delete(WritePolicy policy, DeleteListener listener, Key key);

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
		Task<BatchResults> Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, CancellationToken token, Key[] keys);

		/// <summary>
		/// Asynchronously delete records for specified keys.
		/// Schedule the delete command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// If a key is not found, the corresponding result <see cref="Aerospike.Client.BatchRecord.resultCode"/> will be
		/// <see cref="Aerospike.Client.ResultCode.KEY_NOT_FOUND_ERROR"/>.
		/// </para>
		/// <para>
		/// Requires server version 6.0+
		/// </para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="deletePolicy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results </param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, BatchRecordArrayListener listener, Key[] keys);

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
		void Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, BatchRecordSequenceListener listener, Key[] keys);

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
		Task Touch(WritePolicy policy, CancellationToken token, Key key);

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
		void Touch(WritePolicy policy, WriteListener listener, Key key);

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
		Task<bool> Exists(Policy policy, CancellationToken token, Key key);

		/// <summary>
		/// Asynchronously determine if a record key exists.
		/// Schedule the exists command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void Exists(Policy policy, ExistsListener listener, Key key);

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// Create listener, call asynchronous array exists and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		Task<bool[]> Exists(BatchPolicy policy, CancellationToken token, Key[] keys);

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// Schedule the array exists command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void Exists(BatchPolicy policy, ExistsArrayListener listener, Key[] keys);

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// Schedule the exists command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void Exists(BatchPolicy policy, ExistsSequenceListener listener, Key[] keys);

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
		Task<Record> Get(Policy policy, CancellationToken token, Key key);

		/// <summary>
		/// Asynchronously read entire record for specified key.
		/// Schedule the get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void Get(Policy policy, RecordListener listener, Key key);

		/// <summary>
		/// Asynchronously read record header and bins for specified key.
		/// Create listener, call asynchronous get and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		Task<Record> Get(Policy policy, CancellationToken token, Key key, params string[] binNames);

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
		void Get(Policy policy, RecordListener listener, Key key, params string[] binNames);

		/// <summary>
		/// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
		/// Create listener, call asynchronous get header and return task monitor.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		Task<Record> GetHeader(Policy policy, CancellationToken token, Key key);

		/// <summary>
		/// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
		/// Schedule the get command with a channel selector and return.
		/// Another thread will process the command and send the results to the listener.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void GetHeader(Policy policy, RecordListener listener, Key key);

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
		Task<List<BatchRead>> Get(BatchPolicy policy, CancellationToken token, List<BatchRead> records);

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
		void Get(BatchPolicy policy, BatchListListener listener, List<BatchRead> records);

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
		void Get(BatchPolicy policy, BatchSequenceListener listener, List<BatchRead> records);

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
		Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys);

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
		void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys);

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
		void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys);

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
		Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys, params string[] binNames);

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
		void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys, params string[] binNames);

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
		void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys, params string[] binNames);

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
		Task<Record[]> Get(BatchPolicy policy, CancellationToken token, Key[] keys, params Operation[] ops);

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
		void Get(BatchPolicy policy, RecordArrayListener listener, Key[] keys, params Operation[] ops);

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
		void Get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys, params Operation[] ops);
		
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
		Task<Record[]> GetHeader(BatchPolicy policy, CancellationToken token, Key[] keys);

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
		void GetHeader(BatchPolicy policy, RecordArrayListener listener, Key[] keys);

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
		void GetHeader(BatchPolicy policy, RecordSequenceListener listener, Key[] keys);

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
		Task<Record> Operate(WritePolicy policy, CancellationToken token, Key key, params Operation[] ops);

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
		void Operate(WritePolicy policy, RecordListener listener, Key key, params Operation[] ops);

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
		/// <exception cref="AerospikeException">if queue is full</exception>
		Task<bool> Operate(BatchPolicy policy, CancellationToken token, List<BatchRecord> records);

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
		void Operate(BatchPolicy policy, BatchOperateListListener listener, List<BatchRecord> records);

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
		void Operate(BatchPolicy policy, BatchRecordSequenceListener listener, List<BatchRecord> records);

		/// <summary>
		/// Asynchronously perform read/write operations on multiple keys.
		/// Create listener, call asynchronous delete and return task monitor.
		/// <para>Requires server version 6.0+</para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="writePolicy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read/write operations on record</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		Task<BatchResults> Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, CancellationToken token, Key[] keys, params Operation[] ops);

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
		/// <param name="ops">array of read/write operations on record</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, BatchRecordArrayListener listener, Key[] keys, params Operation[] ops);

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
		/// <param name="ops">array of read operations on record</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		void Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, BatchRecordSequenceListener listener, Key[] keys, params Operation[] ops);

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
		void ScanAll(ScanPolicy policy, RecordSequenceListener listener, string ns, string setName, params string[] binNames);
        
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
		void ScanPartitions(ScanPolicy policy, RecordSequenceListener listener, PartitionFilter partitionFilter, string ns, string setName, params string[] binNames);

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
        Task<object> Execute(WritePolicy policy, CancellationToken token, Key key, string packageName, string functionName, params Value[] functionArgs);

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
		void Execute(WritePolicy policy, ExecuteListener listener, Key key, string packageName, string functionName, params Value[] functionArgs);

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
		Task<BatchResults> Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, CancellationToken token, Key[] keys, string packageName, string functionName, params Value[] functionArgs);

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
		void Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, BatchRecordArrayListener listener, Key[] keys, string packageName, string functionName, params Value[] functionArgs);

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
		void Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, BatchRecordSequenceListener listener, Key[] keys, string packageName, string functionName, params Value[] functionArgs);

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
		void Query(QueryPolicy policy, RecordSequenceListener listener, Statement statement);

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
		void QueryPartitions
		(
			QueryPolicy policy,
			RecordSequenceListener listener,
			Statement statement,
			PartitionFilter partitionFilter
		);
	}
}
