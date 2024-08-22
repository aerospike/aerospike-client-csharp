/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using System.Reflection;

namespace Aerospike.Client
{
	public interface IAerospikeClient
	{
		//-------------------------------------------------------
		// Operations policies
		//-------------------------------------------------------

		/// <summary>
		/// Default read policy that is used when read command policy is null.
		/// </summary>
		Policy ReadPolicyDefault { get; set; }

		/// <summary>
		/// Default write policy that is used when write command policy is null.
		/// </summary>
		WritePolicy WritePolicyDefault { get; set; }

		/// <summary>
		/// Default scan policy that is used when scan command policy is null.
		/// </summary>
		ScanPolicy ScanPolicyDefault { get; set; }

		/// <summary>
		/// Default query policy that is used when query command policy is null.
		/// </summary>
		QueryPolicy QueryPolicyDefault { get; set; }

		/// <summary>
		/// Default parent policy used in batch read commands.Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		BatchPolicy BatchPolicyDefault { get; set; }

		/// <summary>
		/// Default parent policy used in batch write commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		BatchPolicy BatchParentPolicyWriteDefault { get; set; }

		/// <summary>
		/// Default write policy used in batch operate commands.
		/// Write policy fields include generation, expiration, durableDelete, etc...
		/// </summary>
		BatchWritePolicy BatchWritePolicyDefault { get; set; }

		/// <summary>
		/// Default delete policy used in batch delete commands.
		/// </summary>
		BatchDeletePolicy BatchDeletePolicyDefault { get; set; }

		/// <summary>
		/// Default user defined function policy used in batch UDF excecute commands.
		/// </summary>
		BatchUDFPolicy BatchUDFPolicyDefault { get; set; }

		/// <summary>
		/// Default multi-record transactions (MRT) policy when verifying record versions in a batch on a commit.
		/// </summary>
		TxnVerifyPolicy TxnVerifyPolicyDefault { get; set; }

		/// <summary>
		/// Default multi-record transactions (MRT) policy when rolling the transaction records forward (commit)
	    /// or back(abort) in a batch.
		/// </summary>
		TxnRollPolicy TxnRollPolicyDefault { get; set; }

		/// <summary>
		/// Default info policy that is used when info command policy is null.
		/// </summary>
		InfoPolicy InfoPolicyDefault { get; set; }

		//-------------------------------------------------------
		// Cluster Connection Management
		//-------------------------------------------------------

		/// <summary>
		/// Close all client connections to database server nodes.
		/// </summary>
		void Close();

		/// <summary>
		/// Return if we are ready to talk to the database server cluster.
		/// </summary>
		bool Connected { get; }

		/// <summary>
		/// Cluster associated with this AerospikeClient instance.
		/// </summary>
		Cluster Cluster { get; }

		/// <summary>
		/// Return array of active server nodes in the cluster.
		/// </summary>
		Node[] Nodes { get; }


		/// <summary>
		/// Enable extended periodic cluster and node latency metrics.
		/// </summary>
		void EnableMetrics(MetricsPolicy metricsPolicy);

		/// <summary>
		/// Disable extended periodic cluster and node latency metrics.
		/// </summary>
		void DisableMetrics();

		/// <summary>
		/// Return operating cluster statistics snapshot.
		/// </summary>
		ClusterStats GetClusterStats();

		//-------------------------------------------------------
		// Multi-Record Transactions
		//-------------------------------------------------------

		/// <summary>
		/// Attempt to commit the given multi-record transaction. First, the expected record versions are
		/// sent to the server nodes for verification.If all nodes return success, the command is
		/// committed.Otherwise, the command is aborted.
		/// <p>
		/// Requires server version 8.0+
		/// </p>
		/// </summary>
		/// <param name="txn">multi-record transaction</param>
		CommitStatus.CommitStatusType Commit(Txn txn);

		/// <summary>
		/// Abort and rollback the given multi-record transaction.
		/// <p>
		/// Requires server version 8.0+
		/// </p>
		/// </summary>
		/// <param name="txn">multi-record transaction</param>
		AbortStatus.AbortStatusType Abort(Txn txn);

		//-------------------------------------------------------
		// Write Record Operations
		//-------------------------------------------------------

		/// <summary>
		/// Write record bin(s).
		/// The policy specifies the command timeout, record expiration and how the command is
		/// handled when the record already exists.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if write fails</exception>
		void Put(WritePolicy policy, Key key, params Bin[] bins);

		//-------------------------------------------------------
		// String Operations
		//-------------------------------------------------------

		/// <summary>
		/// Append bin string values to existing record bin values.
		/// The policy specifies the command timeout, record expiration and how the command is
		/// handled when the record already exists.
		/// This call only works for string values. 
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if append fails</exception>
		void Append(WritePolicy policy, Key key, params Bin[] bins);

		/// <summary>
		/// Prepend bin string values to existing record bin values.
		/// The policy specifies the command timeout, record expiration and how the command is
		/// handled when the record already exists.
		/// This call works only for string values. 
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs </param>
		/// <exception cref="AerospikeException">if prepend fails</exception>
		void Prepend(WritePolicy policy, Key key, params Bin[] bins);

		//-------------------------------------------------------
		// Arithmetic Operations
		//-------------------------------------------------------

		/// <summary>
		/// Add integer bin values to existing record bin values.
		/// The policy specifies the command timeout, record expiration and how the command is
		/// handled when the record already exists.
		/// This call only works for integer values. 
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if add fails</exception>
		void Add(WritePolicy policy, Key key, params Bin[] bins);

		//-------------------------------------------------------
		// Delete Operations
		//-------------------------------------------------------

		/// <summary>
		/// Delete record for specified key.
		/// Return whether record existed on server before deletion.
		/// The policy specifies the command timeout.
		/// </summary>
		/// <param name="policy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if delete fails</exception>
		bool Delete(WritePolicy policy, Key key);

		/// <summary>
		/// Delete records for specified keys. If a key is not found, the corresponding result
		/// <see cref="BatchRecord.resultCode"/> will be <see cref="ResultCode.KEY_NOT_FOUND_ERROR"/>.
		/// <para>
		/// Requires server version 6.0+
		/// </para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="deletePolicy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchRecordArray">which contains results for keys that did complete</exception>
		BatchResults Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys);

		/// <summary>
		/// Remove records in specified namespace/set efficiently.  This method is many orders of magnitude 
		/// faster than deleting records one at a time.
		/// <para>
		/// See <a href="https://www.aerospike.com/docs/reference/info#truncate">https://www.aerospike.com/docs/reference/info#truncate</a>
		/// </para>
		/// <para>
		/// This asynchronous server call may return before the truncation is complete.  The user can still
		/// write new records after the server returns because new records will have last update times
		/// greater than the truncate cutoff (set at the time of truncate call).
		/// </para>
		/// </summary>
		/// <param name="policy">info command configuration parameters, pass in null for defaults</param>
		/// <param name="ns">required namespace</param>
		/// <param name="set">optional set name.  Pass in null to delete all sets in namespace.</param>
		/// <param name="beforeLastUpdate">
		/// optionally delete records before record last update time.
		/// If specified, value must be before the current time.
		/// Pass in null to delete all records in namespace/set regardless of last update time.
		/// </param>
		void Truncate(InfoPolicy policy, string ns, string set, DateTime? beforeLastUpdate);

		//-------------------------------------------------------
		// Touch Operations
		//-------------------------------------------------------

		/// <summary>
		/// Reset record's time to expiration using the policy's expiration.
		/// Fail if the record does not exist.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if touch fails</exception>
		void Touch(WritePolicy policy, Key key);

		//-------------------------------------------------------
		// Existence-Check Operations
		//-------------------------------------------------------

		/// <summary>
		/// Determine if a record key exists.
		/// Return whether record exists or not.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		bool Exists(Policy policy, Key key);

		/// <summary>
		/// Check if multiple record keys exist in one batch call.
		/// The returned boolean array is in positional order with the original key array order.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchExists">which contains results for keys that did complete</exception>
		bool[] Exists(BatchPolicy policy, Key[] keys);

		//-------------------------------------------------------
		// Read Record Operations
		//-------------------------------------------------------

		/// <summary>
		/// Read entire record for specified key.
		/// If found, return record instance.  If not found, return null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults </param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		Record Get(Policy policy, Key key);

		/// <summary>
		/// Read record header and bins for specified key.
		/// If found, return record instance.  If not found, return null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		Record Get(Policy policy, Key key, params string[] binNames);

		/// <summary>
		/// Read record generation and expiration only for specified key.  Bins are not read.
		/// If found, return record instance.  If not found, return null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		Record GetHeader(Policy policy, Key key);

		//-------------------------------------------------------
		// Batch Read Operations
		//-------------------------------------------------------

		/// <summary>
		/// Read multiple records for specified batch keys in one batch call.
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// The returned records are located in the same list.
		/// If the BatchRead key field is not found, the corresponding record field will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="records">list of unique record identifiers and the bins to retrieve.
		/// The returned records are located in the same list.</param>
		/// <returns>true if all batch key requests succeeded</returns>
		/// <exception cref="AerospikeException">if read fails</exception>
		bool Get(BatchPolicy policy, List<BatchRead> records);

		/// <summary>
		/// Read multiple records for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		Record[] Get(BatchPolicy policy, Key[] keys);

		/// <summary>
		/// Read multiple record headers and bins for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		Record[] Get(BatchPolicy policy, Key[] keys, params string[] binNames);

		/// <summary>
		/// Read multiple records for specified keys using read operations in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read operations on record</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		Record[] Get(BatchPolicy policy, Key[] keys, params Operation[] ops);

		/// <summary>
		/// Read multiple record header data for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		Record[] GetHeader(BatchPolicy policy, Key[] keys);

		//-------------------------------------------------------
		// Join methods
		//-------------------------------------------------------

		/// <summary>
		/// Read specified bins in left record and then join with right records.  Each join bin name
		/// (Join.leftKeysBinName) must exist in the left record.  The join bin must contain a list of 
		/// keys. Those key are used to retrieve other records using a separate batch get.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique main record identifier</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <param name="joins">array of join definitions</param>
		/// <exception cref="AerospikeException">if main read or join reads fail</exception>
		Record Join(BatchPolicy policy, Key key, string[] binNames, params Join[] joins);

		/// <summary>
		/// Read all bins in left record and then join with right records.  Each join bin name
		/// (Join.binNameKeys) must exist in the left record.  The join bin must contain a list of 
		/// keys. Those key are used to retrieve other records using a separate batch get.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique main record identifier</param>
		/// <param name="joins">array of join definitions</param>
		/// <exception cref="AerospikeException">if main read or join reads fail</exception>
		Record Join(BatchPolicy policy, Key key, params Join[] joins);

		//-------------------------------------------------------
		// Generic Database Operations
		//-------------------------------------------------------

		/// <summary>
		/// Perform multiple read/write operations on a single key in one batch call.
		/// An example would be to add an integer value to an existing record and then
		/// read the result, all in one database call.
		/// <para>
		/// The server executes operations in the same order as the operations array.
		/// Both scalar bin operations (Operation) and CDT bin operations (ListOperation,
		/// MapOperation) can be performed in same call.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="operations">database operations to perform</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		Record Operate(WritePolicy policy, Key key, params Operation[] operations);

		//-------------------------------------------------------
		// Batch Read/Write Operations
		//-------------------------------------------------------

		/// <summary>
		/// Read/Write multiple records for specified batch keys in one batch call.
		/// This method allows different namespaces/bins for each key in the batch.
		/// The returned records are located in the same list.
		/// <para>
		/// <see cref="BatchRecord"/> can be <see cref="BatchRead"/>, <see cref="BatchWrite"/>, <see cref="BatchDelete"/> or
		/// <see cref="BatchUDF"/>.
		/// </para>
		/// <para>
		/// Requires server version 6.0+
		/// </para>
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="records">list of unique record identifiers and read/write operations</param>
		/// <returns>true if all batch sub-commands succeeded</returns>
		/// <exception cref="AerospikeException">if command fails</exception>
		bool Operate(BatchPolicy policy, List<BatchRecord> records);

		/// <summary>
		/// Perform read/write operations on multiple keys. If a key is not found, the corresponding result
		/// <see cref="BatchRecord.resultCode"/> will be <see cref="ResultCode.KEY_NOT_FOUND_ERROR"/>.
		/// <para>
		/// Requires server version 6.0+
		/// </para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="writePolicy">write configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">database operations to perform</param>
		/// <exception cref="AerospikeException.BatchRecordArray">which contains results for keys that did complete</exception>
		BatchResults Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, params Operation[] ops);

		//-------------------------------------------------------
		// Scan Operations
		//-------------------------------------------------------

		/// <summary>
		/// Read all records in specified namespace and set.  If the policy's 
		/// concurrentNodes is specified, each server node will be read in
		/// parallel.  Otherwise, server nodes are read in series.
		/// <para>
		/// This call will block until the scan is complete - callbacks are made
		/// within the scope of this call.
		/// </para>
		/// </summary>
		/// <param name="policy">scan configuration parameters, pass in null for defaults</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="callback">read callback method - called with record data</param>
		/// <param name="binNames">optional bin to retrieve. All bins will be returned if not specified.</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		void ScanAll(ScanPolicy policy, string ns, string setName, ScanCallback callback, params string[] binNames);

		/// <summary>
		/// Read all records in specified namespace and set for one node only.
		/// The node is specified by name.
		/// <para>
		/// This call will block until the scan is complete - callbacks are made
		/// within the scope of this call.
		/// </para>
		/// </summary>
		/// <param name="policy">scan configuration parameters, pass in null for defaults</param>
		/// <param name="nodeName">server node name</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="callback">read callback method - called with record data</param>
		/// <param name="binNames">optional bin to retrieve. All bins will be returned if not specified.</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		void ScanNode(ScanPolicy policy, string nodeName, string ns, string setName, ScanCallback callback, params string[] binNames);

		/// <summary>
		/// Read all records in specified namespace and set for one node only.
		/// <para>
		/// This call will block until the scan is complete - callbacks are made
		/// within the scope of this call.
		/// </para>
		/// </summary>
		/// <param name="policy">scan configuration parameters, pass in null for defaults</param>
		/// <param name="node">server node</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="callback">read callback method - called with record data</param>
		/// <param name="binNames">optional bin to retrieve. All bins will be returned if not specified.</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		void ScanNode(ScanPolicy policy, Node node, string ns, string setName, ScanCallback callback, params string[] binNames);

		/// <summary>
		/// Read records in specified namespace, set and partition filter.
		/// <para>
		/// This call will block until the scan is complete - callbacks are made
		/// within the scope of this call.
		/// </para>
		/// </summary>
		/// <param name="policy">scan configuration parameters, pass in null for defaults</param>
		/// <param name="partitionFilter">filter on a subset of data partitions</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="callback">read callback method - called with record data</param>
		/// <param name="binNames">optional bin to retrieve. All bins will be returned if not specified.</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		void ScanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, string ns, string setName, ScanCallback callback, params string[] binNames);

		//---------------------------------------------------------------
		// User defined functions
		//---------------------------------------------------------------

		/// <summary>
		/// Register package located in a file containing user defined functions with server.
		/// This asynchronous server call will return before command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// RegisterTask instance.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="clientPath">path of client file containing user defined functions, relative to current directory</param>
		/// <param name="serverPath">path to store user defined functions on the server, relative to configured script directory.</param>
		/// <param name="language">language of user defined functions</param>
		/// <exception cref="AerospikeException">if register fails</exception>
		RegisterTask Register(Policy policy, string clientPath, string serverPath, Language language);

		/// <summary>
		/// Register package located in a resource containing user defined functions with server.
		/// This asynchronous server call will return before command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// RegisterTask instance.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="resourceAssembly">assembly where resource is located.  Current assembly can be obtained by: Assembly.GetExecutingAssembly()</param>
		/// <param name="resourcePath">namespace path where Lua resource is located.  Example: Aerospike.Client.Resources.mypackage.lua</param>
		/// <param name="serverPath">path to store user defined functions on the server, relative to configured script directory.</param>
		/// <param name="language">language of user defined functions</param>
		/// <exception cref="AerospikeException">if register fails</exception>
		RegisterTask Register(Policy policy, Assembly resourceAssembly, string resourcePath, string serverPath, Language language);

		/// <summary>
		/// Register UDF functions located in a code string with server. Example:
		/// <code>
		/// String code = @"
		/// local function reducer(val1,val2)
		///	  return val1 + val2
		/// end
		///
		/// function sum_single_bin(stream,name)
		///   local function mapper(rec)
		///     return rec[name]
		///   end
		///   return stream : map(mapper) : reduce(reducer)
		/// end
		///";
		///
		///	client.RegisterUdfString(null, code, "mysum.lua", Language.LUA);
		/// </code>
		/// <para>
		/// This asynchronous server call will return before command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// RegisterTask instance.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="code">code string containing user defined functions</param>
		/// <param name="serverPath">path to store user defined functions on the server, relative to configured script directory.</param>
		/// <param name="language">language of user defined functions</param>
		/// <exception cref="AerospikeException">if register fails</exception>
		RegisterTask RegisterUdfString(Policy policy, string code, string serverPath, Language language);

		/// <summary>
		/// Remove user defined function from server nodes.
		/// </summary>
		/// <param name="policy">info configuration parameters, pass in null for defaults</param>
		/// <param name="serverPath">location of UDF on server nodes.  Example: mylua.lua </param>
		/// <exception cref="AerospikeException">if remove fails</exception>
		void RemoveUdf(InfoPolicy policy, string serverPath);

		/// <summary>
		/// Execute user defined function on server and return results.
		/// The function operates on a single record.
		/// The package name is used to locate the udf file location:
		/// <para>
		/// udf file = &lt;server udf dir&gt;/&lt;package name&gt;.lua
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="args">arguments passed in to user defined function</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		object Execute(WritePolicy policy, Key key, string packageName, string functionName, params Value[] args);

		/// <summary>
		/// Execute user defined function on server for each key and return results.
		/// The package name is used to locate the udf file location:
		/// <para>
		/// udf file = &lt;server udf dir&gt;/&lt;package name&gt;.lua
		/// </para>
		/// <para>
		/// Requires server version 6.0+
		/// </para>
		/// </summary>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="udfPolicy">udf configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="functionArgs">arguments passed in to user defined function</param>
		/// <exception cref="AerospikeException.BatchRecordArray">which contains results for keys that did complete</exception>
		BatchResults Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, string packageName, string functionName, params Value[] functionArgs);

		//----------------------------------------------------------
		// Query/Execute
		//----------------------------------------------------------

		/// <summary>
		/// Apply user defined function on records that match the background query statement filter.
		/// Records are not returned to the client.
		/// This asynchronous server call will return before the command is complete.  
		/// The user can optionally wait for command completion by using the returned 
		/// ExecuteTask instance.
		/// </summary>
		/// <param name="policy">configuration parameters, pass in null for defaults</param>
		/// <param name="statement">background query definition</param>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">function name</param>
		/// <param name="functionArgs">to pass to function name, if any</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		ExecuteTask Execute(WritePolicy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs);

		/// <summary>
		/// Apply operations on records that match the background query statement filter.
		/// Records are not returned to the client.
		/// This asynchronous server call will return before the command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// ExecuteTask instance.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="statement">background query definition</param>
		/// <param name="operations">list of operations to be performed on selected records</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		ExecuteTask Execute(WritePolicy policy, Statement statement, params Operation[] operations);

		//--------------------------------------------------------
		// Query functions
		//--------------------------------------------------------

		/// <summary>
		/// Execute query and call action for each record returned from server.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">query definition</param>
		/// <param name="action">action methods to be called for each record</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		void Query(QueryPolicy policy, Statement statement, Action<Key, Record> action);

		/// <summary>
		/// Execute query and return record iterator.  The query executor puts records on a queue in 
		/// separate threads.  The calling thread concurrently pops records off the queue through the 
		/// record iterator.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">query definition</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		RecordSet Query(QueryPolicy policy, Statement statement);

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
		/// <param name="statement">query definition</param>
		/// <param name="listener">where to send results</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		void Query(QueryPolicy policy, Statement statement, QueryListener listener);

		/// <summary>
		/// Execute query for specified partitions and return records via the listener. This method will
		/// block until the query is complete. Listener callbacks are made within the scope of this call.
		/// <para>
		/// If <see cref="QueryPolicy.maxConcurrentNodes"/> is not 1, the supplied listener must handle
		/// shared data in a thread-safe manner, because the listener will be called by multiple query
		/// threads (one thread per node) in parallel.
		/// </para>
		/// <para>
		/// The completion status of all partitions is stored in the partitionFilter when the query terminates.
		/// This partitionFilter can then be used to resume an incomplete query at a later time.
		/// This is the preferred method for query terminate/resume functionality.
		/// </para>
		/// <para>
		/// Requires server version 6.0+ if using a secondary index query.
		/// </para>
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="statement">query definition</param>
		/// <param name="partitionFilter">
		/// data partition filter. Set to <see cref="PartitionFilter.All"/> for all partitions.
		/// </param>
		/// <param name="listener">where to send results</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		void Query
		(
			QueryPolicy policy,
			Statement statement,
			PartitionFilter partitionFilter,
			QueryListener listener
		);

		/// <summary>
		/// Execute query for specified partitions and return record iterator.  The query executor puts
		/// records on a queue in separate threads.  The calling thread concurrently pops records off
		/// the queue through the record iterator.
		/// <para>
		/// Requires server version 6.0+ if using a secondary index query.
		/// </para>
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="statement">query definition</param>
		/// <param name="partitionFilter">filter on a subset of data partitions</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		RecordSet QueryPartitions
		(
			QueryPolicy policy,
			Statement statement,
			PartitionFilter partitionFilter
		);

		/// <summary>
		/// Execute query, apply statement's aggregation function, and return result iterator. 
		/// The aggregation function should be located in a Lua script file that can be found from the 
		/// "LuaConfig.PackagePath" paths static variable.  The default package path is "udf/?.lua"
		/// where "?" is the packageName.
		/// <para>
		/// The query executor puts results on a queue in separate threads.  The calling thread 
		/// concurrently pops results off the queue through the ResultSet iterator.
		/// The aggregation function is called on both server and client (final reduce).
		/// Therefore, the Lua script file must also reside on both server and client.
		/// </para>
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="statement">query definition</param>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		ResultSet QueryAggregate
		(
			QueryPolicy policy,
			Statement statement,
			string packageName,
			string functionName,
			params Value[] functionArgs
		);

		/// <summary>
		/// Execute query, apply statement's aggregation function, call action for each aggregation
		/// object returned from server. 
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="statement">
		/// query definition with aggregate functions already initialized by SetAggregateFunction().
		/// </param>
		/// <param name="action">action methods to be called for each aggregation object</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		void QueryAggregate(QueryPolicy policy, Statement statement, Action<Object> action);

		/// <summary>
		/// Execute query, apply statement's aggregation function, and return result iterator. 
		/// The aggregation function should be initialized via the statement's SetAggregateFunction()
		/// and should be located in a Lua resource file located in an assembly.
		/// <para>
		/// The query executor puts results on a queue in separate threads.  The calling thread 
		/// concurrently pops results off the queue through the ResultSet iterator.
		/// The aggregation function is called on both server and client (final reduce).
		/// Therefore, the Lua script file must also reside on both server and client.
		/// </para>
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="statement">
		/// query definition with aggregate functions already initialized by SetAggregateFunction().
		/// </param>
		/// <exception cref="AerospikeException">if query fails</exception>
		ResultSet QueryAggregate(QueryPolicy policy, Statement statement);

		/// <summary>
		/// Create scalar secondary index.
		/// This asynchronous server call will return before command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// IndexTask instance.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="indexName">name of secondary index</param>
		/// <param name="binName">bin name that data is indexed on</param>
		/// <param name="indexType">underlying data type of secondary index</param>
		/// <exception cref="AerospikeException">if index create fails</exception>
		IndexTask CreateIndex
		(
			Policy policy,
			string ns,
			string setName,
			string indexName,
			string binName,
			IndexType indexType
		);

		/// <summary>
		/// Create complex secondary index on bins containing collections.
		/// This asynchronous server call will return before command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// IndexTask instance.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="indexName">name of secondary index</param>
		/// <param name="binName">bin name that data is indexed on</param>
		/// <param name="indexType">underlying data type of secondary index</param>
		/// <param name="indexCollectionType">index collection type</param>
		/// <param name="ctx">optional context to index on elements within a CDT</param>
		/// <exception cref="AerospikeException">if index create fails</exception>
		IndexTask CreateIndex
		(
			Policy policy,
			string ns,
			string setName,
			string indexName,
			string binName,
			IndexType indexType,
			IndexCollectionType indexCollectionType,
			params CTX[] ctx
		);

		/// <summary>
		/// Delete secondary index.
		/// This asynchronous server call will return before command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// IndexTask instance.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="indexName">name of secondary index</param>
		/// <exception cref="AerospikeException">if index drop fails</exception>
		IndexTask DropIndex(Policy policy, string ns, string setName, string indexName);

		//-----------------------------------------------------------------
		// XDR - Cross datacenter replication
		//-----------------------------------------------------------------

		/// <summary>
		/// Set XDR filter for given datacenter name and namespace. The expression filter indicates
		/// which records XDR should ship to the datacenter.
		/// </summary>
		/// <param name="policy">info configuration parameters, pass in null for defaults</param>
		/// <param name="datacenter">XDR datacenter name</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="filter">expression filter</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		void SetXDRFilter(InfoPolicy policy, string datacenter, string ns, Expression filter);

		//-------------------------------------------------------
		// User administration
		//-------------------------------------------------------

		/// <summary>
		/// Create user with password and roles.  Clear-text password will be hashed using bcrypt 
		/// before sending to server.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="password">user password in clear-text format</param>
		/// <param name="roles">variable arguments array of role names.  Predefined roles are listed in Role.cs</param>		
		void CreateUser(AdminPolicy policy, string user, string password, IList<string> roles);

		/// <summary>
		/// Remove user from cluster.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		void DropUser(AdminPolicy policy, string user);

		/// <summary>
		/// Change user's password.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="password">user password in clear-text format</param>
		void ChangePassword(AdminPolicy policy, string user, string password);

		/// <summary>
		/// Add roles to user's list of roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="roles">role names.  Predefined roles are listed in Role.cs</param>
		void GrantRoles(AdminPolicy policy, string user, IList<string> roles);

		/// <summary>
		/// Remove roles from user's list of roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="roles">role names.  Predefined roles are listed in Role.cs</param>
		void RevokeRoles(AdminPolicy policy, string user, IList<string> roles);

		/// <summary>
		/// Create user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <exception cref="AerospikeException">if command fails </exception>
		void CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges);

		/// <summary>
		/// Create user defined role with optional privileges and whitelist.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">optional list of privileges assigned to role.</param>
		/// <param name="whitelist">
		/// optional list of allowable IP addresses assigned to role.
		/// IP addresses can contain wildcards (ie. 10.1.2.0/24).
		/// </param>
		/// <exception cref="AerospikeException">if command fails</exception>
		void CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges, IList<string> whitelist);

		/// <summary>
		/// Create user defined role with optional privileges, whitelist and read/write quotas.
		/// Quotas require server security configuration "enable-quotas" to be set to true.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">optional list of privileges assigned to role.</param>
		/// <param name="whitelist">
		/// optional list of allowable IP addresses assigned to role.
		/// IP addresses can contain wildcards (ie. 10.1.2.0/24).
		/// </param>
		/// <param name="readQuota">optional maximum reads per second limit, pass in zero for no limit.</param>
		/// <param name="writeQuota">optional maximum writes per second limit, pass in zero for no limit.</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		void CreateRole
		(
			AdminPolicy policy,
			string roleName,
			IList<Privilege> privileges,
			IList<string> whitelist,
			int readQuota,
			int writeQuota
		);

		/// <summary>
		/// Drop user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		void DropRole(AdminPolicy policy, string roleName);

		/// <summary>
		/// Grant privileges to an user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		void GrantPrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges);

		/// <summary>
		/// Revoke privileges from an user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		void RevokePrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges);

		/// <summary>
		/// Set IP address whitelist for a role.  If whitelist is null or empty, remove existing whitelist from role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="whitelist">
		/// list of allowable IP addresses or null.
		/// IP addresses can contain wildcards (ie. 10.1.2.0/24).
		/// </param>
		/// <exception cref="AerospikeException">if command fails</exception>
		void SetWhitelist(AdminPolicy policy, string roleName, IList<string> whitelist);

		/// <summary>
		/// Set maximum reads/writes per second limits for a role.  If a quota is zero, the limit is removed.
		/// Quotas require server security configuration "enable-quotas" to be set to true.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="readQuota">maximum reads per second limit, pass in zero for no limit.</param>
		/// <param name="writeQuota">maximum writes per second limit, pass in zero for no limit.</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		void SetQuotas(AdminPolicy policy, string roleName, int readQuota, int writeQuota);

		/// <summary>
		/// Retrieve roles for a given user.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name filter</param>
		User QueryUser(AdminPolicy policy, string user);

		/// <summary>
		/// Retrieve all users and their roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		List<User> QueryUsers(AdminPolicy policy);

		/// <summary>
		/// Retrieve role definition.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name filter</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		Role QueryRole(AdminPolicy policy, string roleName);

		/// <summary>
		/// Retrieve all roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		List<Role> QueryRoles(AdminPolicy policy);
	}
}