/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System.Collections.Generic;

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
	public class AsyncClient : AerospikeClient
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
		/// This method schedules the put command with a channel selector and returns.
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
			AsyncWrite async = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.WRITE);
			async.Execute();
		}

		//-------------------------------------------------------
		// String Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously append bin string values to existing record bin values.
		/// This method schedules the append command with a channel selector and returns.
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
			AsyncWrite async = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.APPEND);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously prepend bin string values to existing record bin values.
		/// This method schedules the prepend command with a channel selector and returns.
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
			AsyncWrite async = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.PREPEND);
			async.Execute();
		}

		//-------------------------------------------------------
		// Arithmetic Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously add integer bin values to existing record bin values.
		/// This method schedules the add command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call only works for integer values. 
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Add(WritePolicy policy, WriteListener listener, Key key, params Bin[] bins)
		{
			AsyncWrite async = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.ADD);
			async.Execute();
		}

		//-------------------------------------------------------
		// Delete Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously delete record for specified key.
		/// This method schedules the delete command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy specifies the transaction timeout.
		/// </para>
		/// </summary>
		/// <param name="policy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Delete(WritePolicy policy, DeleteListener listener, Key key)
		{
			AsyncDelete async = new AsyncDelete(cluster, policy, key, listener);
			async.Execute();
		}

		//-------------------------------------------------------
		// Touch Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously create record if it does not already exist.  If the record exists, the record's 
		/// time to expiration will be reset to the policy's expiration.
		/// <para>
		/// This method schedules the touch command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Touch(WritePolicy policy, WriteListener listener, Key key)
		{
			AsyncTouch async = new AsyncTouch(cluster, policy, listener, key);
			async.Execute();
		}

		//-------------------------------------------------------
		// Existence-Check Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously determine if a record key exists.
		/// This method schedules the exists command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Exists(Policy policy, ExistsListener listener, Key key)
		{
			AsyncExists async = new AsyncExists(cluster, policy, key, listener);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// This method schedules the exists command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Exists(Policy policy, ExistsArrayListener listener, Key[] keys)
		{
			new AsyncBatchExistsArrayExecutor(cluster, policy, keys, listener);
		}

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// This method schedules the exists command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// <para>
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Exists(Policy policy, ExistsSequenceListener listener, Key[] keys)
		{
			new AsyncBatchExistsSequenceExecutor(cluster, policy, keys, listener);
		}

		//-------------------------------------------------------
		// Read Record Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously read entire record for specified key.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(Policy policy, RecordListener listener, Key key)
		{
			AsyncRead async = new AsyncRead(cluster, policy, listener, key, null);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously read record header and bins for specified key.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(Policy policy, RecordListener listener, Key key, params string[] binNames)
		{
			AsyncRead async = new AsyncRead(cluster, policy, listener, key, binNames);
			async.Execute();
		}

		/// <summary>
		/// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// <para>
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void GetHeader(Policy policy, RecordListener listener, Key key)
		{
			AsyncReadHeader async = new AsyncReadHeader(cluster, policy, listener, key);
			async.Execute();
		}

		//-------------------------------------------------------
		// Batch Read Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously read multiple records for specified keys in one batch call.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(Policy policy, RecordArrayListener listener, Key[] keys)
		{
			new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
		}

		/// <summary>
		/// Asynchronously read multiple records for specified keys in one batch call.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// <para>
		/// If a key is not found, the record will be null.
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(Policy policy, RecordSequenceListener listener, Key[] keys)
		{
			new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
		}

		/// <summary>
		/// Asynchronously read multiple record headers and bins for specified keys in one batch call.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(Policy policy, RecordArrayListener listener, Key[] keys, params string[] binNames)
		{
			HashSet<string> names = BinNamesToHashSet(binNames);
			new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, names, Command.INFO1_READ);
		}

		/// <summary>
		/// Asynchronously read multiple record headers and bins for specified keys in one batch call.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// <para>
		/// If a key is not found, the record will be null.
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Get(Policy policy, RecordSequenceListener listener, Key[] keys, params string[] binNames)
		{
			HashSet<string> names = BinNamesToHashSet(binNames);
			new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, names, Command.INFO1_READ);
		}

		/// <summary>
		/// Asynchronously read multiple record header data for specified keys in one batch call.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in a single call.
		/// <para>
		/// If a key is not found, the record will be null.
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void GetHeader(Policy policy, RecordArrayListener listener, Key[] keys)
		{
			new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		/// <summary>
		/// Asynchronously read multiple record header data for specified keys in one batch call.
		/// This method schedules the get command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener in multiple unordered calls.
		/// <para>
		/// If a key is not found, the record will be null.
		/// The policy can be used to specify timeouts.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void GetHeader(Policy policy, RecordSequenceListener listener, Key[] keys)
		{
			new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		//-------------------------------------------------------
		// Generic Database Operations
		//-------------------------------------------------------

		/// <summary>
		/// Asynchronously perform multiple read/write operations on a single key in one batch call.
		/// An example would be to add an integer value to an existing record and then
		/// read the result, all in one database call.
		/// <para>
		/// This method schedules the operate command with a channel selector and returns.
		/// Another thread will process the command and send the results to the listener.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="operations">database operations to perform</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void Operate(WritePolicy policy, RecordListener listener, Key key, params Operation[] operations)
		{
			AsyncOperate async = new AsyncOperate(cluster, policy, listener, key, operations);
			async.Execute();
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
		/// <param name="binNames">
		/// optional bin to retrieve. All bins will be returned if not specified.
		/// Aerospike 2 servers ignore this parameter.
		/// </param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public void ScanAll(ScanPolicy policy, RecordSequenceListener listener, string ns, string setName, params string[] binNames)
		{
			if (policy == null)
			{
				policy = new ScanPolicy();
			}

			// Retry policy must be one-shot for scans.
			policy.maxRetries = 0;
			new AsyncScanExecutor(cluster, policy, listener, ns, setName, binNames);
		}
	}
}
