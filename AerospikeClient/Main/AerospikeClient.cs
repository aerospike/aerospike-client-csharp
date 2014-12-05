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
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Aerospike.Client
{
	/// <summary>
	/// Instantiate an AerospikeClient object to access an Aerospike
	/// database cluster and perform database operations.
	/// <para>
	/// This client is thread-safe. One client instance should be used per cluster.
	/// Multiple threads should share this cluster instance.
	/// </para>
	/// <para>
	/// Your application uses this class API to perform database operations such as
	/// writing and reading records, and selecting sets of records. Write operations
	/// include specialized functionality such as append/prepend and arithmetic
	/// addition.
	/// </para>
	/// <para>
	/// Each record may have multiple bins, unless the Aerospike server nodes are
	/// configured as "single-bin". In "multi-bin" mode, partial records may be
	/// written or read by specifying the relevant subset of bins.
	/// </para>
	/// </summary>
	public class AerospikeClient : IDisposable
	{
		//-------------------------------------------------------
		// Member variables.
		//-------------------------------------------------------

		protected internal Cluster cluster;

		/// <summary>
		/// Default read policy that is used when read command policy is null.
		/// </summary>
		public readonly Policy readPolicyDefault;

		/// <summary>
		/// Default write policy that is used when write command policy is null.
		/// </summary>
		public readonly WritePolicy writePolicyDefault;

		/// <summary>
		/// Default scan policy that is used when scan command policy is null.
		/// </summary>
		public readonly ScanPolicy scanPolicyDefault;

		/// <summary>
		/// Default query policy that is used when query command policy is null.
		/// </summary>
		public readonly QueryPolicy queryPolicyDefault;

		/// <summary>
		/// Default batch policy that is used when batch command policy is null.
		/// </summary>
		public readonly BatchPolicy batchPolicyDefault;
		
		//-------------------------------------------------------
		// Constructors
		//-------------------------------------------------------

		/// <summary>
		/// Initialize Aerospike client.
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
		public AerospikeClient(string hostname, int port) 
			: this(new ClientPolicy(), new Host(hostname, port))
		{
		}

		/// <summary>
		/// Initialize Aerospike client.
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
		public AerospikeClient(ClientPolicy policy, string hostname, int port) 
			: this(policy, new Host(hostname, port))
		{
		}

		/// <summary>
		/// Initialize Aerospike client with suitable hosts to seed the cluster map.
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
		public AerospikeClient(ClientPolicy policy, params Host[] hosts)
		{
			if (policy == null)
			{
				policy = new ClientPolicy();
			}
			this.readPolicyDefault = policy.readPolicyDefault;
			this.writePolicyDefault = policy.writePolicyDefault;
			this.scanPolicyDefault = policy.scanPolicyDefault;
			this.queryPolicyDefault = policy.queryPolicyDefault;
			this.batchPolicyDefault = policy.batchPolicyDefault;

			cluster = new Cluster(policy, hosts);
			cluster.InitTendThread(policy.failIfNotConnected);
		}

		/// <summary>
		/// Construct client without initialization.
		/// Should only be used by classes inheriting from this client.
		/// </summary>
		protected internal AerospikeClient(ClientPolicy policy)
		{
			if (policy != null)
			{
				this.readPolicyDefault = policy.readPolicyDefault;
				this.writePolicyDefault = policy.writePolicyDefault;
				this.scanPolicyDefault = policy.scanPolicyDefault;
				this.queryPolicyDefault = policy.queryPolicyDefault;
				this.batchPolicyDefault = policy.batchPolicyDefault;
			}
			else
			{
				this.readPolicyDefault = new Policy();
				this.writePolicyDefault = new WritePolicy();
				this.scanPolicyDefault = new ScanPolicy();
				this.queryPolicyDefault = new QueryPolicy();
				this.batchPolicyDefault = new BatchPolicy();
			}
		}

		//-------------------------------------------------------
		// Cluster Connection Management
		//-------------------------------------------------------

		/// <summary>
		/// Close all client connections to database server nodes.
		/// </summary>
		public void Dispose()
		{
			Close();
		}

		/// <summary>
		/// Close all client connections to database server nodes.
		/// </summary>
		public void Close()
		{
			cluster.Close();
		}

		/// <summary>
		/// Return if we are ready to talk to the database server cluster.
		/// </summary>
		public bool Connected
		{
			get
			{
				return cluster.Connected;
			}
		}

		/// <summary>
		/// Return array of active server nodes in the cluster.
		/// </summary>
		public Node[] Nodes
		{
			get
			{
				return cluster.Nodes;
			}
		}

		//-------------------------------------------------------
		// Write Record Operations
		//-------------------------------------------------------

		/// <summary>
		/// Write record bin(s).
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if write fails</exception>
		public void Put(WritePolicy policy, Key key, params Bin[] bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.WRITE);
			command.Execute();
		}

		//-------------------------------------------------------
		// String Operations
		//-------------------------------------------------------

		/// <summary>
		/// Append bin string values to existing record bin values.
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call only works for string values. 
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if append fails</exception>
		public void Append(WritePolicy policy, Key key, params Bin[] bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.APPEND);
			command.Execute();
		}

		/// <summary>
		/// Prepend bin string values to existing record bin values.
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call works only for string values. 
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs </param>
		/// <exception cref="AerospikeException">if prepend fails</exception>
		public void Prepend(WritePolicy policy, Key key, params Bin[] bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.PREPEND);
			command.Execute();
		}

		//-------------------------------------------------------
		// Arithmetic Operations
		//-------------------------------------------------------

		/// <summary>
		/// Add integer bin values to existing record bin values.
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call only works for integer values. 
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if add fails</exception>
		public void Add(WritePolicy policy, Key key, params Bin[] bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.ADD);
			command.Execute();
		}

		//-------------------------------------------------------
		// Delete Operations
		//-------------------------------------------------------

		/// <summary>
		/// Delete record for specified key.
		/// Return whether record existed on server before deletion.
		/// The policy specifies the transaction timeout.
		/// </summary>
		/// <param name="policy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if delete fails</exception>
		public bool Delete(WritePolicy policy, Key key)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			DeleteCommand command = new DeleteCommand(cluster, policy, key);
			command.Execute();
			return command.Existed();
		}

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
		public void Touch(WritePolicy policy, Key key)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			TouchCommand command = new TouchCommand(cluster, policy, key);
			command.Execute();
		}

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
		public bool Exists(Policy policy, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			ExistsCommand command = new ExistsCommand(cluster, policy, key);
			command.Execute();
			return command.Exists();
		}

		/// <summary>
		/// Check if multiple record keys exist in one batch call.
		/// The returned boolean array is in positional order with the original key array order.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		[System.Obsolete("Use Exists(BatchPolicy, Key[]) instead.")]
		public bool[] Exists(Policy policy, Key[] keys)
		{
			BatchPolicy batchPolicy = (policy == null) ? batchPolicyDefault : new BatchPolicy(policy);
			return Exists(batchPolicy, keys);
		}

		/// <summary>
		/// Check if multiple record keys exist in one batch call.
		/// The returned boolean array is in positional order with the original key array order.
		/// The policy can be used to specify timeouts and maximum concurrent threads.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public bool[] Exists(BatchPolicy policy, Key[] keys)
		{
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			bool[] existsArray = new bool[keys.Length];
			BatchExecutor.Execute(cluster, policy, keys, existsArray, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			return existsArray;
		}
		
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
		public Record Get(Policy policy, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			ReadCommand command = new ReadCommand(cluster, policy, key, null);
			command.Execute();
			return command.Record;
		}

		/// <summary>
		/// Read record header and bins for specified key.
		/// If found, return record instance.  If not found, return null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public Record Get(Policy policy, Key key, params string[] binNames)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			ReadCommand command = new ReadCommand(cluster, policy, key, binNames);
			command.Execute();
			return command.Record;
		}

		/// <summary>
		/// Read record generation and expiration only for specified key.  Bins are not read.
		/// If found, return record instance.  If not found, return null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public Record GetHeader(Policy policy, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			ReadHeaderCommand command = new ReadHeaderCommand(cluster, policy, key);
			command.Execute();
			return command.Record;
		}

		//-------------------------------------------------------
		// Batch Read Operations
		//-------------------------------------------------------

		/// <summary>
		/// Read multiple records for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		[System.Obsolete("Use Get(BatchPolicy, Key[]) instead.")]
		public Record[] Get(Policy policy, Key[] keys)
		{
			BatchPolicy batchPolicy = (policy == null) ? batchPolicyDefault : new BatchPolicy(policy);
			return Get(batchPolicy, keys);
		}

		/// <summary>
		/// Read multiple records for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// The policy can be used to specify timeouts and maximum concurrent threads.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public Record[] Get(BatchPolicy policy, Key[] keys)
		{
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			Record[] records = new Record[keys.Length];
			BatchExecutor.Execute(cluster, policy, keys, null, records, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
			return records;
		}
		
		/// <summary>
		/// Read multiple record headers and bins for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		[System.Obsolete("Use Get(BatchPolicy, Key[], params string[]) instead.")]
		public Record[] Get(Policy policy, Key[] keys, params string[] binNames)
		{
			BatchPolicy batchPolicy = (policy == null) ? batchPolicyDefault : new BatchPolicy(policy);
			return Get(batchPolicy, keys, binNames);
		}

		/// <summary>
		/// Read multiple record headers and bins for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// The policy can be used to specify timeouts and maximum concurrent threads.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public Record[] Get(BatchPolicy policy, Key[] keys, params string[] binNames)
		{
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			Record[] records = new Record[keys.Length];
			HashSet<string> names = BinNamesToHashSet(binNames);
			BatchExecutor.Execute(cluster, policy, keys, null, records, names, Command.INFO1_READ);
			return records;
		}
		
		/// <summary>
		/// Read multiple record header data for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		[System.Obsolete("Use GetHeader(BatchPolicy, Key[]) instead.")]
		public Record[] GetHeader(Policy policy, Key[] keys)
		{
			BatchPolicy batchPolicy = (policy == null) ? batchPolicyDefault : new BatchPolicy(policy);
			return GetHeader(batchPolicy, keys);
		}

		/// <summary>
		/// Read multiple record header data for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// The policy can be used to specify timeouts and maximum concurrent threads.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public Record[] GetHeader(BatchPolicy policy, Key[] keys)
		{
			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			Record[] records = new Record[keys.Length];
			BatchExecutor.Execute(cluster, policy, keys, null, records, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			return records;
		}

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
		public Record Join(BatchPolicy policy, Key key, string[] binNames, params Join[] joins)
		{
			string[] names = new string[binNames.Length + joins.Length];
			int count = 0;

			foreach (string binName in binNames)
			{
				names[count++] = binName;
			}

			foreach (Join join in joins)
			{
				names[count++] = join.leftKeysBinName;
			}
			Record record = Get(policy, key, names);
			JoinRecords(policy, record, joins);
			return record;
		}

		/// <summary>
		/// Read all bins in left record and then join with right records.  Each join bin name
		/// (Join.binNameKeys) must exist in the left record.  The join bin must contain a list of 
		/// keys. Those key are used to retrieve other records using a separate batch get.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique main record identifier</param>
		/// <param name="joins">array of join definitions</param>
		/// <exception cref="AerospikeException">if main read or join reads fail</exception>
		public Record Join(BatchPolicy policy, Key key, params Join[] joins)
		{
			Record record = Get(policy, key);
			JoinRecords(policy, record, joins);
			return record;
		}

		//-------------------------------------------------------
		// Generic Database Operations
		//-------------------------------------------------------

		/// <summary>
		/// Perform multiple read/write operations on a single key in one batch call.
		/// A record will be returned if there is a read in the operations list.
		/// An example would be to add an integer value to an existing record and then
		/// read the result, all in one database call.
		/// <para>
		/// Write operations are always performed first, regardless of operation order
		/// relative to read operations.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="operations">database operations to perform</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public Record Operate(WritePolicy policy, Key key, params Operation[] operations)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			OperateCommand command = new OperateCommand(cluster, policy, key, operations);
			command.Execute();
			return command.Record;
		}

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
		/// <param name="binNames">
		/// optional bin to retrieve. All bins will be returned if not specified.
		/// Aerospike 2 servers ignore this parameter.
		/// </param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		public void ScanAll(ScanPolicy policy, string ns, string setName, ScanCallback callback, params string[] binNames)
		{
			if (policy == null)
			{
				policy = scanPolicyDefault;
			}

			Node[] nodes = cluster.Nodes;

			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.");
			}

			if (policy.concurrentNodes)
			{
				Executor executor = new Executor(nodes.Length);
				long taskId = Environment.TickCount;

				foreach (Node node in nodes)
				{
					ScanCommand command = new ScanCommand(node, policy, ns, setName, callback, binNames, taskId);
					executor.AddCommand(command);
				}

				executor.Execute(policy.maxConcurrentNodes);
			}
			else
			{
				foreach (Node node in nodes)
				{
					ScanNode(policy, node, ns, setName, callback, binNames);
				}
			}
		}

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
		/// <param name="binNames">
		/// optional bin to retrieve. All bins will be returned if not specified.
		/// Aerospike 2 servers ignore this parameter.
		/// </param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		public void ScanNode(ScanPolicy policy, string nodeName, string ns, string setName, ScanCallback callback, params string[] binNames)
		{
			Node node = cluster.GetNode(nodeName);
			ScanNode(policy, node, ns, setName, callback, binNames);
		}

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
		/// <param name="binNames">
		/// optional bin to retrieve. All bins will be returned if not specified.
		/// Aerospike 2 servers ignore this parameter.
		/// </param>
		/// <exception cref="AerospikeException">if transaction fails</exception>
		public void ScanNode(ScanPolicy policy, Node node, string ns, string setName, ScanCallback callback, params string[] binNames)
		{
			if (policy == null)
			{
				policy = scanPolicyDefault;
			}
			long taskId = Environment.TickCount;

			ScanCommand command = new ScanCommand(node, policy, ns, setName, callback, binNames, taskId);
			command.Execute();
		}

		//-------------------------------------------------------------------
		// Large collection functions (Supported by Aerospike 3 servers only)
		//-------------------------------------------------------------------

		/// <summary>
		/// Initialize large list operator.  This operator can be used to create and manage a list 
		/// within a single bin.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default</param>
		[System.Obsolete("Use GetLargeList(WritePolicy policy, Key key, string binName, string userModule) instead.")]
		public LargeList GetLargeList(Policy policy, Key key, string binName, string userModule)
		{
			WritePolicy writePolicy = (policy == null) ? writePolicyDefault : new WritePolicy(policy);
			return new LargeList(this, writePolicy, key, binName, userModule);
		}

		/// <summary>
		/// Initialize large list operator.  This operator can be used to create and manage a list 
		/// within a single bin.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default</param>
		public LargeList GetLargeList(WritePolicy policy, Key key, string binName, string userModule)
		{
			return new LargeList(this, policy, key, binName, userModule);
		}
		
		/// <summary>
		/// Initialize large map operator.  This operator can be used to create and manage a map 
		/// within a single bin.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default</param>
		[System.Obsolete("Use GetLargeMap(WritePolicy policy, Key key, string binName, string userModule) instead.")]
		public LargeMap GetLargeMap(Policy policy, Key key, string binName, string userModule)
		{
			WritePolicy writePolicy = (policy == null) ? writePolicyDefault : new WritePolicy(policy);
			return new LargeMap(this, writePolicy, key, binName, userModule);
		}

		/// <summary>
		/// Initialize large map operator.  This operator can be used to create and manage a map 
		/// within a single bin.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default</param>
		public LargeMap GetLargeMap(WritePolicy policy, Key key, string binName, string userModule)
		{
			return new LargeMap(this, policy, key, binName, userModule);
		}
		
		/// <summary>
		/// Initialize large set operator.  This operator can be used to create and manage a set 
		/// within a single bin.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default</param>
		[System.Obsolete("Use GetLargeSet(WritePolicy policy, Key key, string binName, string userModule) instead.")]
		public LargeSet GetLargeSet(Policy policy, Key key, string binName, string userModule)
		{
			WritePolicy writePolicy = (policy == null) ? writePolicyDefault : new WritePolicy(policy);
			return new LargeSet(this, writePolicy, key, binName, userModule);
		}

		/// <summary>
		/// Initialize large set operator.  This operator can be used to create and manage a set 
		/// within a single bin.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default</param>
		public LargeSet GetLargeSet(WritePolicy policy, Key key, string binName, string userModule)
		{
			return new LargeSet(this, policy, key, binName, userModule);
		}
		
		/// <summary>
		/// Initialize large stack operator.  This operator can be used to create and manage a stack 
		/// within a single bin.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default</param>
		[System.Obsolete("Use GetLargeStack(WritePolicy policy, Key key, string binName, string userModule) instead.")]
		public LargeStack GetLargeStack(Policy policy, Key key, string binName, string userModule)
		{
			WritePolicy writePolicy = (policy == null) ? writePolicyDefault : new WritePolicy(policy);
			return new LargeStack(this, writePolicy, key, binName, userModule);
		}

		/// <summary>
		/// Initialize large stack operator.  This operator can be used to create and manage a stack 
		/// within a single bin.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binName">bin name</param>
		/// <param name="userModule">Lua function name that initializes list configuration parameters, pass null for default</param>
		public LargeStack GetLargeStack(WritePolicy policy, Key key, string binName, string userModule)
		{
			return new LargeStack(this, policy, key, binName, userModule);
		}
		
		//---------------------------------------------------------------
		// User defined functions (Supported by Aerospike 3 servers only)
		//---------------------------------------------------------------

		/// <summary>
		/// Register package containing user defined functions with server.
		/// This asynchronous server call will return before command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// RegisterTask instance.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="clientPath">path of client file containing user defined functions, relative to current directory</param>
		/// <param name="serverPath">path to store user defined functions on the server, relative to configured script directory.</param>
		/// <param name="language">language of user defined functions</param>
		/// <exception cref="AerospikeException">if register fails</exception>
		public RegisterTask Register(Policy policy, string clientPath, string serverPath, Language language)
		{
			string content = Util.ReadFileEncodeBase64(clientPath);

			StringBuilder sb = new StringBuilder(serverPath.Length + content.Length + 100);
			sb.Append("udf-put:filename=");
			sb.Append(serverPath);
			sb.Append(";content=");
			sb.Append(content);
			sb.Append(";content-len=");
			sb.Append(content.Length);
			sb.Append(";udf-type=");
			sb.Append(language);
			sb.Append(";");

			// Send UDF to one node. That node will distribute the UDF to other nodes.
			string command = sb.ToString();
			Node node = cluster.GetRandomNode();
			int timeout = (policy == null) ? 0 : policy.timeout;
			Connection conn = node.GetConnection(timeout);

			try
			{
				Info info = new Info(conn, command);
				Info.NameValueParser parser = info.GetNameValueParser();
				string error = null;
				string file = null;
				string line = null;
				string message = null;

				while (parser.Next())
				{
					string name = parser.GetName();

					if (name.Equals("error"))
					{
						error = parser.GetValue();
					}
					else if (name.Equals("file"))
					{
						file = parser.GetValue();
					}
					else if (name.Equals("line"))
					{
						line = parser.GetValue();
					}
					else if (name.Equals("message"))
					{
						message = parser.GetStringBase64();
					}
				}

				if (error != null)
				{
					throw new AerospikeException("Registration failed: " + error + Environment.NewLine +
						"File: " + file + Environment.NewLine +
						"Line: " + line + Environment.NewLine +
						"Message: " + message
						);
				}
				node.PutConnection(conn);
				return new RegisterTask(cluster, serverPath);
			}
			catch (Exception)
			{
				conn.Close();
				throw;
			}
		}

		/// <summary>
		/// Execute user defined function on server and return results.
		/// The function operates on a single record.
		/// The package name is used to locate the udf file location:
		/// <para>
		/// udf file = &lt;server udf dir&gt;/&lt;package name&gt;.lua
		/// </para>
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="args">arguments passed in to user defined function</param>
		/// <exception cref="AerospikeException">if transaction fails</exception>
		[System.Obsolete("Use Execute(WritePolicy policy, Key key, string packageName, string functionName, params Value[] args) instead.")]
		public object Execute(Policy policy, Key key, string packageName, string functionName, params Value[] args)
		{
			WritePolicy writePolicy = (policy == null) ? writePolicyDefault : new WritePolicy(policy);
			return Execute(writePolicy, key, packageName, functionName, args);
		}

		/// <summary>
		/// Execute user defined function on server and return results.
		/// The function operates on a single record.
		/// The package name is used to locate the udf file location:
		/// <para>
		/// udf file = &lt;server udf dir&gt;/&lt;package name&gt;.lua
		/// </para>
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="packageName">server package name where user defined function resides</param>
		/// <param name="functionName">user defined function</param>
		/// <param name="args">arguments passed in to user defined function</param>
		/// <exception cref="AerospikeException">if transaction fails</exception>
		public object Execute(WritePolicy policy, Key key, string packageName, string functionName, params Value[] args)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			ExecuteCommand command = new ExecuteCommand(cluster, policy, key, packageName, functionName, args);
			command.Execute();

			Record record = command.Record;

			if (record == null || record.bins == null)
			{
				return null;
			}

			IDictionary<string, object> map = record.bins;
			object obj;

			if (map.TryGetValue("SUCCESS", out obj))
			{
				return obj;
			}

			if (map.TryGetValue("FAILURE", out obj))
			{
				throw new AerospikeException(obj.ToString());
			}
			throw new AerospikeException("Invalid UDF return value");
		}
		
		//----------------------------------------------------------
		// Query/Execute UDF (Supported by Aerospike 3 servers only)
		//----------------------------------------------------------

		/// <summary>
		/// Apply user defined function on records that match the statement filter.
		/// Records are not returned to the client.
		/// This asynchronous server call will return before command is complete.  
		/// The user can optionally wait for command completion by using the returned 
		/// ExecuteTask instance.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">configuration parameters, pass in null for defaults</param>
		/// <param name="statement">record filter</param>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">function name</param>
		/// <param name="functionArgs">to pass to function name, if any</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		[System.Obsolete("Use Execute(WritePolicy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs) instead.")]
		public ExecuteTask Execute(Policy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs)
		{
			WritePolicy writePolicy = (policy == null) ? writePolicyDefault : new WritePolicy(policy);
			return Execute(writePolicy, statement, packageName, functionName, functionArgs);
		}

		/// <summary>
		/// Apply user defined function on records that match the statement filter.
		/// Records are not returned to the client.
		/// This asynchronous server call will return before command is complete.  
		/// The user can optionally wait for command completion by using the returned 
		/// ExecuteTask instance.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">configuration parameters, pass in null for defaults</param>
		/// <param name="statement">record filter</param>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">function name</param>
		/// <param name="functionArgs">to pass to function name, if any</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public ExecuteTask Execute(WritePolicy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}

			statement.SetAggregateFunction(packageName, functionName, functionArgs, false);
			statement.Prepare();

			Node[] nodes = cluster.Nodes;
			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}

			Executor executor = new Executor(nodes.Length);

			foreach (Node node in nodes)
			{
				ServerCommand command = new ServerCommand(node, policy, statement);
				executor.AddCommand(command);
			}

			executor.Execute(nodes.Length);
			return new ExecuteTask(cluster, statement);
		}
		
		//--------------------------------------------------------
		// Query functions (Supported by Aerospike 3 servers only)
		//--------------------------------------------------------

		/// <summary>
		/// Execute query and return record iterator.  The query executor puts records on a queue in 
		/// separate threads.  The calling thread concurrently pops records off the queue through the 
		/// record iterator.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">database query command</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public RecordSet Query(QueryPolicy policy, Statement statement)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}
			QueryRecordExecutor executor = new QueryRecordExecutor(cluster, policy, statement);
			executor.Execute();
			return executor.RecordSet;
		}

		#if (! LITE)
		/// <summary>
		/// Execute query, apply statement's aggregation function, and return result iterator. The query 
		/// executor puts results on a queue in separate threads.  The calling thread concurrently pops 
		/// results off the queue through the result iterator.
		/// <para>
		/// The aggregation function is called on both server and client (final reduce).  Therefore,
		/// the Lua script files must also reside on both server and client.
		/// The package name is used to locate the udf file location:
		/// </para>
		/// <para>
		/// udf file = &lt;udf dir&gt;/&lt;package name&gt;.lua
		/// </para>
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">database query command</param>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public ResultSet QueryAggregate(QueryPolicy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}
			QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, packageName, functionName, functionArgs);
			executor.Execute();
			return executor.ResultSet;
		}
		#endif

		/// <summary>
		/// Create secondary index.
		/// This asynchronous server call will return before command is complete.
		/// The user can optionally wait for command completion by using the returned
		/// IndexTask instance.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="indexName">name of secondary index</param>
		/// <param name="binName">bin name that data is indexed on</param>
		/// <param name="indexType">type of secondary index</param>
		/// <exception cref="AerospikeException">if index create fails</exception>
		public IndexTask CreateIndex(Policy policy, string ns, string setName, string indexName, string binName, IndexType indexType)
		{
			StringBuilder sb = new StringBuilder(500);
			sb.Append("sindex-create:ns=");
			sb.Append(ns);

			if (setName != null && setName.Length > 0)
			{
				sb.Append(";set=");
				sb.Append(setName);
			}

			sb.Append(";indexname=");
			sb.Append(indexName);
			sb.Append(";numbins=1");
			sb.Append(";indexdata=");
			sb.Append(binName);
			sb.Append(",");
			sb.Append(indexType);
			sb.Append(";priority=normal");

			// Send index command to one node. That node will distribute the command to other nodes.
			String response = SendInfoCommand(policy, sb.ToString());

			if (response.Equals("OK", StringComparison.CurrentCultureIgnoreCase))
			{
				// Return task that could optionally be polled for completion.
				return new IndexTask(cluster, ns, indexName);
			}

			if (response.StartsWith("FAIL:200"))
			{
				// Index has already been created.  Do not need to poll for completion.
				return new IndexTask();
			}

			throw new AerospikeException("Create index failed: " + response);
		}

		/// <summary>
		/// Delete secondary index.
		/// <para>
		/// This method is only supported by Aerospike 3 servers.
		/// </para>
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="indexName">name of secondary index</param>
		/// <exception cref="AerospikeException">if index create fails</exception>
		public void DropIndex(Policy policy, string ns, string setName, string indexName)
		{
			StringBuilder sb = new StringBuilder(500);
			sb.Append("sindex-delete:ns=");
			sb.Append(ns);

			if (setName != null && setName.Length > 0)
			{
				sb.Append(";set=");
				sb.Append(setName);
			}
			sb.Append(";indexname=");
			sb.Append(indexName);

			// Send index command to one node. That node will distribute the command to other nodes.
			String response = SendInfoCommand(policy, sb.ToString());

			if (response.Equals("OK", StringComparison.CurrentCultureIgnoreCase))
			{
				return;
			}

			if (response.StartsWith("FAIL:201"))
			{
				// Index did not previously exist. Return without error.
				return;
			}

			throw new AerospikeException("Drop index failed: " + response);
		}

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
		/// <param name="roles">variable arguments array of role names.  Valid roles are listed in Role.cs</param>		
		public void CreateUser(AdminPolicy policy, string user, string password, List<string> roles)
		{
			string hash = AdminCommand.HashPassword(password);
			AdminCommand command = new AdminCommand();
			command.CreateUser(cluster, policy, user, hash, roles);
		}

		/// <summary>
		/// Remove user from cluster.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		public void DropUser(AdminPolicy policy, string user)
		{
			AdminCommand command = new AdminCommand();
			command.DropUser(cluster, policy, user);
		}

		/// <summary>
		/// Change user's password.  Clear-text password will be hashed using bcrypt 
		/// before sending to server.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="password">user password in clear-text format</param>
		public void ChangePassword(AdminPolicy policy, string user, string password)
		{
			if (cluster.user == null)
			{
				throw new AerospikeException("Invalid user");
			}

			string hash = AdminCommand.HashPassword(password);
			AdminCommand command = new AdminCommand();
			byte[] userBytes = ByteUtil.StringToUtf8(user);

			if (Util.ByteArrayEquals(userBytes, cluster.user))
			{
				// Change own password.
				command.ChangePassword(cluster, policy, userBytes, hash);
			}
			else
			{
				// Change other user's password by user admin.
				command.SetPassword(cluster, policy, userBytes, hash);
			}
			cluster.ChangePassword(userBytes, hash);
		}

		/// <summary>
		/// Add roles to user's list of roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="roles">role names.  Valid roles are listed in Role.cs</param>
		public void GrantRoles(AdminPolicy policy, string user, List<string> roles)
		{
			AdminCommand command = new AdminCommand();
			command.GrantRoles(cluster, policy, user, roles);
		}

		/// <summary>
		/// Remove roles from user's list of roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="roles">role names.  Valid roles are listed in Role.cs</param>
		public void RevokeRoles(AdminPolicy policy, string user, List<string> roles)
		{
			AdminCommand command = new AdminCommand();
			command.RevokeRoles(cluster, policy, user, roles);
		}

		/// <summary>
		/// Replace user's list of roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="roles">role names.  Valid roles are listed in Role.cs</param>
		public void ReplaceRoles(AdminPolicy policy, string user, List<string> roles)
		{
			AdminCommand command = new AdminCommand();
			command.ReplaceRoles(cluster, policy, user, roles);
		}

		/// <summary>
		/// Retrieve roles for a given user.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name filter</param>
		public UserRoles QueryUser(AdminPolicy policy, string user)
		{
			AdminCommand command = new AdminCommand();
			return command.QueryUser(cluster, policy, user);
		}

		/// <summary>
		/// Retrieve all users and their roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		public List<UserRoles> QueryUsers(AdminPolicy policy)
		{
			AdminCommand command = new AdminCommand();
			return command.QueryUsers(cluster, policy);
		}

		//-------------------------------------------------------
		// Internal Methods
		//-------------------------------------------------------

		protected internal static HashSet<string> BinNamesToHashSet(string[] binNames)
		{
			// Create lookup table for bin name filtering.
			HashSet<string> names = new HashSet<string>();

			foreach (string binName in binNames)
			{
				names.Add(binName);
			}
			return names;
		}

		private string SendInfoCommand(Policy policy, string command)
		{
			Node node = cluster.GetRandomNode();
			int timeout = (policy == null) ? 0 : policy.timeout;
			Connection conn = node.GetConnection(timeout);
			Info info;

			try
			{
				info = new Info(conn, command);
				node.PutConnection(conn);
			}
			catch (Exception)
			{
				conn.Close();
				throw;
			}
			return info.GetValue();
		}

		private void JoinRecords(BatchPolicy policy, Record record, Join[] joins)
		{
			foreach (Join join in joins)
			{
				List<object> keyList = (List<object>)record.GetValue(join.leftKeysBinName);

				if (keyList != null)
				{
					Key[] keyArray = new Key[keyList.Count];
					int count = 0;

					foreach (object obj in keyList)
					{
						Value value = Value.Get(obj);
						keyArray[count++] = new Key(join.rightNamespace, join.rightSetName, value);
					}

					Record[] records;
					if (join.rightBinNames == null || join.rightBinNames.Length == 0)
					{
						records = Get(policy, keyArray);
					}
					else
					{
						records = Get(policy, keyArray, join.rightBinNames);
					}
					record.bins[join.leftKeysBinName] = records;
				}
			}
		}
	}
}
