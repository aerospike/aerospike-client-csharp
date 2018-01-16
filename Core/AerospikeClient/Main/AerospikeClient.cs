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
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Reflection;
using System.IO;
using AerospikeClient.Pooled_Objects;

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
	public class AerospikeClient : IDisposable, IAerospikeClient
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

		/// <summary>
		/// Default info policy that is used when info command policy is null.
		/// </summary>
		public readonly InfoPolicy infoPolicyDefault;

		protected readonly WritePolicy operatePolicyReadDefault;

        private static readonly StringBuilderPool _pool = new StringBuilderPool(() => new StringBuilder());

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
			this.infoPolicyDefault = policy.infoPolicyDefault;
			this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);

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
				this.infoPolicyDefault = policy.infoPolicyDefault;
			}
			else
			{
				this.readPolicyDefault = new Policy();
				this.writePolicyDefault = new WritePolicy();
				this.scanPolicyDefault = new ScanPolicy();
				this.queryPolicyDefault = new QueryPolicy();
				this.batchPolicyDefault = new BatchPolicy();
				this.infoPolicyDefault = new InfoPolicy();
			}
			this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);
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
			WriteCommand command = new WriteCommand(policy, key, bins, Operation.Type.WRITE);
			command.Execute(cluster, policy, key, null, false);
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
			WriteCommand command = new WriteCommand(policy, key, bins, Operation.Type.APPEND);
			command.Execute(cluster, policy, key, null, false);
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
			WriteCommand command = new WriteCommand(policy, key, bins, Operation.Type.PREPEND);
			command.Execute(cluster, policy, key, null, false);
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
			WriteCommand command = new WriteCommand(policy, key, bins, Operation.Type.ADD);
			command.Execute(cluster, policy, key, null, false);
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
			DeleteCommand command = new DeleteCommand(policy, key);
			command.Execute(cluster, policy, key, null, false);
			return command.Existed();
		}

		/// <summary>
		/// Remove records in specified namespace/set efficiently.  This method is many orders of magnitude 
		/// faster than deleting records one at a time.  Works with Aerospike Enterprise Server versions >= 3.12.
		/// See <a href="https://www.aerospike.com/docs/reference/info#truncate">https://www.aerospike.com/docs/reference/info#truncate</a>
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
		public void Truncate(InfoPolicy policy, string ns, string set, DateTime? beforeLastUpdate)
		{
			if (policy == null)
			{
				policy = infoPolicyDefault;
			}
			var sb = _pool.Allocate();
			sb.Append("truncate:namespace=");
			sb.Append(ns);

			if (!string.IsNullOrEmpty(set))
			{
				sb.Append(";set=");
				sb.Append(set);
			}

			if (beforeLastUpdate.HasValue)
			{
				sb.Append(";lut=");
				// Convert to nanoseconds since unix epoch.
				sb.Append(Util.NanosFromEpoch(beforeLastUpdate.Value));
			}

			// Send truncate command to one node. That node will distribute the command to other nodes.
			Node node = cluster.GetRandomNode();
			string response = Info.Request(policy, node, sb.ToString());
            _pool.Free(sb);
			if (!response.Equals("ok", StringComparison.CurrentCultureIgnoreCase))
			{
				throw new AerospikeException("Truncate failed: " + response);
			}
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
			TouchCommand command = new TouchCommand(policy, key);
			command.Execute(cluster, policy, key, null, false);
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
			ExistsCommand command = new ExistsCommand(policy, key);
			command.Execute(cluster, policy, key, null, true);
			return command.Exists();
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
			ReadCommand command = new ReadCommand(policy, key, null);
			command.Execute(cluster, policy, key, null, true);
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
			ReadCommand command = new ReadCommand(policy, key, binNames);
			command.Execute(cluster, policy, key, null, true);
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
			ReadHeaderCommand command = new ReadHeaderCommand(policy, key);
			command.Execute(cluster, policy, key, null, true);
			return command.Record;
		}

		//-------------------------------------------------------
		// Batch Read Operations
		//-------------------------------------------------------

		/// <summary>
		/// Read multiple records for specified batch keys in one batch call.
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// The returned records are located in the same list.
		/// If the BatchRecord key field is not found, the corresponding record field will be null.
		/// The policy can be used to specify timeouts and maximum concurrent threads.
		/// This method requires Aerospike Server version >= 3.6.0.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="records">list of unique record identifiers and the bins to retrieve.
		/// The returned records are located in the same list.</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public void Get(BatchPolicy policy, List<BatchRead> records)
		{
			if (records.Count == 0)
			{
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records);

			if (policy.maxConcurrentThreads == 1 || batchNodes.Count <= 1)
			{
				// Run batch requests sequentially in same thread.
				foreach (BatchNode batchNode in batchNodes)
				{
					if (!batchNode.node.HasBatchIndex)
					{
						throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
					}
					MultiCommand command = new BatchReadListCommand(batchNode, policy, records);
					command.Execute(cluster, policy, null, batchNode.node, true);
				}
			}
			else
			{
				// Run batch requests in parallel in separate threads.
				//			
				// Multiple threads write to the record list, so one might think that
				// volatile or memory barriers are needed on the write threads and this read thread.
				// This should not be necessary here because it happens in Executor which does a 
				// volatile write (Interlocked.Increment(ref completedCount)) at the end of write threads
				// and a synchronized WaitTillComplete() in this thread.
				Executor executor = new Executor(cluster, policy, batchNodes.Count);

				foreach (BatchNode batchNode in batchNodes)
				{
					if (!batchNode.node.HasBatchIndex)
					{
						throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Requested command requires a server that supports new batch index protocol.");
					}
					MultiCommand command = new BatchReadListCommand(batchNode, policy, records);
					executor.AddCommand(batchNode.node, command);
				}
				executor.Execute(policy.maxConcurrentThreads);
			}
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
			BatchExecutor.Execute(cluster, policy, keys, null, records, binNames, Command.INFO1_READ);
			return records;
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
		/// <para>
		/// Both scalar bin operations (Operation) and list bin operations (ListOperation)
		/// can be performed in same call.
		/// </para>
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="operations">database operations to perform</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public Record Operate(WritePolicy policy, Key key, params Operation[] operations)
		{
			OperateArgs args = new OperateArgs();
			OperateCommand command = new OperateCommand(key, operations);
			command.EstimateOperate(operations, args);

			if (policy == null)
			{
				if (args.hasWrite)
				{
					policy = writePolicyDefault;
				}
				else
				{
					policy = operatePolicyReadDefault;
				}
			}

			if (policy.respondAllOps)
			{
				args.writeAttr |= Command.INFO2_RESPOND_ALL_OPS;
			}
			command.SetArgs(policy, args);
			command.Execute(cluster, policy, key, null, false);
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

			if (policy.scanPercent <= 0 || policy.scanPercent > 100)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid scan percent: " + policy.scanPercent);
			}

			Node[] nodes = cluster.Nodes;

			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.");
			}

			if (policy.concurrentNodes)
			{
				Executor executor = new Executor(cluster, policy, nodes.Length);
				ulong taskId = RandomShift.ThreadLocalInstance.NextLong();

				foreach (Node node in nodes)
				{
					ScanCommand command = new ScanCommand(policy, ns, setName, callback, binNames, taskId);
					executor.AddCommand(node, command);
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

			if (policy.scanPercent <= 0 || policy.scanPercent > 100)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid scan percent: " + policy.scanPercent);
			}

			ulong taskId = RandomShift.ThreadLocalInstance.NextLong();

			ScanCommand command = new ScanCommand(policy, ns, setName, callback, binNames, taskId);
			command.Execute(cluster, policy, null, node, true);
		}
		
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
		public RegisterTask Register(Policy policy, string clientPath, string serverPath, Language language)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			string content = Util.ReadFileEncodeBase64(clientPath);
			return RegisterCommand.Register(cluster, policy, content, serverPath, language);
		}

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
		public RegisterTask Register(Policy policy, Assembly resourceAssembly, string resourcePath, string serverPath, Language language)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			string content;
			using (Stream stream = resourceAssembly.GetManifestResourceStream(resourcePath))
			{
				byte[] bytes = new byte[stream.Length];
				stream.Read(bytes, 0, bytes.Length);
				content = Convert.ToBase64String(bytes);
			}
			return RegisterCommand.Register(cluster, policy, content, serverPath, language);
		}

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
		public RegisterTask RegisterUdfString(Policy policy, string code, string serverPath, Language language)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			byte[] bytes = ByteUtil.StringToUtf8(code);
			string content = Convert.ToBase64String(bytes);
			return RegisterCommand.Register(cluster, policy, content, serverPath, language);
		}

		/// <summary>
		/// Remove user defined function from server nodes.
		/// </summary>
		/// <param name="policy">info configuration parameters, pass in null for defaults</param>
		/// <param name="serverPath">location of UDF on server nodes.  Example: mylua.lua </param>
		/// <exception cref="AerospikeException">if remove fails</exception>
		public void RemoveUdf(InfoPolicy policy, string serverPath)
		{
			if (policy == null)
			{
				policy = infoPolicyDefault;
			}
			// Send UDF command to one node. That node will distribute the UDF command to other nodes.
			string command = "udf-remove:filename=" + serverPath;
			Node node = cluster.GetRandomNode();
			string response = Info.Request(policy, node, command);

			if (response.Equals("ok", StringComparison.CurrentCultureIgnoreCase))
			{
				return;
			}

			if (response.StartsWith("error=file_not_found"))
			{
				// UDF has already been removed.
				return;
			}
			throw new AerospikeException("Remove UDF failed: " + response);
		}

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
		/// <exception cref="AerospikeException">if transaction fails</exception>
		public object Execute(WritePolicy policy, Key key, string packageName, string functionName, params Value[] args)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			ExecuteCommand command = new ExecuteCommand(policy, key, packageName, functionName, args);
			command.Execute(cluster, policy, key, null, false);

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
		// Query/Execute UDF
		//----------------------------------------------------------

		/// <summary>
		/// Apply user defined function on records that match the statement filter.
		/// Records are not returned to the client.
		/// This asynchronous server call will return before command is complete.  
		/// The user can optionally wait for command completion by using the returned 
		/// ExecuteTask instance.
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

			statement.SetAggregateFunction(packageName, functionName, functionArgs);
			statement.Prepare(false);

			Node[] nodes = cluster.Nodes;
			if (nodes.Length == 0)
			{
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}

			Executor executor = new Executor(cluster, policy, nodes.Length);

			foreach (Node node in nodes)
			{
				ServerCommand command = new ServerCommand(policy, statement);
				executor.AddCommand(node, command);
			}

			executor.Execute(nodes.Length);
			return new ExecuteTask(cluster, policy, statement);
		}
		
		//--------------------------------------------------------
		// Query functions
		//--------------------------------------------------------

		/// <summary>
		/// Execute query and call action for each record returned from server.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">database query command</param>
		/// <param name="action">action methods to be called for each record</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public void Query(QueryPolicy policy, Statement statement, Action<Key, Record> action)
		{
			using (RecordSet rs = Query(policy, statement))
			{
				while (rs.Next())
				{
					action(rs.Key, rs.Record);
				}
			}
		}

		/// <summary>
		/// Execute query and return record iterator.  The query executor puts records on a queue in 
		/// separate threads.  The calling thread concurrently pops records off the queue through the 
		/// record iterator.
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

#if NETFRAMEWORK
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
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">database query command</param>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public ResultSet QueryAggregate(QueryPolicy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs)
		{
			statement.SetAggregateFunction(packageName, functionName, functionArgs);
			return QueryAggregate(policy, statement);
		}

		/// <summary>
		/// Execute query, apply statement's aggregation function, call action for each aggregation
		/// object returned from server. 
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">database query command with aggregate functions already initialized by SetAggregateFunction()</param>
		/// <param name="action">action methods to be called for each aggregation object</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public void QueryAggregate(QueryPolicy policy, Statement statement, Action<Object> action)
		{
			using (ResultSet rs = QueryAggregate(policy, statement))
			{
				while (rs.Next())
				{
					action(rs.Object);
				}
			}
		}

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
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">database query command with aggregate functions already initialized by SetAggregateFunction()</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public ResultSet QueryAggregate(QueryPolicy policy, Statement statement)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}
			statement.Prepare(true);

			QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement);
			executor.Execute();
			return executor.ResultSet;
		}
#endif

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
		public IndexTask CreateIndex(Policy policy, string ns, string setName, string indexName, string binName, IndexType indexType)
		{
			return CreateIndex(policy, ns, setName, indexName, binName, indexType, IndexCollectionType.DEFAULT);	
		}

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
		/// <exception cref="AerospikeException">if index create fails</exception>
		public IndexTask CreateIndex(Policy policy, string ns, string setName, string indexName, string binName, IndexType indexType, IndexCollectionType indexCollectionType)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			var sb = _pool.Allocate();
			sb.Append("sindex-create:ns=");
			sb.Append(ns);

			if (!string.IsNullOrEmpty(setName))
			{
				sb.Append(";set=");
				sb.Append(setName);
			}

			sb.Append(";indexname=");
			sb.Append(indexName);
			sb.Append(";numbins=1");

			if (indexCollectionType != IndexCollectionType.DEFAULT)
			{
				sb.Append(";indextype=");
				sb.Append(indexCollectionType);
			}

			sb.Append(";indexdata=");
			sb.Append(binName);
			sb.Append(",");
			sb.Append(indexType);
			sb.Append(";priority=normal");

			// Send index command to one node. That node will distribute the command to other nodes.
			String response = SendInfoCommand(policy, sb.ToString());
            _pool.Free(sb);
			if (response.Equals("OK", StringComparison.CurrentCultureIgnoreCase))
			{
				// Return task that could optionally be polled for completion.
				return new IndexTask(cluster, policy, ns, indexName);
			}

			ParseInfoError("Create index failed", response);
			return null;
		}
		
		/// <summary>
		/// Delete secondary index.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="ns">namespace - equivalent to database name</param>
		/// <param name="setName">optional set name - equivalent to database table</param>
		/// <param name="indexName">name of secondary index</param>
		/// <exception cref="AerospikeException">if index create fails</exception>
		public void DropIndex(Policy policy, string ns, string setName, string indexName)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			var sb = _pool.Allocate();
			sb.Append("sindex-delete:ns=");
			sb.Append(ns);

			if (!string.IsNullOrEmpty(setName))
			{
				sb.Append(";set=");
				sb.Append(setName);
			}
			sb.Append(";indexname=");
			sb.Append(indexName);

			// Send index command to one node. That node will distribute the command to other nodes.
			String response = SendInfoCommand(policy, sb.ToString());
            _pool.Free(sb);
			if (response.Equals("OK", StringComparison.CurrentCultureIgnoreCase))
			{
				return;
			}

			ParseInfoError("Drop index failed", response);
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
		/// <param name="roles">variable arguments array of role names.  Predefined roles are listed in Role.cs</param>		
		public void CreateUser(AdminPolicy policy, string user, string password, IList<string> roles)
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
		/// <param name="roles">role names.  Predefined roles are listed in Role.cs</param>
		public void GrantRoles(AdminPolicy policy, string user, IList<string> roles)
		{
			AdminCommand command = new AdminCommand();
			command.GrantRoles(cluster, policy, user, roles);
		}

		/// <summary>
		/// Remove roles from user's list of roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="roles">role names.  Predefined roles are listed in Role.cs</param>
		public void RevokeRoles(AdminPolicy policy, string user, IList<string> roles)
		{
			AdminCommand command = new AdminCommand();
			command.RevokeRoles(cluster, policy, user, roles);
		}

		/// <summary>
		/// Create user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <exception cref="AerospikeException">if command fails </exception>
		public void CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			AdminCommand command = new AdminCommand();
			command.CreateRole(cluster, policy, roleName, privileges);
		}

		/// <summary>
		/// Drop user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public void DropRole(AdminPolicy policy, string roleName)
		{
			AdminCommand command = new AdminCommand();
			command.DropRole(cluster, policy, roleName);
		}

		/// <summary>
		/// Grant privileges to an user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public void GrantPrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			AdminCommand command = new AdminCommand();
			command.GrantPrivileges(cluster, policy, roleName, privileges);
		}

		/// <summary>
		/// Revoke privileges from an user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public void RevokePrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			AdminCommand command = new AdminCommand();
			command.RevokePrivileges(cluster, policy, roleName, privileges);
		}
	
		/// <summary>
		/// Retrieve roles for a given user.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name filter</param>
		public User QueryUser(AdminPolicy policy, string user)
		{
			AdminCommand.UserCommand command = new AdminCommand.UserCommand(1);
			return command.QueryUser(cluster, policy, user);
		}

		/// <summary>
		/// Retrieve all users and their roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		public List<User> QueryUsers(AdminPolicy policy)
		{
			AdminCommand.UserCommand command = new AdminCommand.UserCommand(100);
			return command.QueryUsers(cluster, policy);
		}

		/// <summary>
		/// Retrieve role definition.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name filter</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public Role QueryRole(AdminPolicy policy, string roleName)
		{
			AdminCommand.RoleCommand command = new AdminCommand.RoleCommand(1);
			return command.QueryRole(cluster, policy, roleName);
		}

		/// <summary>
		/// Retrieve all roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public List<Role> QueryRoles(AdminPolicy policy)
		{
			AdminCommand.RoleCommand command = new AdminCommand.RoleCommand(100);
			return command.QueryRoles(cluster, policy);
		}

		//-------------------------------------------------------
		// Internal Methods
		//-------------------------------------------------------

		private string SendInfoCommand(Policy policy, string command)
		{
			Node node = cluster.GetRandomNode();
			Connection conn = node.GetConnection(policy.socketTimeout);
			Info info;

			try
			{
				info = new Info(conn, command);
				node.PutConnection(conn);
			}
			catch (Exception)
			{
				node.CloseConnection(conn);
				throw;
			}
			return info.GetValue();
		}

		private void ParseInfoError(string prefix, string response)
		{
			string message = prefix + ": " + response;
			string[] list = response.Split(':');

			if (list.Length >= 2 && list[0].Equals("FAIL"))
			{
				int code = 0;

				try
				{
					code = Convert.ToInt32(list[1]);
				}
				catch (Exception)
				{
				}
				throw new AerospikeException(code, message);
			}
			throw new AerospikeException(message);
		}

		private void JoinRecords(BatchPolicy policy, Record record, Join[] joins)
		{
			if (record == null)
			{
				return;
			}

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
