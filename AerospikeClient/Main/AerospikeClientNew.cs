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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text;
using System.Threading.Channels;

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
	public class AerospikeClientNew : IDisposable, IAerospikeClientNew
	{
		//-------------------------------------------------------
		// Member variables.
		//-------------------------------------------------------
		public Cluster Cluster { get; set; }

		/// <summary>
		/// Return if we are ready to talk to the database server cluster.
		/// </summary>
		public bool Connected
		{
			get
			{
				return Cluster.Connected;
			}
		}

		/// <summary>
		/// Return array of active server nodes in the cluster.
		/// </summary>
		public Node[] Nodes
		{
			get
			{
				return Cluster.Nodes;
			}
		}

		/// <summary>
		/// Default read policy that is used when read command policy is null.
		/// </summary>
		public Policy ReadPolicyDefault { get; set; }

		/// <summary>
		/// Default write policy that is used when write command policy is null.
		/// </summary>
		public WritePolicy WritePolicyDefault { get; set; }

		/// <summary>
		/// Default scan policy that is used when scan command policy is null.
		/// </summary>
		public ScanPolicy ScanPolicyDefault { get; set; }

		/// <summary>
		/// Default query policy that is used when query command policy is null.
		/// </summary>
		public QueryPolicy QueryPolicyDefault { get; set; }

		/// <summary>
		/// Default parent policy used in batch read commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy BatchPolicyDefault { get; set; }

		/// <summary>
		/// Default parent policy used in batch write commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy BatchParentPolicyWriteDefault { get; set; }

		/// <summary>
		/// Default write policy used in batch operate commands.
		/// Write policy fields include generation, expiration, durableDelete, etc...
		/// </summary>
		public BatchWritePolicy BatchWritePolicyDefault { get; set; }

		/// <summary>
		/// Default delete policy used in batch delete commands.
		/// </summary>
		public BatchDeletePolicy BatchDeletePolicyDefault { get; set; }

		/// <summary>
		/// Default user defined function policy used in batch UDF excecute commands.
		/// </summary>
		public BatchUDFPolicy BatchUDFPolicyDefault { get; set; }

		/// <summary>
		/// Default info policy that is used when info command policy is null.
		/// </summary>
		public InfoPolicy InfoPolicyDefault { get; set; }

		public WritePolicy OperatePolicyReadDefault { get; set; }


		private readonly ArrayPool<Byte> bufferPool;

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
		public AerospikeClientNew(string hostname, int port)
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
		public AerospikeClientNew(ClientPolicy policy, string hostname, int port)
			: this(policy, new Host(hostname, port))
		{
		}

		/// <summary>
		/// Initialize Aerospike client with suitable hosts to seed the cluster map.
		/// The client policy is used to set defaults and size internal data structures.
		/// For the first host connection that succeeds, the client will:
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
		public AerospikeClientNew(ClientPolicy policy, params Host[] hosts)
		{
			policy ??= new ClientPolicy();
			this.ReadPolicyDefault = policy.readPolicyDefault;
			this.WritePolicyDefault = policy.writePolicyDefault;
			this.ScanPolicyDefault = policy.scanPolicyDefault;
			this.QueryPolicyDefault = policy.queryPolicyDefault;
			this.BatchPolicyDefault = policy.batchPolicyDefault;
			this.BatchParentPolicyWriteDefault = policy.batchParentPolicyWriteDefault;
			this.BatchWritePolicyDefault = policy.batchWritePolicyDefault;
			this.BatchDeletePolicyDefault = policy.batchDeletePolicyDefault;
			this.BatchUDFPolicyDefault = policy.batchUDFPolicyDefault;
			this.InfoPolicyDefault = policy.infoPolicyDefault;
			this.OperatePolicyReadDefault = new WritePolicy(this.ReadPolicyDefault);

			bufferPool = ArrayPool<byte>.Create();

			Cluster = new Cluster(policy, hosts);
			Cluster.InitTendThread(policy.failIfNotConnected);
		}

		/// <summary>
		/// Construct client without initialization.
		/// Should only be used by classes inheriting from this client.
		/// </summary>
		protected internal AerospikeClientNew(ClientPolicy policy)
		{
			if (policy != null)
			{
				this.ReadPolicyDefault = policy.readPolicyDefault;
				this.WritePolicyDefault = policy.writePolicyDefault;
				this.ScanPolicyDefault = policy.scanPolicyDefault;
				this.QueryPolicyDefault = policy.queryPolicyDefault;
				this.BatchPolicyDefault = policy.batchPolicyDefault;
				this.BatchParentPolicyWriteDefault = policy.batchParentPolicyWriteDefault;
				this.BatchWritePolicyDefault = policy.batchWritePolicyDefault;
				this.BatchDeletePolicyDefault = policy.batchDeletePolicyDefault;
				this.BatchUDFPolicyDefault = policy.batchUDFPolicyDefault;
				this.InfoPolicyDefault = policy.infoPolicyDefault;
			}
			else
			{
				this.ReadPolicyDefault = new Policy();
				this.WritePolicyDefault = new WritePolicy();
				this.ScanPolicyDefault = new ScanPolicy();
				this.QueryPolicyDefault = new QueryPolicy();
				this.BatchPolicyDefault = BatchPolicy.ReadDefault();
				this.BatchParentPolicyWriteDefault = BatchPolicy.WriteDefault();
				this.BatchWritePolicyDefault = new BatchWritePolicy();
				this.BatchDeletePolicyDefault = new BatchDeletePolicy();
				this.BatchUDFPolicyDefault = new BatchUDFPolicy();
				this.InfoPolicyDefault = new InfoPolicy();
			}
			this.OperatePolicyReadDefault = new WritePolicy(this.ReadPolicyDefault);
		}

		//-------------------------------------------------------
		// Cluster Connection Management
		//-------------------------------------------------------

		public bool Disposed { get; private set; }
		private void Dispose(bool disposing)
		{
			if (!Disposed)
			{
				if (disposing)
				{
					this.Close();
				}

				Disposed = true;
			}
		}

		/// <summary>
		/// Close all client connections to database server nodes.
		/// </summary>
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Close all client connections to database server nodes.
		/// </summary>
		public void Close()
		{
			Cluster.Close();
		}

		

		/// <summary>
		/// Enable extended periodic cluster and node latency metrics.
		/// </summary>
		public void EnableMetrics(MetricsPolicy metricsPolicy)
		{
			Cluster.EnableMetrics(metricsPolicy);
		}

		/// <summary>
		/// Disable extended periodic cluster and node latency metrics.
		/// </summary>
		public void DisableMetrics()
		{
			Cluster.DisableMetrics();
		}

		/// <summary>
		/// Return operating cluster statistics snapshot.
		/// </summary>
		public ClusterStats GetClusterStats()
		{
			return Cluster.GetStats();
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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if write fails</exception>
		public async Task Put(WritePolicy policy, Key key, Bin[] bins, CancellationToken token)
		{
			policy ??= WritePolicyDefault;
			WriteCommandNew command = new(bufferPool, Cluster, policy, key, bins, Operation.Type.WRITE);
			await command.Execute(token);
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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if append fails</exception>
		public async Task Append(WritePolicy policy, Key key, Bin[] bins, CancellationToken token)
		{
			policy ??= WritePolicyDefault;
			WriteCommandNew command = new(bufferPool, Cluster, policy, key, bins, Operation.Type.APPEND);
			await command.Execute(token);
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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if prepend fails</exception>
		public async Task Prepend(WritePolicy policy, Key key, Bin[] bins, CancellationToken token)
		{
			policy ??= WritePolicyDefault;
			WriteCommandNew command = new(bufferPool, Cluster, policy, key, bins, Operation.Type.PREPEND);
			await command.Execute(token);
		}

		//-------------------------------------------------------
		// Arithmetic Operations
		//-------------------------------------------------------

		/// <summary>
		/// Add integer/double bin values to existing record bin values.
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if add fails</exception>
		public async Task Add(WritePolicy policy, Key key, Bin[] bins, CancellationToken token)
		{
			policy ??= WritePolicyDefault;
			WriteCommandNew command = new(bufferPool, Cluster, policy, key, bins, Operation.Type.ADD);
			await command.Execute(token);
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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if delete fails</exception>
		public async Task<bool> Delete(WritePolicy policy, Key key, CancellationToken token)
		{
			policy ??= WritePolicyDefault;
			DeleteCommandNew command = new(bufferPool, Cluster, policy, key);
			await command.Execute(token);
			return command.Existed;
		}

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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException.BatchRecordArray">which contains results for keys that did complete</exception>
		public async Task<BatchResults> Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys, CancellationToken token)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(Array.Empty<BatchRecord>(), true);
			}

			batchPolicy ??= BatchParentPolicyWriteDefault;

			deletePolicy ??= BatchDeletePolicyDefault;

			BatchAttr attr = new();
			attr.SetDelete(deletePolicy);

			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			try
			{
				BatchStatus status = new(true);
				List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, batchPolicy, keys, records, attr.hasWrite, status);
				BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchOperateArrayCommandNew(bufferPool, Cluster, batchNode, batchPolicy, keys, null, records, attr, status);
				}

				await commands[0].Execute(Cluster, batchPolicy, commands, status, token);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				// Batch terminated on fatal error.
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}

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
		/// <param name="token">cancellation token</param>
		public async Task Truncate(InfoPolicy policy, string ns, string set, DateTime? beforeLastUpdate, CancellationToken token)
		{
			throw new NotImplementedException();
			policy ??= InfoPolicyDefault;

			// Send truncate command to one node. That node will distribute the command to other nodes.
			Node node = Cluster.GetRandomNode();

			StringBuilder sb = new(200);

			if (set != null)
			{
				sb.Append("truncate:namespace=");
				sb.Append(ns);
				sb.Append(";set=");
				sb.Append(set);
			}
			else
			{
				sb.Append("truncate-namespace:namespace=");
				sb.Append(ns);
			}

			if (beforeLastUpdate.HasValue)
			{
				sb.Append(";lut=");
				// Convert to nanoseconds since unix epoch.
				sb.Append(Util.NanosFromEpoch(beforeLastUpdate.Value));
			}

			string response = Info.Request(policy, node, sb.ToString());

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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if touch fails</exception>
		public async Task Touch(WritePolicy policy, Key key, CancellationToken token)
		{
			policy ??= WritePolicyDefault;
			TouchCommandNew command = new(bufferPool, Cluster, policy, key);
			await command.Execute(token);
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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task<bool> Exists(Policy policy, Key key, CancellationToken token)
		{
			policy ??= ReadPolicyDefault;
			ExistsCommandNew command = new(bufferPool, Cluster, policy, key);
			await command.Execute(token);
			return command.Exists;
		}

		/// <summary>
		/// Check if multiple record keys exist in one batch call.
		/// The returned boolean array is in positional order with the original key array order.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException.BatchExists">which contains results for keys that did complete</exception>
		public async Task<bool[]> Exists(BatchPolicy policy, Key[] keys, CancellationToken token)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<bool>();
			}

			policy ??= BatchPolicyDefault;


			bool[] existsArray = new bool[keys.Length];

			try
			{
				BatchStatus status = new(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = Cluster.GetRandomNode();
					BatchNode batchNode = new(node, keys);
					BatchCommandNew command = new BatchExistsArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, existsArray, status);
					await command.Execute(Cluster, policy, new[] { command }, status, token);
					return existsArray;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, policy, keys, null, false, status);
				BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchExistsArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, existsArray, status);
				}
				await commands[0].Execute(Cluster, policy, commands, status, token);
				return existsArray;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchExists(existsArray, e);
			}
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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public async Task<Record> Get(Policy policy, Key key, CancellationToken token)
		{
			policy ??= ReadPolicyDefault;
			ReadCommandNew command = new(bufferPool, Cluster, policy, key);
			await command.Execute(token);
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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public async Task<Record> Get(Policy policy, Key key, string[] binNames, CancellationToken token)
		{
			policy ??= ReadPolicyDefault;
			ReadCommandNew command = new(bufferPool, Cluster, policy, key, binNames);
			await command.Execute(token);
			return command.Record;
		}

		/// <summary>
		/// Read record generation and expiration only for specified key.  Bins are not read.
		/// If found, return record instance.  If not found, return null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		public async Task<Record> GetHeader(Policy policy, Key key, CancellationToken token)
		{
			policy ??= ReadPolicyDefault;
			ReadHeaderCommandNew command = new(bufferPool, Cluster, policy, key);
			await command.Execute(token);
			return command.Record;
		}

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
		/// <param name="token">cancellation token</param>
		/// <returns>true if all batch key requests succeeded</returns>
		/// <exception cref="AerospikeException">if read fails</exception>
		public async Task<bool> Get(BatchPolicy policy, List<BatchRead> records, CancellationToken token)
		{
			if (records.Count == 0)
			{
				return true;
			}

			policy ??= BatchPolicyDefault;

			BatchStatus status = new(true);
			List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, policy, records, status);
			BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new BatchReadListCommandNew(bufferPool, Cluster, batchNode, policy, records, status);
			}
			await commands[0].Execute(Cluster, policy, commands, status, token);
			return status.GetStatus();
		}

		/// <summary>
		/// Read multiple records for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		public async Task<Record[]> Get(BatchPolicy policy, Key[] keys, CancellationToken token)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<Record>();
			}

			policy ??= BatchPolicyDefault;

			Record[] records = new Record[keys.Length];

			try
			{
				BatchStatus status = new(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = Cluster.GetRandomNode();
					BatchNode batchNode = new(node, keys);
					BatchCommandNew command = new BatchGetArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_GET_ALL, false, status);
					await command.Execute(Cluster, policy, new[] { command }, status, token);
					return records;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, policy, keys, null, false, status);
				BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchGetArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_GET_ALL, false, status);
				}
				await commands[0].Execute(Cluster, policy, commands, status, token);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}

		/// <summary>
		/// Read multiple record headers and bins for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		public async Task<Record[]> Get(BatchPolicy policy, Key[] keys, string[] binNames, CancellationToken token)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<Record>();
			}

			policy ??= BatchPolicyDefault;

			Record[] records = new Record[keys.Length];

			try
			{
				BatchStatus status = new(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = Cluster.GetRandomNode();
					BatchNode batchNode = new(node, keys);
					BatchCommandNew command = new BatchGetArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, binNames, null, records, Command.INFO1_READ, false, status);
					await command.Execute(Cluster, policy, new[] { command }, status, token);
					return records;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, policy, keys, null, false, status);
				BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchGetArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, binNames, null, records, Command.INFO1_READ, false, status);
				}
				await commands[0].Execute(Cluster, policy, commands, status, token);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}

		/// <summary>
		/// Read multiple records for specified keys using read operations in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read operations on record</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		public async Task<Record[]> Get(BatchPolicy policy, Key[] keys, Operation[] ops, CancellationToken token)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<Record>();
			}

			policy ??= BatchPolicyDefault;

			Record[] records = new Record[keys.Length];

			try
			{
				BatchStatus status = new(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = Cluster.GetRandomNode();
					BatchNode batchNode = new(node, keys);
					BatchCommandNew command = new BatchGetArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, null, ops, records, Command.INFO1_READ, true, status);
					await command.Execute(Cluster, policy, new[] { command }, status, token);
					return records;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, policy, keys, null, false, status);
				BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchGetArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, null, ops, records, Command.INFO1_READ, true, status);
				}
				await commands[0].Execute(Cluster, policy, commands, status, token);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}

		/// <summary>
		/// Read multiple record header data for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		public async Task<Record[]> GetHeader(BatchPolicy policy, Key[] keys, CancellationToken token)
		{
			if (keys.Length == 0)
			{
				return Array.Empty<Record>();
			}

			policy ??= BatchPolicyDefault;

			Record[] records = new Record[keys.Length];

			try
			{
				BatchStatus status = new(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = Cluster.GetRandomNode();
					BatchNode batchNode = new(node, keys);
					BatchCommandNew command = new BatchGetArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_NOBINDATA, false, status);
					await command.Execute(Cluster, policy, new[] { command }, status, token);
					return records;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, policy, keys, null, false, status);
				BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchGetArrayCommandNew(bufferPool, Cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_NOBINDATA, false, status);
				}
				await commands[0].Execute(Cluster, policy, commands, status, token);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
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
		/*public Record Join(BatchPolicy policy, Key key, string[] binNames, params Join[] joins)
		{
			throw new NotImplementedException();
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
			throw new NotImplementedException();
			Record record = Get(policy, key);
			JoinRecords(policy, record, joins);
			return record;
		}*/

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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task<Record> Operate(WritePolicy policy, Key key, Operation[] operations, CancellationToken token)
		{
			OperateArgs args = new OperateArgs(policy, WritePolicyDefault, OperatePolicyReadDefault, operations);
			policy = args.writePolicy;

			if (args.hasWrite)
			{
				/*if (policy.Txn != null)
				{
					TxnMonitor.AddKey(cluster, policy, key);
				}*/

				OperateCommandWriteNew command = new(bufferPool, Cluster, key, args);
				await command.Execute(token);
				return command.Record;
			}
			else
			{
				if (policy?.Txn != null)
				{
					policy.Txn.PrepareRead(key.ns);
				}

				OperateCommandReadNew command = new(bufferPool, Cluster, key, args);
				await command.Execute(token);
				return command.Record;
			}
		}

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
		/// <param name="token">cancellation token</param>
		/// <returns>true if all batch sub-commands succeeded</returns>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task<bool> Operate(BatchPolicy policy, List<BatchRecord> records, CancellationToken token)
		{
			if (records.Count == 0)
			{
				return true;
			}

			policy ??= BatchParentPolicyWriteDefault;

			BatchStatus status = new(true);
			List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, policy, records, status);
			BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new BatchOperateListCommandNew(bufferPool, Cluster, batchNode, policy, records, status);
			}
			await commands[0].Execute(Cluster, policy, commands, status, token);
			return status.GetStatus();
		}

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
		/// <param name="ops">
		/// read/write operations to perform. <see cref="Operation.Get()"/> is not allowed because it returns a
		/// variable number of bins and makes it difficult (sometimes impossible) to lineup operations with 
		/// results. Instead, use <see cref="Operation.Get(string)"/> for each bin name.
		/// </param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException.BatchRecordArray">which contains results for keys that did complete</exception>
		public async Task<BatchResults> Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, Operation[] ops, CancellationToken token)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(Array.Empty<BatchRecord>(), true);
			}

			batchPolicy ??= BatchParentPolicyWriteDefault;

			writePolicy ??= BatchWritePolicyDefault;

			BatchAttr attr = new(batchPolicy, writePolicy, ops);
			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			try
			{
				BatchStatus status = new(true);
				List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, batchPolicy, keys, records, attr.hasWrite, status);
				BatchCommandNew[] commands = new BatchCommandNew[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchOperateArrayCommandNew(bufferPool, Cluster, batchNode, batchPolicy, keys, ops, records, attr, status);
				}

				await commands[0].Execute(Cluster, batchPolicy, commands, status, token);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecordArray(records, e);
			}
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
		/// </param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		public async Task ScanAll(ScanPolicy policy, string ns, string setName, ScanCallback callback, string[] binNames, CancellationToken token)
		{
			throw new NotImplementedException();
			policy ??= ScanPolicyDefault;

			Node[] nodes = Cluster.ValidateNodes();
			PartitionTracker tracker = new(policy, nodes);
			ScanExecutor.ScanPartitions(Cluster, policy, ns, setName, binNames, callback, tracker);
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
		/// </param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		public async Task ScanNode(ScanPolicy policy, string nodeName, string ns, string setName, ScanCallback callback, string[] binNames, CancellationToken token)
		{
			throw new NotImplementedException();
			Node node = Cluster.GetNode(nodeName);
			ScanNode(policy, node, ns, setName, callback, binNames, token);
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
		/// </param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		public async Task ScanNode(ScanPolicy policy, Node node, string ns, string setName, ScanCallback callback, string[] binNames, CancellationToken token)
		{
			throw new NotImplementedException();
			policy ??= ScanPolicyDefault;

			PartitionTracker tracker = new(policy, node);
			ScanExecutor.ScanPartitions(Cluster, policy, ns, setName, binNames, callback, tracker);
		}

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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if scan fails</exception>
		public async Task ScanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, string ns, string setName, ScanCallback callback, string[] binNames, CancellationToken token)
		{
			throw new NotImplementedException();
			policy ??= ScanPolicyDefault;

			Node[] nodes = Cluster.ValidateNodes();
			PartitionTracker tracker = new(policy, nodes, partitionFilter);
			ScanExecutor.ScanPartitions(Cluster, policy, ns, setName, binNames, callback, tracker);
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
			throw new NotImplementedException();
			policy ??= WritePolicyDefault;
			string content = Util.ReadFileEncodeBase64(clientPath);
			return RegisterCommand.Register(Cluster, policy, content, serverPath, language);
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
			throw new NotImplementedException();
			policy ??= WritePolicyDefault;
			string content;
			using (Stream stream = resourceAssembly.GetManifestResourceStream(resourcePath))
			{
				byte[] bytes = new byte[stream.Length];
				stream.Read(bytes, 0, bytes.Length);
				content = Convert.ToBase64String(bytes);
			}
			return RegisterCommand.Register(Cluster, policy, content, serverPath, language);
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
			throw new NotImplementedException();
			policy ??= WritePolicyDefault;
			byte[] bytes = ByteUtil.StringToUtf8(code);
			string content = Convert.ToBase64String(bytes);
			return RegisterCommand.Register(Cluster, policy, content, serverPath, language);
		}

		/// <summary>
		/// Remove user defined function from server nodes.
		/// </summary>
		/// <param name="policy">info configuration parameters, pass in null for defaults</param>
		/// <param name="serverPath">location of UDF on server nodes.  Example: mylua.lua </param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if remove fails</exception>
		public async Task RemoveUdf(InfoPolicy policy, string serverPath, CancellationToken token)
		{
			throw new NotImplementedException();
			policy ??= InfoPolicyDefault;
			// Send UDF command to one node. That node will distribute the UDF command to other nodes.
			string command = "udf-remove:filename=" + serverPath;
			Node node = Cluster.GetRandomNode();
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
			throw new NotImplementedException();
			policy ??= WritePolicyDefault;
			ExecuteCommand command = new(Cluster, policy, key, packageName, functionName, args);
			command.Execute();

			Record record = command.Record;

			if (record == null || record.bins == null)
			{
				return null;
			}

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
		public BatchResults Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			throw new NotImplementedException();
			if (keys.Length == 0)
			{
				return new BatchResults(Array.Empty<BatchRecord>(), true);
			}

			batchPolicy ??= BatchParentPolicyWriteDefault;

			udfPolicy ??= BatchUDFPolicyDefault;

			byte[] argBytes = Packer.Pack(functionArgs);

			BatchAttr attr = new();
			attr.SetUDF(udfPolicy);

			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			try
			{
				BatchStatus status = new(true);
				List<BatchNode> batchNodes = BatchNode.GenerateList(Cluster, batchPolicy, keys, records, attr.hasWrite, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchUDFCommand(Cluster, batchNode, batchPolicy, keys, packageName, functionName, argBytes, records, attr, status);
				}

				BatchExecutor.Execute(Cluster, batchPolicy, commands, status);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				// Batch terminated on fatal error.
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}

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
		public ExecuteTask Execute(WritePolicy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs)
		{
			throw new NotImplementedException();
			policy ??= WritePolicyDefault;

			statement.PackageName = packageName;
			statement.FunctionName = functionName;
			statement.FunctionArgs = functionArgs;

			Cluster.AddCommandCount();

			ulong taskId = statement.PrepareTaskId();
			Node[] nodes = Cluster.ValidateNodes();
			Executor executor = new(nodes.Length);

			foreach (Node node in nodes)
			{
				ServerCommand command = new(Cluster, node, policy, statement, taskId);
				executor.AddCommand(command);
			}

			executor.Execute(nodes.Length);
			return new ExecuteTask(Cluster, policy, statement, taskId);
		}

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
		public ExecuteTask Execute(WritePolicy policy, Statement statement, params Operation[] operations)
		{
			throw new NotImplementedException();
			policy ??= WritePolicyDefault;

			statement.Operations = operations;

			Cluster.AddCommandCount();

			ulong taskId = statement.PrepareTaskId();
			Node[] nodes = Cluster.ValidateNodes();
			Executor executor = new(nodes.Length);

			foreach (Node node in nodes)
			{
				ServerCommand command = new(Cluster, node, policy, statement, taskId);
				executor.AddCommand(command);
			}
			executor.Execute(nodes.Length);
			return new ExecuteTask(Cluster, policy, statement, taskId);
		}

		//--------------------------------------------------------
		// Query functions
		//--------------------------------------------------------

		/// <summary>
		/// Execute query and call action for each record returned from server.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">query definition</param>
		/// <param name="action">action methods to be called for each record</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public async Task Query(QueryPolicy policy, Statement statement, Action<Key, Record> action, CancellationToken token)
		{
			throw new NotImplementedException();
			using RecordSet rs = (RecordSet)await Query(policy, statement, token);
			while (rs.Next())
			{
				action(rs.Key, rs.Record);
			}
		}

		/// <summary>
		/// Execute query and return record iterator.  The query executor puts records on a queue in 
		/// separate threads.  The calling thread concurrently pops records off the queue through the 
		/// record iterator.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">query definition</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public async Task<IRecordSet> Query(QueryPolicy policy, Statement statement, CancellationToken token)
		{
			throw new NotImplementedException();
			policy ??= QueryPolicyDefault;

			Node[] nodes = Cluster.ValidateNodes();

			if (Cluster.hasPartitionQuery || statement.filter == null)
			{
				PartitionTracker tracker = new(policy, statement, nodes);
				QueryPartitionExecutor executor = new(Cluster, policy, statement, nodes.Length, tracker);
				return executor.RecordSet;
			}
			else
			{
				QueryRecordExecutor executor = new(Cluster, policy, statement, nodes);
				executor.Execute();
				return executor.RecordSet;
			}
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
		/// <param name="statement">query definition</param>
		/// <param name="partitionFilter">filter on a subset of data partitions</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public IAsyncEnumerable<KeyRecord> QueryPartitions
		(
			QueryPolicy policy,
			Statement statement,
			PartitionFilter partitionFilter,
			CancellationToken token
		)
		{
			throw new NotImplementedException();

			/*policy ??= QueryPolicyDefault;

			Node[] nodes = Cluster.ValidateNodes();

			if (Cluster.hasPartitionQuery || statement.filter == null)
			{
				PartitionTracker tracker = new(policy, statement, nodes, partitionFilter);

				// Initialize maximum number of nodes to query in parallel.
				List<NodePartitions> list = tracker.AssignPartitionsToNodes(Cluster, statement.ns);
				int maxConcurrentThreads = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= list.Count) ? list.Count : policy.maxConcurrentNodes;
				
				// TODO should we check for one maxConcurrentThread and do something different?
				var channel = Channel.CreateBounded<QueryPartitionCommandNew>( // create channel somewhere else, partition possibly? need using clause so can be disposed
					new BoundedChannelOptions(maxConcurrentThreads)
					{
						SingleWriter = true,
						SingleReader = false,
						AllowSynchronousContinuations = true
					});
				var prepareTask = PrepareQueryPartition(channel, bufferPool, Cluster, policy, statement, tracker, list, token);
				var executeTask = ExecuteQueryPartition(channel, tracker, token);

				await Task.WhenAll(prepareTask, executeTask);

				if (tracker.IsClusterComplete(Cluster, policy)) // TODO, this checks maxRacords, error handling
				{
					// All partitions received.
					//recordSet.Put(RecordSet.END); // How to add last entry?
				}
			}
			else
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "QueryPartitions() not supported");
			}

			//return null; // TODO: get results from channel. use select many*/
		}

		private static async Task PrepareQueryPartition(
			Channel<QueryPartitionCommandNew> channel,
			//RecordSetNew recordSet, 
			ArrayPool<byte> bufferPool, 
			Cluster cluster, 
			QueryPolicy policy, 
			Statement statement, 
			PartitionTracker tracker,
			List<NodePartitions> list,
			CancellationToken token)
		{
			cluster.AddCommandCount();
			
			ulong taskId = statement.PrepareTaskId();
			// Produce query commands
			for (int i = 0; i < list.Count; i++)
			{
				QueryPartitionCommandNew queryCommand = new(bufferPool, cluster, policy, statement, taskId, tracker, list[i]);
				await channel.Writer.WriteAsync(queryCommand, token);
				taskId = RandomShift.ThreadLocalInstance.NextLong(); // Need different way to generate id
			}
			channel.Writer.Complete();
		}

		private static async Task ExecuteQueryPartition(
			Channel<QueryPartitionCommandNew> channel, 
			//RecordSetNew recordSet, 
			PartitionTracker tracker, 
			CancellationToken token)
		{
			// Consume/execute query commands
			while (await channel.Reader.WaitToReadAsync(token))
			{
				while (channel.Reader.TryRead(out var queryCommand))
				{
					try
					{
						await queryCommand.Execute(token);
					}
					catch (Exception exception)
					{
						// Wrap exception because throwing will reset the exception's stack trace.
						// Wrapped exceptions preserve the stack trace in the inner exception.
						AerospikeException ae = new("Query Failed: " + exception.Message, exception);
						tracker.PartitionError();
						ae.Iteration = tracker.iteration;
						//token
						// Trigger cancellation
						throw ae;
					}
				}
			}
		}

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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public async Task<IResultSet> QueryAggregate
		(
			QueryPolicy policy,
			Statement statement,
			string packageName,
			string functionName,
			Value[] functionArgs,
			CancellationToken token
		)
		{
			throw new NotImplementedException();
			statement.SetAggregateFunction(packageName, functionName, functionArgs);
			//return QueryAggregate(policy, statement, token);
		}

		/// <summary>
		/// Execute query, apply statement's aggregation function, call action for each aggregation
		/// object returned from server. 
		/// </summary>
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="statement">
		/// query definition with aggregate functions already initialized by SetAggregateFunction().
		/// </param>
		/// <param name="action">action methods to be called for each aggregation object</param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public async Task QueryAggregate(QueryPolicy policy, Statement statement, Action<Object> action, CancellationToken token)
		{
			throw new NotImplementedException();
			//using ResultSet rs = QueryAggregate(policy, statement, token);
			//while (rs.Next())
			//{
			//	action(rs.Object);
			//}
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
		/// <param name="policy">query configuration parameters, pass in null for defaults</param>
		/// <param name="statement">
		/// query definition with aggregate functions already initialized by SetAggregateFunction().
		/// </param>
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		public async Task<IResultSet> QueryAggregate(QueryPolicy policy, Statement statement, CancellationToken token)
		{
			throw new NotImplementedException();
			policy ??= QueryPolicyDefault;

			Node[] nodes = Cluster.ValidateNodes();
			QueryAggregateExecutor executor = new(Cluster, policy, statement, nodes);
			executor.Execute();
			return executor.ResultSet;
		}

		//--------------------------------------------------------
		// Secondary Index functions
		//--------------------------------------------------------

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
		public IndexTask CreateIndex
		(
			Policy policy,
			string ns,
			string setName,
			string indexName,
			string binName,
			IndexType indexType
		)
		{
			throw new NotImplementedException();
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
		/// <param name="ctx">optional context to index on elements within a CDT</param>
		/// <exception cref="AerospikeException">if index create fails</exception>
		public IndexTask CreateIndex
		(
			Policy policy,
			string ns,
			string setName,
			string indexName,
			string binName,
			IndexType indexType,
			IndexCollectionType indexCollectionType,
			params CTX[] ctx
		)
		{
			throw new NotImplementedException();
			policy ??= WritePolicyDefault;

			StringBuilder sb = new(1024);
			sb.Append("sindex-create:ns=");
			sb.Append(ns);

			if (setName != null && setName.Length > 0)
			{
				sb.Append(";set=");
				sb.Append(setName);
			}

			sb.Append(";indexname=");
			sb.Append(indexName);

			if (ctx != null && ctx.Length > 0)
			{
				byte[] bytes = PackUtil.Pack(ctx);
				string base64 = Convert.ToBase64String(bytes);

				sb.Append(";context=");
				sb.Append(base64);
			}

			if (indexCollectionType != IndexCollectionType.DEFAULT)
			{
				sb.Append(";indextype=");
				sb.Append(indexCollectionType);
			}

			sb.Append(";indexdata=");
			sb.Append(binName);
			sb.Append(',');
			sb.Append(indexType);

			// Send index command to one node. That node will distribute the command to other nodes.
			String response = SendInfoCommand(policy, sb.ToString());

			if (response.Equals("OK", StringComparison.CurrentCultureIgnoreCase))
			{
				// Return task that could optionally be polled for completion.
				return new IndexTask(Cluster, policy, ns, indexName, true);
			}

			ParseInfoError("Create index failed", response);
			return null;
		}

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
		public IndexTask DropIndex(Policy policy, string ns, string setName, string indexName)
		{
			throw new NotImplementedException();
			policy ??= WritePolicyDefault;
			StringBuilder sb = new(500);
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
				return new IndexTask(Cluster, policy, ns, indexName, false);
			}

			ParseInfoError("Drop index failed", response);
			return null;
		}

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
		/// <param name="token">cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task SetXDRFilter(InfoPolicy policy, string datacenter, string ns, Expression filter, CancellationToken token)
		{
			throw new NotImplementedException();
			policy ??= InfoPolicyDefault;

			// Send XDR command to one node. That node will distribute the XDR command to other nodes.
			string command = "xdr-set-filter:dc=" + datacenter + ";namespace=" + ns + ";exp=" + filter.GetBase64();
			Node node = Cluster.GetRandomNode();
			string response = Info.Request(policy, node, command);

			if (response.Equals("ok", StringComparison.CurrentCultureIgnoreCase))
			{
				return;
			}

			ParseInfoError("xdr-set-filter failed", response);
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
		/// <param name="token">cancellation token</param>
		public async Task CreateUser(AdminPolicy policy, string user, string password, IList<string> roles, CancellationToken token)
		{
			throw new NotImplementedException();
			string hash = AdminCommand.HashPassword(password);
			AdminCommand command = new();
			command.CreateUser(Cluster, policy, user, hash, roles);
		}

		/// <summary>
		/// Remove user from cluster.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="token"> cancellation token</param>
		public async Task DropUser(AdminPolicy policy, string user, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.DropUser(Cluster, policy, user);
		}

		/// <summary>
		/// Change user's password.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="password">user password in clear-text format</param>
		/// <param name="token"> cancellation token</param>
		public async Task ChangePassword(AdminPolicy policy, string user, string password, CancellationToken token)
		{
			throw new NotImplementedException();
			if (Cluster.user == null)
			{
				throw new AerospikeException("Invalid user");
			}

			byte[] userBytes = ByteUtil.StringToUtf8(user);
			byte[] passwordBytes = ByteUtil.StringToUtf8(password);

			string hash = AdminCommand.HashPassword(password);
			byte[] hashBytes = ByteUtil.StringToUtf8(hash);

			AdminCommand command = new();

			if (Util.ByteArrayEquals(userBytes, Cluster.user))
			{
				// Change own password.
				command.ChangePassword(Cluster, policy, userBytes, hash);
			}
			else
			{
				// Change other user's password by user admin.
				command.SetPassword(Cluster, policy, userBytes, hash);
			}
			Cluster.ChangePassword(userBytes, passwordBytes, hashBytes);
		}

		/// <summary>
		/// Add roles to user's list of roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="roles">role names.  Predefined roles are listed in Role.cs</param>
		/// <param name="token"> cancellation token</param>
		public async Task GrantRoles(AdminPolicy policy, string user, IList<string> roles, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.GrantRoles(Cluster, policy, user, roles);
		}

		/// <summary>
		/// Remove roles from user's list of roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name</param>
		/// <param name="roles">role names.  Predefined roles are listed in Role.cs</param>
		/// <param name="token"> cancellation token</param>
		public async Task RevokeRoles(AdminPolicy policy, string user, IList<string> roles, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.RevokeRoles(Cluster, policy, user, roles);
		}

		/// <summary>
		/// Create user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails </exception>
		public async Task CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.CreateRole(Cluster, policy, roleName, privileges);
		}

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
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges, IList<string> whitelist, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.CreateRole(Cluster, policy, roleName, privileges, whitelist, 0, 0);
		}

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
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task CreateRole
		(
			AdminPolicy policy,
			string roleName,
			IList<Privilege> privileges,
			IList<string> whitelist,
			int readQuota,
			int writeQuota,
			CancellationToken token
		)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.CreateRole(Cluster, policy, roleName, privileges, whitelist, readQuota, writeQuota);
		}

		/// <summary>
		/// Drop user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task DropRole(AdminPolicy policy, string roleName, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.DropRole(Cluster, policy, roleName);
		}

		/// <summary>
		/// Grant privileges to an user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task GrantPrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.GrantPrivileges(Cluster, policy, roleName, privileges);
		}

		/// <summary>
		/// Revoke privileges from an user defined role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="privileges">privileges assigned to the role.</param>
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task RevokePrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.RevokePrivileges(Cluster, policy, roleName, privileges);
		}

		/// <summary>
		/// Set IP address whitelist for a role.  If whitelist is null or empty, remove existing whitelist from role.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="whitelist">
		/// list of allowable IP addresses or null.
		/// IP addresses can contain wildcards (ie. 10.1.2.0/24).
		/// </param>
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task SetWhitelist(AdminPolicy policy, string roleName, IList<string> whitelist, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.SetWhitelist(Cluster, policy, roleName, whitelist);
		}

		/// <summary>
		/// Set maximum reads/writes per second limits for a role.  If a quota is zero, the limit is removed.
		/// Quotas require server security configuration "enable-quotas" to be set to true.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name</param>
		/// <param name="readQuota">maximum reads per second limit, pass in zero for no limit.</param>
		/// <param name="writeQuota">maximum writes per second limit, pass in zero for no limit.</param>
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task SetQuotas(AdminPolicy policy, string roleName, int readQuota, int writeQuota, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand command = new();
			command.setQuotas(Cluster, policy, roleName, readQuota, writeQuota);
		}

		/// <summary>
		/// Retrieve roles for a given user.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="user">user name filter</param>
		/// <param name="token"> cancellation token</param>
		public async Task<User> QueryUser(AdminPolicy policy, string user, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand.UserCommand command = new(1);
			return command.QueryUser(Cluster, policy, user);
		}

		/// <summary>
		/// Retrieve all users and their roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="token"> cancellation token</param>
		public async Task<List<User>> QueryUsers(AdminPolicy policy, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand.UserCommand command = new(100);
			return command.QueryUsers(Cluster, policy);
		}

		/// <summary>
		/// Retrieve role definition.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="roleName">role name filter</param>
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task<Role> QueryRole(AdminPolicy policy, string roleName, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand.RoleCommand command = new(1);
			return command.QueryRole(Cluster, policy, roleName);
		}

		/// <summary>
		/// Retrieve all roles.
		/// </summary>
		/// <param name="policy">admin configuration parameters, pass in null for defaults</param>
		/// <param name="token"> cancellation token</param>
		/// <exception cref="AerospikeException">if command fails</exception>
		public async Task<List<Role>> QueryRoles(AdminPolicy policy, CancellationToken token)
		{
			throw new NotImplementedException();
			AdminCommand.RoleCommand command = new(100);
			return command.QueryRoles(Cluster, policy);
		}

		//-------------------------------------------------------
		// Internal Methods
		//-------------------------------------------------------

		private string SendInfoCommand(Policy policy, string command)
		{
			Node node = Cluster.GetRandomNode();
			Connection conn = node.GetConnection(policy.socketTimeout);
			Info info;

			try
			{
				info = new Info(conn, command);
				node.PutConnection(conn);
			}
			catch (Exception)
			{
				node.CloseConnectionOnError(conn);
				throw;
			}
			return info.GetValue();
		}

		private static void ParseInfoError(string prefix, string response)
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
			throw new NotImplementedException();
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
					/*if (join.rightBinNames == null || join.rightBinNames.Length == 0)
					{
						records = Get(policy, keyArray);
					}
					else
					{
						records = Get(policy, keyArray, join.rightBinNames);
					}
					record.bins[join.leftKeysBinName] = records;*/
				}
			}
		}
	}
}
