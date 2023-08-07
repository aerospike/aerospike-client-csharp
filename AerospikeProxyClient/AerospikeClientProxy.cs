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
using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.IO;
using Google.Protobuf.Compiler;
using Aerospike.Client;
using System.Threading;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Aerospike.Client.Proxy.KVS;
using Google.Protobuf;
using System.Threading.Channels;
using Grpc.Net.Client;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Google.Protobuf.WellKnownTypes;
using System.Xml.Linq;

namespace Aerospike.Client.Proxy
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
	public class AerospikeClientProxy : IDisposable//, IAerospikeClient
	{
		//-------------------------------------------------------
		// Static variables.
		//-------------------------------------------------------

		// Proxy client version
		public static string Version = GetVersion();

		private static readonly String NotSupported = "Method not supported in proxy client: ";

		//-------------------------------------------------------
		// Member variables.
		//------------------------------------------------------- 

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
		/// Default parent policy used in batch read commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public readonly BatchPolicy batchPolicyDefault;

		/// <summary>
		/// Default parent policy used in batch write commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public readonly BatchPolicy batchParentPolicyWriteDefault;

		/// <summary>
		/// Default write policy used in batch operate commands.
		/// Write policy fields include generation, expiration, durableDelete, etc...
		/// </summary>
		public readonly BatchWritePolicy batchWritePolicyDefault;

		/// <summary>
		/// Default delete policy used in batch delete commands.
		/// </summary>
		public readonly BatchDeletePolicy batchDeletePolicyDefault;

		/// <summary>
		/// Default user defined function policy used in batch UDF excecute commands.
		/// </summary>
		public readonly BatchUDFPolicy batchUDFPolicyDefault;

		/// <summary>
		/// Default info policy that is used when info command policy is null.
		/// </summary>
		public readonly InfoPolicy infoPolicyDefault;

		protected readonly WritePolicy operatePolicyReadDefault;

		private GrpcChannel channel { get; set; }

		//private readonly AuthTokenManager authTokenManager;

		//-------------------------------------------------------
		// Constructors
		//-------------------------------------------------------

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
		public AerospikeClientProxy(ClientPolicy policy, params Host[] hosts)
		{
			policy ??= new ClientPolicy();
			this.readPolicyDefault = policy.readPolicyDefault;
			this.writePolicyDefault = policy.writePolicyDefault;
			this.scanPolicyDefault = policy.scanPolicyDefault;
			this.queryPolicyDefault = policy.queryPolicyDefault;
			this.batchPolicyDefault = policy.batchPolicyDefault;
			this.batchParentPolicyWriteDefault = policy.batchParentPolicyWriteDefault;
			this.batchWritePolicyDefault = policy.batchWritePolicyDefault;
			this.batchDeletePolicyDefault = policy.batchDeletePolicyDefault;
			this.batchUDFPolicyDefault = policy.batchUDFPolicyDefault;
			this.infoPolicyDefault = policy.infoPolicyDefault;
			this.operatePolicyReadDefault = new WritePolicy(this.readPolicyDefault);

			channel = GrpcChannel.ForAddress(new UriBuilder("http", hosts[0].name, hosts[0].port).Uri);

			/*if (policy.user != null || policy.password != null)
			{
				authTokenManager = new AuthTokenManager(policy, channelProvider);
			}
			else
			{
				authTokenManager = null;
			}

			try
			{
				// The gRPC client policy transformed from the client policy.
				GrpcClientPolicy grpcClientPolicy = ToGrpcClientPolicy(policy);
			}
			catch (Exception e)
			{
				if (authTokenManager != null)
				{
					authTokenManager.close();
				}
				throw;
			}*/
		}

		/**
		 * Return client version string.
		 */
		private static string GetVersion()
		{
			//var properties = new Properties();
			string version = null;

			/*try
			{
				properties.load(AerospikeClientProxy.class.getClassLoader().getResourceAsStream("project.properties"));
				version = properties.getProperty("version");
			}
			catch (Exception e) 
			{
				Log.warn("Failed to retrieve client version: " + Util.getErrorMessage(e));
			}*/
			return version;
		}

		//-------------------------------------------------------
		// Operations policies
		//-------------------------------------------------------

		/// <summary>
		/// Default read policy that is used when read command policy is null.
		/// </summary>
		public Policy ReadPolicyDefault
		{
			get { return readPolicyDefault; }
		}

		/// <summary>
		/// Default write policy that is used when write command policy is null.
		/// </summary>
		public WritePolicy WritePolicyDefault
		{
			get { return writePolicyDefault; }
		}

		/// <summary>
		/// Default scan policy that is used when scan command policy is null.
		/// </summary>
		public ScanPolicy ScanPolicyDefault
		{
			get { return scanPolicyDefault; }
		}

		/// <summary>
		/// Default query policy that is used when query command policy is null.
		/// </summary>
		public QueryPolicy QueryPolicyDefault
		{
			get { return queryPolicyDefault; }
		}

		/// <summary>
		/// Default parent policy used in batch read commands.Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy BatchPolicyDefault
		{
			get { return batchPolicyDefault; }
		}

		/// <summary>
		/// Default parent policy used in batch write commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy BatchParentPolicyWriteDefault
		{
			get { return batchParentPolicyWriteDefault; }
		}

		/// <summary>
		/// Default write policy used in batch operate commands.
		/// Write policy fields include generation, expiration, durableDelete, etc...
		/// </summary>
		public BatchWritePolicy BatchWritePolicyDefault
		{
			get { return batchWritePolicyDefault; }
		}

		/// <summary>
		/// Default delete policy used in batch delete commands.
		/// </summary>
		public BatchDeletePolicy BatchDeletePolicyDefault
		{
			get { return batchDeletePolicyDefault; }
		}

		/// <summary>
		/// Default user defined function policy used in batch UDF excecute commands.
		/// </summary>
		public BatchUDFPolicy BatchUDFPolicyDefault
		{
			get { return batchUDFPolicyDefault; }
		}

		/// <summary>
		/// Default info policy that is used when info command policy is null.
		/// </summary>
		public InfoPolicy InfoPolicyDefault
		{
			get { return infoPolicyDefault; }
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
			/*try
			{
				if (authTokenManager != null)
				{
					authTokenManager.close();
				}
			}
			catch (Exception e)
			{
				Log.Warn("Failed to close authTokenManager: " + Util.GetErrorMessage(e));
			}

			if (!sharedThreadPool)
			{
				// Shutdown synchronous thread pool.
				threadPool.shutdown();
			}*/
		}

		/// <summary>
		/// Return if we are ready to talk to the database server cluster.
		/// </summary>
		public bool Connected
		{
			get
			{
				//return executor != null;
				return true;
			}
		}

		/// <summary>
		/// Cluster associated with this AerospikeClient instance.
		/// </summary>
		public Cluster Cluster
		{
			get
			{
				throw new AerospikeException(NotSupported + "getCluster");
			}
		}

		/// <summary>
		/// Return array of active server nodes in the cluster.
		/// </summary>
		public Node[] Nodes
		{
			get
			{
				throw new AerospikeException(NotSupported + "getNodes");
			}
		}

		/// <summary>
		/// Return operating cluster statistics.
		/// </summary>
		public ClusterStats GetClusterStats()
		{
			throw new AerospikeException(NotSupported + "getClusterStats");
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
			policy ??= writePolicyDefault;
			CommandProxy command = new(policy);
			command.Write(channel, policy, key, bins, Operation.Type.WRITE);
		}

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
			CommandProxy command = new(policy);
			await command.WriteAsync(channel, policy, token, key, bins, Operation.Type.WRITE);
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
			policy ??= writePolicyDefault;
			CommandProxy command = new(policy);
			command.Write(channel, policy, key, bins, Operation.Type.APPEND);
		}

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
			CommandProxy command = new(policy);
			await command.WriteAsync(channel, policy, token, key, bins, Operation.Type.APPEND);
		}

		/// <summary>
		/// Prepend bin string values to existing record bin values.
		/// The policy specifies the transaction timeout, record expiration and how the transaction is
		/// handled when the record already exists.
		/// This call works only for string values. 
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if prepend fails</exception>
		public void Prepend(WritePolicy policy, Key key, params Bin[] bins)
		{
			policy ??= writePolicyDefault;
			CommandProxy command = new(policy);
			command.Write(channel, policy, key, bins, Operation.Type.PREPEND);
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
			CommandProxy command = new(policy);
			await command.WriteAsync(channel, policy, token, key, bins, Operation.Type.PREPEND);
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
		/// <exception cref="AerospikeException">if add fails</exception>
		public void Add(WritePolicy policy, Key key, params Bin[] bins)
		{
			policy ??= writePolicyDefault;
			CommandProxy command = new(policy);
			command.Write(channel, policy, key, bins, Operation.Type.ADD);
		}

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
			CommandProxy command = new(policy);
			await command.WriteAsync(channel, policy, token, key, bins, Operation.Type.ADD);
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
			policy ??= writePolicyDefault;
			CommandProxy command = new(policy);
			return command.Delete(channel, policy, key);
		}

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
			CommandProxy command = new(policy);
			return await command.DeleteAsync(channel, policy, key, token);
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
		/// <exception cref="AerospikeException.BatchRecordArray">which contains results for keys that did complete</exception>
		/*public BatchResults Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(new BatchRecord[0], true);
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

			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			try
			{
				BatchStatus status = new BatchStatus(true);
				List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, batchPolicy, keys, records, attr.hasWrite, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchOperateArrayCommand(cluster, batchNode, batchPolicy, keys, null, records, attr, status);
				}

				BatchExecutor.Execute(batchPolicy, commands, status);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				// Batch terminated on fatal error.
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}*/

		// Not supported in proxy client
		public void Truncate(InfoPolicy policy, string ns, string set, DateTime? beforeLastUpdate)
		{
			throw new AerospikeException(NotSupported + "truncate");
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
			policy ??= writePolicyDefault;
			var command = new CommandProxy(policy);
			command.Touch(channel, policy, key);
		}

		/// <summary>
		/// Asynchronously reset record's time to expiration using the policy's expiration.
		/// </summary>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public async Task Touch(WritePolicy policy, CancellationToken token, Key key)
		{
			policy ??= writePolicyDefault;
			var command = new CommandProxy(policy);
			await command.TouchAsync(channel, policy, key, token);
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
			policy ??= readPolicyDefault;
			var command = new CommandProxy(policy);
			return command.Exists(channel, policy, key);
		}

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
			var command = new CommandProxy(policy);
			return await command.ExistsAsync(channel, policy, key, token);
		}

		/// <summary>
		/// Check if multiple record keys exist in one batch call.
		/// The returned boolean array is in positional order with the original key array order.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchExists">which contains results for keys that did complete</exception>
		/*public bool[] Exists(BatchPolicy policy, Key[] keys)
		{
			if (keys.Length == 0)
			{
				return new bool[0];
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}


			bool[] existsArray = new bool[keys.Length];

			try
			{
				BatchStatus status = new BatchStatus(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = cluster.GetRandomNode();
					BatchNode batchNode = new BatchNode(node, keys);
					BatchCommand command = new BatchExistsArrayCommand(cluster, batchNode, policy, keys, existsArray, status);
					BatchExecutor.Execute(command, status);
					return existsArray;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchExistsArrayCommand(cluster, batchNode, policy, keys, existsArray, status);
				}
				BatchExecutor.Execute(policy, commands, status);
				return existsArray;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchExists(existsArray, e);
			}
		}*/

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
			policy ??= readPolicyDefault;
			CommandProxy command = new(policy);
			return command.Read(channel, policy, key);
		}

		/// <summary>
		/// Asynchronously read entire record for specified key.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record> Get(Policy policy, CancellationToken token, Key key)
		{
			policy ??= readPolicyDefault;
			CommandProxy command = new(policy);
			return command.ReadAsync(channel, policy, key, token);
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
			policy ??= readPolicyDefault;
			CommandProxy command = new(policy);
			return command.GetHeader(channel, policy, key);
		}

		/// <summary>
		/// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="token">cancellation token</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<Record> GetHeader(Policy policy, CancellationToken token, Key key)
		{
			policy ??= readPolicyDefault;
			CommandProxy command = new(policy);
			return command.GetHeaderAsync(channel, policy, key, token);
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
		/// <returns>true if all batch key requests succeeded</returns>
		/// <exception cref="AerospikeException">if read fails</exception>
		/*public bool Get(BatchPolicy policy, List<BatchRead> records)
		{
			if (records.Count == 0)
			{
				return true;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			BatchStatus status = new BatchStatus(true);
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new BatchReadListCommand(cluster, batchNode, policy, records, status);
			}
			BatchExecutor.Execute(policy, commands, status);
			return status.GetStatus();
		}*/

		/// <summary>
		/// Read multiple records for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		/*public Record[] Get(BatchPolicy policy, Key[] keys)
		{
			if (keys.Length == 0)
			{
				return new Record[0];
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			Record[] records = new Record[keys.Length];

			try
			{
				BatchStatus status = new BatchStatus(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = cluster.GetRandomNode();
					BatchNode batchNode = new BatchNode(node, keys);
					BatchCommand command = new BatchGetArrayCommand(cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_GET_ALL, false, status);
					BatchExecutor.Execute(command, status);
					return records;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchGetArrayCommand(cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_GET_ALL, false, status);
				}
				BatchExecutor.Execute(policy, commands, status);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}*/

		/// <summary>
		/// Read multiple record headers and bins for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		/*public Record[] Get(BatchPolicy policy, Key[] keys, params string[] binNames)
		{
			if (keys.Length == 0)
			{
				return new Record[0];
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			Record[] records = new Record[keys.Length];

			try
			{
				BatchStatus status = new BatchStatus(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = cluster.GetRandomNode();
					BatchNode batchNode = new BatchNode(node, keys);
					BatchCommand command = new BatchGetArrayCommand(cluster, batchNode, policy, keys, binNames, null, records, Command.INFO1_READ, false, status);
					BatchExecutor.Execute(command, status);
					return records;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchGetArrayCommand(cluster, batchNode, policy, keys, binNames, null, records, Command.INFO1_READ, false, status);
				}
				BatchExecutor.Execute(policy, commands, status);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}*/

		/// <summary>
		/// Read multiple records for specified keys using read operations in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read operations on record</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		/*public Record[] Get(BatchPolicy policy, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				return new Record[0];
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			Record[] records = new Record[keys.Length];

			try
			{
				BatchStatus status = new BatchStatus(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = cluster.GetRandomNode();
					BatchNode batchNode = new BatchNode(node, keys);
					BatchCommand command = new BatchGetArrayCommand(cluster, batchNode, policy, keys, null, ops, records, Command.INFO1_READ, true, status);
					BatchExecutor.Execute(command, status);
					return records;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchGetArrayCommand(cluster, batchNode, policy, keys, null, ops, records, Command.INFO1_READ, true, status);
				}
				BatchExecutor.Execute(policy, commands, status);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
		}*/

		/// <summary>
		/// Read multiple record header data for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		/*public Record[] GetHeader(BatchPolicy policy, Key[] keys)
		{
			if (keys.Length == 0)
			{
				return new Record[0];
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			Record[] records = new Record[keys.Length];

			try
			{
				BatchStatus status = new BatchStatus(false);

				if (policy.allowProleReads)
				{
					// Send all requests to a single random node.
					Node node = cluster.GetRandomNode();
					BatchNode batchNode = new BatchNode(node, keys);
					BatchCommand command = new BatchGetArrayCommand(cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_NOBINDATA, false, status);
					BatchExecutor.Execute(command, status);
					return records;
				}

				List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, keys, null, false, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchGetArrayCommand(cluster, batchNode, policy, keys, null, null, records, Command.INFO1_READ | Command.INFO1_NOBINDATA, false, status);
				}
				BatchExecutor.Execute(policy, commands, status);
				return records;
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecords(records, e);
			}
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
		/// <exception cref="AerospikeException">if command fails</exception>
		public Record Operate(WritePolicy policy, Key key, params Operation[] operations)
		{
			OperateArgs args = new OperateArgs(policy, writePolicyDefault, operatePolicyReadDefault, key, operations);
			var command = new CommandProxy(policy);
			return command.Operate(channel, key, args);
		}

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
		public Task<Record> Operate(WritePolicy policy, CancellationToken token, Key key, params Operation[] ops)
		{
			OperateArgs args = new OperateArgs(policy, writePolicyDefault, operatePolicyReadDefault, key, ops);
			var command = new CommandProxy(policy);
			return command.OperateAsync(channel, key, args, token);
		}

		//-------------------------------------------------------
		// Batch Read/Write Operations
		//-------------------------------------------------------

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
		/// <param name="records">list of unique record identifiers and read/write operations</param>
		/// read/write operations to perform. <see cref="Operation.Get()"/> is not allowed because it returns a
		/// variable number of bins and makes it difficult (sometimes impossible) to lineup operations with 
		/// results. Instead, use <see cref="Operation.Get(string)"/> for each bin name.
		/// </param>
		/// <exception cref="AerospikeException">if queue is full</exception>
		public Task<bool> Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, CancellationToken token, List<BatchRecord> records)
		{
			if (records.Count == 0)
			{
				return (Task<bool>)Task.CompletedTask;
			}

			if (batchPolicy == null)
			{
				batchPolicy = batchParentPolicyWriteDefault;
			}

			if (writePolicy == null)
			{
				writePolicy = batchWritePolicyDefault;
			}

			var command = new CommandProxy(batchPolicy);
			return command.BatchOperate(channel, token, batchPolicy, writePolicy, records);
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
		/// <exception cref="AerospikeException.BatchRecordArray">which contains results for keys that did complete</exception>
		/*public BatchResults Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(new BatchRecord[0], true);
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
			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			try
			{
				BatchStatus status = new BatchStatus(true);
				List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, batchPolicy, keys, records, attr.hasWrite, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchOperateArrayCommand(cluster, batchNode, batchPolicy, keys, ops, records, attr, status);
				}

				BatchExecutor.Execute(batchPolicy, commands, status);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}*/

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
		/// <exception cref="AerospikeException">if scan fails</exception>
		public void ScanAll(ScanPolicy policy, string ns, string setName, ScanCallback callback, params string[] binNames)
		{
			/*if (policy == null)
			{
				policy = scanPolicyDefault;
			}

			Node[] nodes = cluster.ValidateNodes();
			PartitionTracker tracker = new PartitionTracker(policy, nodes);
			ScanExecutor.ScanPartitions(cluster, policy, ns, setName, binNames, callback, tracker);*/
		}

		/// Not supported in proxy client
		public void ScanNode(ScanPolicy policy, string nodeName, string ns, string setName, ScanCallback callback, params string[] binNames)
		{
			throw new AerospikeException(NotSupported + "scanNode");
		}

		/// Not supported in proxy client
		public void ScanNode(ScanPolicy policy, Node node, string ns, string setName, ScanCallback callback, params string[] binNames)
		{
			throw new AerospikeException(NotSupported + "scanNode");
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
		/// <exception cref="AerospikeException">if scan fails</exception>
		public void ScanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, string ns, string setName, ScanCallback callback, params string[] binNames)
		{
			if (policy == null)
			{
				policy = scanPolicyDefault;
			}

			/*Node[] nodes = cluster.ValidateNodes();
			PartitionTracker tracker = new PartitionTracker(policy, nodes, partitionFilter);
			ScanExecutor.ScanPartitions(cluster, policy, ns, setName, binNames, callback, tracker);*/
		}

		//---------------------------------------------------------------
		// User defined functions
		//---------------------------------------------------------------

		/// Not supported in proxy client
		public RegisterTask Register(Policy policy, string clientPath, string serverPath, Language language)
		{
			throw new AerospikeException(NotSupported + "register");
		}

		/// Not supported in proxy client
		public RegisterTask Register(Policy policy, Assembly resourceAssembly, string resourcePath, string serverPath, Language language)
		{
			throw new AerospikeException(NotSupported + "register");
		}

		/// Not supported in proxy client
		public RegisterTask RegisterUdfString(Policy policy, string code, string serverPath, Language language)
		{
			throw new AerospikeException(NotSupported + "registerUdfString");
		}

		/// Not supported in proxy client
		public void RemoveUdf(InfoPolicy policy, string serverPath)
		{
			throw new AerospikeException(NotSupported + "removeUdf");
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
			policy ??= writePolicyDefault;
			var command = new CommandProxy(policy);

			Record record = command.Execute(channel, policy, key, packageName, functionName, args);

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
			var command = new CommandProxy(policy);

			Record record = await command.ExecuteAsync(channel, token, policy, key, packageName, functionName);

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
		/*public BatchResults Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(new BatchRecord[0], true);
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

			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			try
			{
				BatchStatus status = new BatchStatus(true);
				List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, batchPolicy, keys, records, attr.hasWrite, status);
				BatchCommand[] commands = new BatchCommand[batchNodes.Count];
				int count = 0;

				foreach (BatchNode batchNode in batchNodes)
				{
					commands[count++] = new BatchUDFCommand(cluster, batchNode, batchPolicy, keys, packageName, functionName, argBytes, records, attr, status);
				}

				BatchExecutor.Execute(batchPolicy, commands, status);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				// Batch terminated on fatal error.
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}*/

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
		/*public ExecuteTask Execute(WritePolicy policy, Statement statement, string packageName, string functionName, params Value[] functionArgs)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}

			statement.PackageName = packageName;
			statement.FunctionName = functionName;
			statement.FunctionArgs = functionArgs;

			ulong taskId = statement.PrepareTaskId();
			Node[] nodes = cluster.ValidateNodes();
			Executor executor = new Executor(nodes.Length);

			foreach (Node node in nodes)
			{
				ServerCommand command = new ServerCommand(cluster, node, policy, statement, taskId);
				executor.AddCommand(command);
			}

			executor.Execute(nodes.Length);
			return new ExecuteTask(cluster, policy, statement, taskId);
		}*/

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
		/*public ExecuteTask Execute(WritePolicy policy, Statement statement, params Operation[] operations)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}

			statement.Operations = operations;

			ulong taskId = statement.PrepareTaskId();
			Node[] nodes = cluster.ValidateNodes();
			Executor executor = new Executor(nodes.Length);

			foreach (Node node in nodes)
			{
				ServerCommand command = new ServerCommand(cluster, node, policy, statement, taskId);
				executor.AddCommand(command);
			}
			executor.Execute(nodes.Length);
			return new ExecuteTask(cluster, policy, statement, taskId);
		}*/

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
		/*public void Query(QueryPolicy policy, Statement statement, Action<Key, Record> action)
		{
			using (RecordSet rs = Query(policy, statement))
			{
				while (rs.Next())
				{
					action(rs.Key, rs.Record);
				}
			}
		}*/

		/// <summary>
		/// Execute query and return record iterator.  The query executor puts records on a queue in 
		/// separate threads.  The calling thread concurrently pops records off the queue through the 
		/// record iterator.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="statement">query definition</param>
		/// <exception cref="AerospikeException">if query fails</exception>
		/*public RecordSet Query(QueryPolicy policy, Statement statement)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}

			Node[] nodes = cluster.ValidateNodes();

			if (cluster.hasPartitionQuery || statement.filter == null)
			{
				PartitionTracker tracker = new PartitionTracker(policy, statement, nodes);
				QueryPartitionExecutor executor = new QueryPartitionExecutor(cluster, policy, statement, nodes.Length, tracker);
				return executor.RecordSet;
			}
			else
			{
				QueryRecordExecutor executor = new QueryRecordExecutor(cluster, policy, statement, nodes);
				executor.Execute();
				return executor.RecordSet;
			}
		}*/

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
		/*public void Query(QueryPolicy policy, Statement statement, QueryListener listener)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}

			Node[] nodes = cluster.ValidateNodes();

			if (cluster.hasPartitionQuery || statement.filter == null)
			{
				PartitionTracker tracker = new PartitionTracker(policy, statement, nodes);
				QueryListenerExecutor.execute(cluster, policy, statement, listener, tracker);
			}
			else
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Query by partition is not supported");
			}
		}*/

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
		/*public void Query
		(
			QueryPolicy policy,
			Statement statement,
			PartitionFilter partitionFilter,
			QueryListener listener
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
				QueryListenerExecutor.execute(cluster, policy, statement, listener, tracker);
			}
			else
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Query by partition is not supported");
			}
		}*/

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
		/*public RecordSet QueryPartitions
		(
			QueryPolicy policy,
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
				QueryPartitionExecutor executor = new QueryPartitionExecutor(cluster, policy, statement, nodes.Length, tracker);
				return executor.RecordSet;
			}
			else
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "QueryPartitions() not supported");
			}
		}*/

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
		/*public ResultSet QueryAggregate
		(
			QueryPolicy policy,
			Statement statement,
			string packageName,
			string functionName,
			params Value[] functionArgs
		)
		{
			//statement.SetAggregateFunction(packageName, functionName, functionArgs);
			//return QueryAggregate(policy, statement);
		}*/

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
		/*public void QueryAggregate(QueryPolicy policy, Statement statement, Action<Object> action)
		{
			using (ResultSet rs = QueryAggregate(policy, statement))
			{
				while (rs.Next())
				{
					action(rs.Object);
				}
			}
		}*/

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
		/*public ResultSet QueryAggregate(QueryPolicy policy, Statement statement)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}

			Node[] nodes = cluster.ValidateNodes();
			QueryAggregateExecutor executor = new QueryAggregateExecutor(cluster, policy, statement, nodes);
			executor.Execute();
			return executor.ResultSet;
		}*/

		//--------------------------------------------------------
		// Secondary Index functions
		//--------------------------------------------------------

		/// Not supported in proxy client
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
			throw new AerospikeException(NotSupported + "createIndex");
		}

		/// Not supported in proxy client
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
			throw new AerospikeException(NotSupported + "createIndex");
		}
		
		/// Not supported by proxy client
		public IndexTask DropIndex(Policy policy, string ns, string setName, string indexName)
		{
			throw new AerospikeException(NotSupported + "dropIndex");
		}

		//-----------------------------------------------------------------
		// XDR - Cross datacenter replication
		//-----------------------------------------------------------------

		/// Not supported in proxy client
		public void SetXDRFilter(InfoPolicy policy, string datacenter, string ns, Expression filter)
		{
			throw new AerospikeException(NotSupported + "setXDRFilter");
		}

		//-------------------------------------------------------
		// User administration
		//-------------------------------------------------------

		/// Not supported in proxy client		
		public void CreateUser(AdminPolicy policy, string user, string password, IList<string> roles)
		{
			throw new AerospikeException(NotSupported + "createUser");
		}

		/// Not supported in proxy client
		public void DropUser(AdminPolicy policy, string user)
		{
			throw new AerospikeException(NotSupported + "dropUser");
		}

		/// Not supported in proxy client
		public void ChangePassword(AdminPolicy policy, string user, string password)
		{
			throw new AerospikeException(NotSupported + "changePassword");
		}

		/// Not supported in proxy client
		public void GrantRoles(AdminPolicy policy, string user, IList<string> roles)
		{
			throw new AerospikeException(NotSupported + "grantRoles");
		}

		/// Not supported in proxy client
		public void RevokeRoles(AdminPolicy policy, string user, IList<string> roles)
		{
			throw new AerospikeException(NotSupported + "revokeRoles");
		}

		/// Not supported in proxy client
		public void CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			throw new AerospikeException(NotSupported + "createRole");
		}

		/// Not supported in proxy client
		public void CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges, IList<string> whitelist)
		{
			throw new AerospikeException(NotSupported + "createRole");
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
		/// <exception cref="AerospikeException">if command fails</exception>
		public void CreateRole
		(
			AdminPolicy policy,
			string roleName,
			IList<Privilege> privileges,
			IList<string> whitelist,
			int readQuota,
			int writeQuota
		)
		{
			throw new AerospikeException(NotSupported + "createRole");
		}

		/// Not supported in proxy client
		public void DropRole(AdminPolicy policy, string roleName)
		{
			throw new AerospikeException(NotSupported + "dropRole");
		}

		/// Not supported in proxy client
		public void GrantPrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			throw new AerospikeException(NotSupported + "grantPrivileges");
		}

		/// Not supported in proxy client
		public void RevokePrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			throw new AerospikeException(NotSupported + "revokePrivileges");
		}

		/// Not supported in proxy client
		public void SetWhitelist(AdminPolicy policy, string roleName, IList<string> whitelist)
		{
			throw new AerospikeException(NotSupported + "setWhitelist");
		}

		/// Not supported in proxy client
		public void SetQuotas(AdminPolicy policy, string roleName, int readQuota, int writeQuota)
		{
			throw new AerospikeException(NotSupported + "setQuotas");
		}

		/// Not supported in proxy client
		public User QueryUser(AdminPolicy policy, string user)
		{
			throw new AerospikeException(NotSupported + "queryUser");
		}

		/// Not supported in proxy client
		public List<User> QueryUsers(AdminPolicy policy)
		{
			throw new AerospikeException(NotSupported + "queryUsers");
		}

		/// Not supported in proxy client
		public Role QueryRole(AdminPolicy policy, string roleName)
		{
			throw new AerospikeException(NotSupported + "queryRole");
		}

		/// Not supported in proxy client
		public List<Role> QueryRoles(AdminPolicy policy)
		{
			throw new AerospikeException(NotSupported + "queryRoles");
		}		
	}
}
