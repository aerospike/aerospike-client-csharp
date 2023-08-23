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
using Aerospike.Client.KVS;
using Google.Protobuf;
using System.Threading.Channels;
using Grpc.Net.Client;
using Google.Protobuf.WellKnownTypes;
using System.Xml.Linq;

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
	public class AerospikeClientProxy : IDisposable, IAerospikeClient
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
		public Policy readPolicyDefault { get; set; }

		/// <summary>
		/// Default write policy that is used when write command policy is null.
		/// </summary>
		public WritePolicy writePolicyDefault { get; set; }

		/// <summary>
		/// Default scan policy that is used when scan command policy is null.
		/// </summary>
		public ScanPolicy scanPolicyDefault { get; set; }

		/// <summary>
		/// Default query policy that is used when query command policy is null.
		/// </summary>
		public QueryPolicy queryPolicyDefault { get; set; }

		/// <summary>
		/// Default parent policy used in batch read commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy batchPolicyDefault { get; set; }

		/// <summary>
		/// Default parent policy used in batch write commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy batchParentPolicyWriteDefault { get; set; }

		/// <summary>
		/// Default write policy used in batch operate commands.
		/// Write policy fields include generation, expiration, durableDelete, etc...
		/// </summary>
		public BatchWritePolicy batchWritePolicyDefault { get; set; }

		/// <summary>
		/// Default delete policy used in batch delete commands.
		/// </summary>
		public BatchDeletePolicy batchDeletePolicyDefault { get; set; }

		/// <summary>
		/// Default user defined function policy used in batch UDF excecute commands.
		/// </summary>
		public BatchUDFPolicy batchUDFPolicyDefault { get; set; }

		/// <summary>
		/// Default info policy that is used when info command policy is null.
		/// </summary>
		public InfoPolicy infoPolicyDefault { get; set; }

		protected WritePolicy operatePolicyReadDefault { get; set; }

		internal GrpcChannel channel { get; set; }

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
			set { readPolicyDefault = value; }
		}

		/// <summary>
		/// Default write policy that is used when write command policy is null.
		/// </summary>
		public WritePolicy WritePolicyDefault
		{
			get { return writePolicyDefault; }
			set { writePolicyDefault = value; }
		}

		/// <summary>
		/// Default scan policy that is used when scan command policy is null.
		/// </summary>
		public ScanPolicy ScanPolicyDefault
		{
			get { return scanPolicyDefault; }
			set { scanPolicyDefault = value; }
		}

		/// <summary>
		/// Default query policy that is used when query command policy is null.
		/// </summary>
		public QueryPolicy QueryPolicyDefault
		{
			get { return queryPolicyDefault; }
			set { queryPolicyDefault = value; }
		}

		/// <summary>
		/// Default parent policy used in batch read commands.Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy BatchPolicyDefault
		{
			get { return batchPolicyDefault; }
			set { batchPolicyDefault = value; }
		}

		/// <summary>
		/// Default parent policy used in batch write commands. Parent policy fields
		/// include socketTimeout, totalTimeout, maxRetries, etc...
		/// </summary>
		public BatchPolicy BatchParentPolicyWriteDefault
		{
			get { return batchParentPolicyWriteDefault; }
			set { batchParentPolicyWriteDefault = value; }
		}

		/// <summary>
		/// Default write policy used in batch operate commands.
		/// Write policy fields include generation, expiration, durableDelete, etc...
		/// </summary>
		public BatchWritePolicy BatchWritePolicyDefault
		{
			get { return batchWritePolicyDefault; }
			set { batchWritePolicyDefault = value; }
		}

		/// <summary>
		/// Default delete policy used in batch delete commands.
		/// </summary>
		public BatchDeletePolicy BatchDeletePolicyDefault
		{
			get { return batchDeletePolicyDefault; }
			set { batchDeletePolicyDefault = value; }
		}

		/// <summary>
		/// Default user defined function policy used in batch UDF excecute commands.
		/// </summary>
		public BatchUDFPolicy BatchUDFPolicyDefault
		{
			get { return batchUDFPolicyDefault; }
			set { batchUDFPolicyDefault = value; }
		}

		/// <summary>
		/// Default info policy that is used when info command policy is null.
		/// </summary>
		public InfoPolicy InfoPolicyDefault
		{
			get { return infoPolicyDefault; }
			set { infoPolicyDefault = value; }
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
				throw new AerospikeException(NotSupported + "GetCluster");
			}
		}

		/// <summary>
		/// Return array of active server nodes in the cluster.
		/// </summary>
		public Node[] Nodes
		{
			get
			{
				throw new AerospikeException(NotSupported + "GetNodes");
			}
		}

		/// <summary>
		/// Return operating cluster statistics.
		/// </summary>
		public ClusterStats GetClusterStats()
		{
			throw new AerospikeException(NotSupported + "GetClusterStats");
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
			WriteCommand command = new(null, policy, key, bins, Operation.Type.WRITE);
			command.ExecuteGRPC(channel);
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
			WriteCommand command = new(null, policy, key, bins, Operation.Type.APPEND);
			command.ExecuteGRPC(channel);
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
			WriteCommand command = new(null, policy, key, bins, Operation.Type.PREPEND);
			command.ExecuteGRPC(channel);
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
			WriteCommand command = new(null, policy, key, bins, Operation.Type.ADD);
			command.ExecuteGRPC(channel);
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
			DeleteCommand command = new(null, policy, key);
			command.ExecuteGRPC(channel);
			return command.Existed();
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
		public BatchResults Delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)
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
				Operate(batchPolicy, records, status);
				return new BatchResults(records, status.GetStatus());
			}
			catch (Exception e)
			{
				// Batch terminated on fatal error.
				throw new AerospikeException.BatchRecordArray(records, e);
			}
		}

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
			TouchCommand command = new(null, policy, key);
			command.ExecuteGRPC(channel);
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
			ExistsCommand command = new(null, policy, key);
			command.ExecuteGRPC(channel);
			return command.Exists();
		}

		/// <summary>
		/// Check if multiple record keys exist in one batch call.
		/// The returned boolean array is in positional order with the original key array order.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchExists">which contains results for keys that did complete</exception>
		public bool[] Exists(BatchPolicy policy, Key[] keys)
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
				Operate(policy, records, status);
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
			ReadCommand command = new(null, policy, key);
			command.ExecuteGRPC(channel);
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
			policy ??= readPolicyDefault;
			ReadCommand command = new(null, policy, key, binNames);
			command.ExecuteGRPC(channel);
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
			policy ??= readPolicyDefault;
			ReadHeaderCommand command = new(null, policy, key);
			command.ExecuteGRPC(channel);
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
		/// <returns>true if all batch key requests succeeded</returns>
		/// <exception cref="AerospikeException">if read fails</exception>
		public bool Get(BatchPolicy policy, List<BatchRead> records)
		{
			if (records.Count == 0)
			{
				return true;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			BatchStatus status = new(true);
			Operate(policy, records.ToArray(), status);
			return status.GetStatus();
		}

		/// <summary>
		/// Read multiple records for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		public Record[] Get(BatchPolicy policy, Key[] keys)
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
				Operate(policy, batchRecords, status);
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
		/// Read multiple record headers and bins for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="binNames">array of bins to retrieve</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		public Record[] Get(BatchPolicy policy, Key[] keys, params string[] binNames)
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
				Operate(policy, batchRecords, status);
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
		/// Read multiple records for specified keys using read operations in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <param name="ops">array of read operations on record</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		public Record[] Get(BatchPolicy policy, Key[] keys, params Operation[] ops)
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
				Operate(policy, batchRecords, status);
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
		/// Read multiple record header data for specified keys in one batch call.
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		/// </summary>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException.BatchRecords">which contains results for keys that did complete</exception>
		public Record[] GetHeader(BatchPolicy policy, Key[] keys)
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
				Operate(policy, batchRecords, status);
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

		// Not supported in proxy client
		public Record Join(BatchPolicy policy, Key key, string[] binNames, params Join[] joins)
		{
			throw new AerospikeException(NotSupported + "Join");
		}

		// Not supported in proxy client
		public Record Join(BatchPolicy policy, Key key, params Join[] joins)
		{
			throw new AerospikeException(NotSupported + "Join");
		}

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
			OperateArgs args = new(policy, writePolicyDefault, operatePolicyReadDefault, key, operations);
			OperateCommand command = new(null, key, args);
			command.ExecuteGRPC(channel);
			return command.Record;
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
		/// <returns>true if all batch sub-commands succeeded</returns>
		/// <exception cref="AerospikeException">if command fails</exception>
		public bool Operate(BatchPolicy policy, List<BatchRecord> records)
		{
			policy ??= batchParentPolicyWriteDefault;

			BatchStatus status = new(true);
			Operate(policy, records.ToArray(), status);
			return status.GetStatus();
		}

		private void Operate(BatchPolicy policy, BatchRecord[] records, BatchStatus status)
		{
			policy ??= batchParentPolicyWriteDefault;

			BatchNode batch = new(records);
			BatchOperateListCommand command = new(null, batch, policy, records, status);
			command.ExecuteGRPC(channel);
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
		public BatchResults Operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, params Operation[] ops)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(Array.Empty<BatchRecord>(), true);
			}

			batchPolicy ??= batchParentPolicyWriteDefault;
			writePolicy ??= batchWritePolicyDefault;

			BatchAttr attr = new BatchAttr(batchPolicy, writePolicy, ops);
			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			try
			{
				BatchStatus status = new(true);
				BatchNode batchNode = new(records);
				BatchOperateArrayCommand command = new(null, batchNode, batchPolicy, keys, ops, records, attr, status);
				command.ExecuteGRPC(channel);
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
		/// <exception cref="AerospikeException">if scan fails</exception>
		public void ScanAll(ScanPolicy policy, string ns, string setName, ScanCallback callback, params string[] binNames)
		{
			ScanPartitions(policy, PartitionFilter.All(), ns, setName, callback, binNames);
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
			policy ??= scanPolicyDefault;
			CancellationToken token = new();
			PartitionTracker tracker = new(policy, null, partitionFilter);
			RecordSet recordSet = new(null, policy.recordQueueSize, token);
			ScanPartitionCommandProxy command = new(policy, ns, setName, binNames, tracker, partitionFilter, recordSet);
			command.ExecuteGRPC(channel);
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

			var command = new ExecuteCommand(null, policy, key, packageName, functionName, args);
			command.ExecuteGRPC(channel);

			var record = command.Record;

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
		public BatchResults Execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, string packageName, string functionName, params Value[] functionArgs)
		{
			if (keys.Length == 0)
			{
				return new BatchResults(Array.Empty<BatchRecord>(), true);
			}

			batchPolicy ??= batchParentPolicyWriteDefault;
			udfPolicy ??= batchUDFPolicyDefault;

			byte[] argBytes = Packer.Pack(functionArgs);

			BatchAttr attr = new BatchAttr();
			attr.SetUDF(udfPolicy);

			BatchRecord[] records = new BatchRecord[keys.Length];

			for (int i = 0; i < keys.Length; i++)
			{
				records[i] = new BatchUDF(udfPolicy, keys[i], packageName, functionName, functionArgs);
			}

			try
			{
				BatchStatus status = new(true);
				Operate(batchPolicy, records, status);
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
			policy ??= writePolicyDefault;

			statement.PackageName = packageName;
			statement.FunctionName = functionName;
			statement.FunctionArgs = functionArgs;

			ulong taskId = statement.PrepareTaskId();
			ServerCommand command = new(null, null, policy, statement, taskId);
			command.ExecuteGRPC(channel);
				
			return new ExecuteTaskProxy(channel, policy, statement, taskId);
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
			policy ??= writePolicyDefault;

			statement.Operations = operations;

			ulong taskId = statement.PrepareTaskId();
			ServerCommand command = new(null, null, policy, statement, taskId);
			command.ExecuteGRPC(channel);

			return new ExecuteTaskProxy(channel, policy, statement, taskId);
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
		/// <exception cref="AerospikeException">if query fails</exception>
		public RecordSet Query(QueryPolicy policy, Statement statement)
		{
			return QueryPartitions(policy, statement, PartitionFilter.All());
		}

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
		public void Query
		(
			QueryPolicy policy,
			Statement statement,
			PartitionFilter partitionFilter,
			QueryListener listener
		)
		{
			QueryPartitions(policy, statement, partitionFilter);
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
		/// <exception cref="AerospikeException">if query fails</exception>
		public RecordSet QueryPartitions
		(
			QueryPolicy policy,
			Statement statement,
			PartitionFilter partitionFilter
		)
		{
			CancellationToken token = new();
			policy ??= queryPolicyDefault;
			PartitionTracker tracker = new(policy, statement, (Node[])null, partitionFilter);
			RecordSet recordSet = new(null, policy.recordQueueSize, token);
			QueryPartitionCommandProxy command = new(policy, null, statement, null, tracker, partitionFilter, recordSet);
			command.ExecuteGRPC(channel);
			return recordSet;
		}

		/// Not supported in proxy client
		public ResultSet QueryAggregate
		(
			QueryPolicy policy,
			Statement statement,
			string packageName,
			string functionName,
			params Value[] functionArgs
		)
		{
			throw new AerospikeException(NotSupported + "QueryAggregate");
		}

		/// Not supported in proxy client
		public void QueryAggregate(QueryPolicy policy, Statement statement, Action<Object> action)
		{
			throw new AerospikeException(NotSupported + "QueryAggregate");
		}

		/// Not supported in proxy client
		public ResultSet QueryAggregate(QueryPolicy policy, Statement statement)
		{
			throw new AerospikeException(NotSupported + "QueryAggregate");
		}

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
			throw new AerospikeException(NotSupported + "CreateIndex");
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
			throw new AerospikeException(NotSupported + "CreateIndex");
		}
		
		/// Not supported by proxy client
		public IndexTask DropIndex(Policy policy, string ns, string setName, string indexName)
		{
			throw new AerospikeException(NotSupported + "DropIndex");
		}

		//-----------------------------------------------------------------
		// XDR - Cross datacenter replication
		//-----------------------------------------------------------------

		/// Not supported in proxy client
		public void SetXDRFilter(InfoPolicy policy, string datacenter, string ns, Expression filter)
		{
			throw new AerospikeException(NotSupported + "SetXDRFilter");
		}

		//-------------------------------------------------------
		// User administration
		//-------------------------------------------------------

		/// Not supported in proxy client		
		public void CreateUser(AdminPolicy policy, string user, string password, IList<string> roles)
		{
			throw new AerospikeException(NotSupported + "CreateUser");
		}

		/// Not supported in proxy client
		public void DropUser(AdminPolicy policy, string user)
		{
			throw new AerospikeException(NotSupported + "DropUser");
		}

		/// Not supported in proxy client
		public void ChangePassword(AdminPolicy policy, string user, string password)
		{
			throw new AerospikeException(NotSupported + "ChangePassword");
		}

		/// Not supported in proxy client
		public void GrantRoles(AdminPolicy policy, string user, IList<string> roles)
		{
			throw new AerospikeException(NotSupported + "GrantRoles");
		}

		/// Not supported in proxy client
		public void RevokeRoles(AdminPolicy policy, string user, IList<string> roles)
		{
			throw new AerospikeException(NotSupported + "RevokeRoles");
		}

		/// Not supported in proxy client
		public void CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			throw new AerospikeException(NotSupported + "CreateRole");
		}

		/// Not supported in proxy client
		public void CreateRole(AdminPolicy policy, string roleName, IList<Privilege> privileges, IList<string> whitelist)
		{
			throw new AerospikeException(NotSupported + "CreateRole");
		}

		/// Not supported in proxy client
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
			throw new AerospikeException(NotSupported + "CreateRole");
		}

		/// Not supported in proxy client
		public void DropRole(AdminPolicy policy, string roleName)
		{
			throw new AerospikeException(NotSupported + "DropRole");
		}

		/// Not supported in proxy client
		public void GrantPrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			throw new AerospikeException(NotSupported + "GrantPrivileges");
		}

		/// Not supported in proxy client
		public void RevokePrivileges(AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			throw new AerospikeException(NotSupported + "RevokePrivileges");
		}

		/// Not supported in proxy client
		public void SetWhitelist(AdminPolicy policy, string roleName, IList<string> whitelist)
		{
			throw new AerospikeException(NotSupported + "SetWhitelist");
		}

		/// Not supported in proxy client
		public void SetQuotas(AdminPolicy policy, string roleName, int readQuota, int writeQuota)
		{
			throw new AerospikeException(NotSupported + "SetQuotas");
		}

		/// Not supported in proxy client
		public User QueryUser(AdminPolicy policy, string user)
		{
			throw new AerospikeException(NotSupported + "QueryUser");
		}

		/// Not supported in proxy client
		public List<User> QueryUsers(AdminPolicy policy)
		{
			throw new AerospikeException(NotSupported + "QueryUsers");
		}

		/// Not supported in proxy client
		public Role QueryRole(AdminPolicy policy, string roleName)
		{
			throw new AerospikeException(NotSupported + "QueryRole");
		}

		/// Not supported in proxy client
		public List<Role> QueryRoles(AdminPolicy policy)
		{
			throw new AerospikeException(NotSupported + "QueryRoles");
		}		
	}
}
