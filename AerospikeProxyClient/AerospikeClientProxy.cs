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

		// Lower limit of proxy server connection.
		private static readonly int MIN_CONNECTIONS = 1;

		/// Is threadPool shared between other client instances or classes.  If threadPool is
		/// not shared (default), threadPool will be shutdown when the client instance is closed.
		/// <para>
		/// If threadPool is shared, threadPool will not be shutdown when the client instance is
		/// closed. This shared threadPool should be shutdown manually before the program
		/// terminates.  Shutdown is recommended, but not absolutely required if threadPool is
		/// constructed to use daemon threads.
		/// </para>
		/// Default: false
		private readonly bool sharedThreadPool;

		/// Underlying thread pool used in synchronous batch, scan, and query commands. These commands
		/// are often sent to multiple server nodes in parallel threads.  A thread pool improves
		/// performance because threads do not have to be created/destroyed for each command.
		/// The default, null, indicates that the following daemon thread pool will be used:
		/// <pre>
		/// threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
		///     public final Thread newThread(Runnable runnable) {
		/// 			Thread thread = new Thread(runnable);
		/// 			thread.setDaemon(true);
		/// 			return thread;
		///        }
		///    });
		/// </pre>
		/// Daemon threads automatically terminate when the program terminates.
		/// <p>
		/// Default: null (use Executors.newCachedThreadPool)
		/// </p>
		//private readonly ExecutorService threadPool;

		// Upper limit of proxy server connection.
		private static readonly int MAX_CONNECTIONS = 8;

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

		private GrpcChannel Channel { get; set; }

		private KVS.KVS.KVSClient KVS { get; set; }

		internal byte[] buffer;
		internal int offset;

		public const int MSG_TOTAL_HEADER_SIZE = 30;
		public const int FIELD_HEADER_SIZE = 5;
		public const int OPERATION_HEADER_SIZE = 8;
		public const int MSG_REMAINING_HEADER_SIZE = 22;
		public const ulong CL_MSG_VERSION = 2UL;
		public const ulong AS_MSG_TYPE = 3UL;
		public const int COMPRESS_THRESHOLD = 128;
		public const ulong MSG_TYPE_COMPRESSED = 4UL;
		internal readonly int serverTimeout;
		private int resultCode;
		int generation;
		int expiration;
		int batchIndex;
		int fieldCount;
		int opCount;
		int info3;

		//private readonly AuthTokenManager authTokenManager;
		//private readonly GrpcCallExecutor executor;

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

			Channel = GrpcChannel.ForAddress(new UriBuilder("http", hosts[0].name, hosts[0].port).Uri);
			KVS = new KVS.KVS.KVSClient(Channel);

			offset = 0;
			buffer = ThreadLocalData.GetBuffer();

			if (writePolicyDefault.totalTimeout > 0)
			{
				var socketTimeout = (writePolicyDefault.socketTimeout < writePolicyDefault.totalTimeout && writePolicyDefault.socketTimeout > 0) ? writePolicyDefault.socketTimeout : writePolicyDefault.totalTimeout;
				this.serverTimeout = socketTimeout;
			}
			else
			{
				this.serverTimeout = 0;
			}

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
				executor.close();
			}
			catch (Exception e)
			{
				Log.Warn("Failed to close grpcCallExecutor: " + Util.GetErrorMessage(e));
			}

			try
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
			if (policy == null)
			{
				policy = writePolicyDefault;
			}

			SetWrite(policy, Operation.Type.WRITE, key, bins);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(buffer),
				//WritePolicy = GRPCConversions.ToGrpc(policy)
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			//var options = 
			var response = KVS.Write(request);
			ParseResult(policy);
			//WriteCommand command = new(cluster, policy, key, bins, Operation.Type.WRITE);
			//command.Execute();
		}

		public void SetWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				offset += policy.filterExp.Size();
				fieldCount++;
			}

			foreach (Bin bin in bins)
			{
				EstimateOperationSize(bin);
			}

			bool compress = SizeBuffer(policy);

			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, bins.Length);
			WriteKey(policy, key);

			/*if (policy.filterExp != null)
			{
				policy.filterExp.Write(this);
			}*/

			foreach (Bin bin in bins)
			{
				WriteOperation(bin, operation);
			}
			End(compress);
		}

		private void WriteKey(Policy policy, Key key)
		{
			// Write key into buffer.
			if (key.ns != null)
			{
				WriteField(key.ns, FieldType.NAMESPACE);
			}

			if (key.setName != null)
			{
				WriteField(key.setName, FieldType.TABLE);
			}

			WriteField(key.digest, FieldType.DIGEST_RIPE);

			if (policy.sendKey)
			{
				WriteField(key.userKey, FieldType.KEY);
			}
		}

		private void WriteField(Value value, int type)
		{
			offset = offset + FIELD_HEADER_SIZE;
			buffer[offset++] = (byte)value.Type;
			int len = value.Write(buffer, offset) + 1;
			WriteFieldHeader(len, type);
			offset += len;
		}

		private void WriteField(string str, int type)
		{
			int len = ByteUtil.StringToUtf8(str, buffer, offset + FIELD_HEADER_SIZE);
			WriteFieldHeader(len, type);
			offset += len;
		}

		private void WriteField(byte[] bytes, int type)
		{
			Array.Copy(bytes, 0, buffer, offset + FIELD_HEADER_SIZE, bytes.Length);
			WriteFieldHeader(bytes.Length, type);
			offset += bytes.Length;
		}

		private void WriteField(int val, int type)
		{
			WriteFieldHeader(4, type);
			offset += ByteUtil.IntToBytes((uint)val, buffer, offset);
		}

		private void WriteField(ulong val, int type)
		{
			WriteFieldHeader(8, type);
			offset += ByteUtil.LongToBytes(val, buffer, offset);
		}

		private void WriteFieldHeader(int size, int type)
		{
			offset += ByteUtil.IntToBytes((uint)size + 1, buffer, offset);
			buffer[offset++] = (byte)type;
		}

		private void WriteOperation(Bin bin, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(bin.name, buffer, offset + OPERATION_HEADER_SIZE);
			int valueLength = bin.value.Write(buffer, offset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), buffer, offset);
			offset += 4;
			buffer[offset++] = Operation.GetProtocolType(operationType);
			buffer[offset++] = (byte)bin.value.Type;
			buffer[offset++] = (byte)0;
			buffer[offset++] = (byte)nameLength;
			offset += nameLength + valueLength;
		}

		private void End(bool compress)
		{
			if (!compress)
			{
				End();
				return;
			}

			// Write proto header.
			ulong size = ((ulong)offset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, buffer, 0);

			byte[] srcBuf = buffer;
			int srcSize = offset;

			// Increase requested buffer size in case compressed buffer size is
			// greater than the uncompressed buffer size.
			offset += 16 + 100;

			// This method finds buffer of requested size, resets offset to segment offset
			// and returns buffer max size;
			int trgBufSize = SizeBuffer();

			// Compress to target starting at new offset plus new header.
			int trgSize = ByteUtil.Compress(srcBuf, srcSize, buffer, offset + 16, trgBufSize - 16) + 16;

			ulong proto = ((ulong)trgSize - 8) | (CL_MSG_VERSION << 56) | (MSG_TYPE_COMPRESSED << 48);
			ByteUtil.LongToBytes(proto, buffer, offset);
			ByteUtil.LongToBytes((ulong)srcSize, buffer, offset + 8);
			SetLength(trgSize);
		}

		private void WriteHeaderWrite(WritePolicy policy, int writeAttr, int fieldCount, int operationCount)
		{
			// Set flags.
			int generation = 0;
			int infoAttr = 0;

			switch (policy.recordExistsAction)
			{
				case RecordExistsAction.UPDATE:
					break;
				case RecordExistsAction.UPDATE_ONLY:
					infoAttr |= Command.INFO3_UPDATE_ONLY;
					break;
				case RecordExistsAction.REPLACE:
					infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
					break;
				case RecordExistsAction.REPLACE_ONLY:
					infoAttr |= Command.INFO3_REPLACE_ONLY;
					break;
				case RecordExistsAction.CREATE_ONLY:
					writeAttr |= Command.INFO2_CREATE_ONLY;
					break;
			}

			switch (policy.generationPolicy)
			{
				case GenerationPolicy.NONE:
					break;
				case GenerationPolicy.EXPECT_GEN_EQUAL:
					generation = policy.generation;
					writeAttr |= Command.INFO2_GENERATION;
					break;
				case GenerationPolicy.EXPECT_GEN_GT:
					generation = policy.generation;
					writeAttr |= Command.INFO2_GENERATION_GT;
					break;
			}

			if (policy.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= Command.INFO3_COMMIT_MASTER;
			}

			if (policy.durableDelete)
			{
				writeAttr |= Command.INFO2_DURABLE_DELETE;
			}

			offset += 8;

			// Write all header data except total size which must be written last. 
			buffer[offset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			buffer[offset++] = (byte)0;
			buffer[offset++] = (byte)writeAttr;
			buffer[offset++] = (byte)infoAttr;
			buffer[offset++] = 0; // unused
			buffer[offset++] = 0; // clear the result code
			offset += ByteUtil.IntToBytes((uint)generation, buffer, offset);
			offset += ByteUtil.IntToBytes((uint)policy.expiration, buffer, offset);
			offset += ByteUtil.IntToBytes((uint)serverTimeout, buffer, offset);
			offset += ByteUtil.ShortToBytes((ushort)fieldCount, buffer, offset);
			offset += ByteUtil.ShortToBytes((ushort)operationCount, buffer, offset);
		}

		private void Begin()
		{
			offset = MSG_TOTAL_HEADER_SIZE;
		}

		private void EstimateOperationSize()
		{
			offset += OPERATION_HEADER_SIZE;
		}

		private void EstimateOperationSize(Bin bin)
		{
			offset += ByteUtil.EstimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
			offset += bin.value.EstimateSize();
		}

		private bool SizeBuffer(Policy policy)
		{
			if (policy.compress && offset > COMPRESS_THRESHOLD)
			{
				// Command will be compressed. First, write uncompressed command
				// into separate buffer. Save normal buffer for compressed command.
				// Normal buffer in async mode is from buffer pool that is used to
				// minimize memory pinning during socket operations.
				buffer = new byte[offset];
				offset = 0;
				return true;
			}
			else
			{
				// Command will be uncompressed.
				SizeBuffer();
				return false;
			}
		}

		internal int SizeBuffer()
		{
			buffer = ThreadLocalData.GetBuffer();

			if (offset > buffer.Length)
			{
				buffer = ThreadLocalData.ResizeBuffer(offset);
			}
			offset = 0;
			return buffer.Length;
		}

		internal void SizeBuffer(int size)
		{
			if (size > buffer.Length)
			{
				buffer = ThreadLocalData.ResizeBuffer(size);
			}
		}

		internal void End()
		{
			// Write total size of message.
			ulong size = ((ulong)offset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, buffer, 0);
		}

		internal void SetLength(int length)
		{
			offset = length;
		}

		private int EstimateKeySize(Policy policy, Key key)
		{
			int fieldCount = 0;

			if (key.ns != null)
			{
				offset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (key.setName != null)
			{
				offset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			offset += key.digest.Length + FIELD_HEADER_SIZE;
			fieldCount++;

			if (policy.sendKey)
			{
				offset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}
			return fieldCount;
		}

		int ParseResult(Policy policy)
		{
			int resultCode = ParseResultCode();

			switch (resultCode)
			{
				case ResultCode.OK:
					break;

				case ResultCode.FILTERED_OUT:
					if (policy.failOnFilteredOut)
					{
						throw new AerospikeException(resultCode);
					}
					break;

				default:
					throw new AerospikeException(resultCode);
			}

			return resultCode;
		}

		/// <summary>
		/// Asynchronously write record bin(s).
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void Put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin...bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.WRITE);
			command.execute();
		}*/

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
			//WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.APPEND);
			//command.Execute();
		}

		/// <summary>
		/// Asynchronously append bin string values to existing record bin values.
		/// This call only works for string values.
		/// </summary>
		///
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void Append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin...bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.APPEND);
			command.execute();
		}*/

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
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			//WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.PREPEND);
			//command.Execute();
		}

		/// <summary>
		/// Asynchronously prepend bin string values to existing record bin values.
		/// This call only works for string values.
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin...bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.PREPEND);
			command.execute();
		}*/

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
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			//WriteCommand command = new WriteCommand(cluster, policy, key, bins, Operation.Type.ADD);
			//command.Execute();
		}

		/// <summary>
		/// Asynchronously add integer/double bin values to existing record bin values.
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="bins">array of bin name/value pairs</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin...bins)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			WriteCommandProxy command = new WriteCommandProxy(executor, listener, policy, key, bins, Operation.Type.ADD);
			command.execute();
		}*/

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
			//DeleteCommand command = new DeleteCommand(cluster, policy, key);
			//command.Execute();
			//return command.Existed();
			return false;
		}

		/// <summary>
		/// Asynchronously delete record for specified key.
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		////
		/*public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			DeleteCommandProxy command = new DeleteCommandProxy(executor, listener, policy, key);
			command.execute();
		}*/


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

		/// <summary>
		/// Asynchronously delete records for specified keys.
		/// <p>
		/// If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
		/// {@link ResultCode#KEY_NOT_FOUND_ERROR}.
		/// </p>
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="deletePolicy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void delete(
			EventLoop eventLoop,
			BatchRecordArrayListener listener,
			BatchPolicy batchPolicy,
			BatchDeletePolicy deletePolicy,
			Key[] keys
		)
		{
			if (keys.length == 0)
			{
				listener.onSuccess(new BatchRecord[0], true);
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
			attr.setDelete(deletePolicy);

			CommandProxy command = new BatchProxy.OperateRecordArrayCommand(executor,
				batchPolicy, keys, null, listener, attr);

			command.execute();
		}*/

		/// <summary>
		/// Asynchronously delete records for specified keys.
		/// <p>
		/// Each record result is returned in separate onRecord() calls.
		/// If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
		/// {@link ResultCode#KEY_NOT_FOUND_ERROR}.
		/// </p>
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="batchPolicy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="deletePolicy">delete configuration parameters, pass in null for defaults</param>
		/// <param name="keys">array of unique record identifiers</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void Delete(
			EventLoop eventLoop,
			BatchRecordSequenceListener listener,
			BatchPolicy batchPolicy,
			BatchDeletePolicy deletePolicy,
			Key[] keys
		)
		{
			if (keys.length == 0)
			{
				listener.onSuccess();
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
			attr.setDelete(deletePolicy);

			CommandProxy command = new BatchProxy.OperateRecordSequenceCommand(executor,
				batchPolicy, keys, null, listener, attr);

			command.execute();
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
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			//TouchCommand command = new TouchCommand(cluster, policy, key);
			//command.Execute();
		}

		/// <summary>
		/// Asynchronously reset record's time to expiration using the policy's expiration.
		/// Fail if the record does not exist.
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results, pass in null for fire and forget</param>
		/// <param name="policy">write configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void Touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			TouchCommandProxy command = new TouchCommandProxy(executor, listener, policy, key);
			command.execute();
		}*/

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
			//ExistsCommand command = new ExistsCommand(cluster, policy, key);
			//command.Execute();
			//return command.Exists();
			return false;
		}

		/// <summary>
		/// Asynchronously determine if a record key exists.
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void Exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			ExistsCommandProxy command = new ExistsCommandProxy(executor, listener, policy, key);
			command.execute();
		}*/

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

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// <p>
		/// The returned boolean array is in positional order with the original key array order.
		/// </p>
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">unique record identifiers</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys)
		{
			if (keys.length == 0)
			{
				listener.onSuccess(keys, new boolean[0]);
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.ExistsArrayCommand(executor, policy, listener, keys);
			command.execute();
		}*/

		/// <summary>
		/// Asynchronously check if multiple record keys exist in one batch call.
		/// <p>
		/// Each key's result is returned in separate onExists() calls.
		/// </p>
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="keys">unique record identifiers</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys)
		{
			if (keys.length == 0)
			{
				listener.onSuccess();
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.ExistsSequenceCommand(executor, policy, listener, keys);
			command.execute();
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
			buffer = ThreadLocalData.GetBuffer();
			Debugger.Launch();
			if (policy == null)
			{
				policy = readPolicyDefault;
			}

			string[] binNames = null;

			SetRead(policy, key, binNames);

			var request = new AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				Payload = ByteString.CopyFrom(buffer),
				//ReadPolicy = GRPCConversions.SetRequestPolicy(policy)
			};
			GRPCConversions.SetRequestPolicy(policy, request);
			//var options = 
			var response = KVS.Read(request);
			return ParseRecordResult(policy);

			//ReadCommand command = new ReadCommand(cluster, policy, key);
			//command.Execute();
			//return command.Record;
		}

		public int ParseResultCode()
		{
			return buffer[offset] & 0xFF;
		}

		public int ParseHeader()
		{
			resultCode = ParseResultCode();
			offset += 1;
			generation = ByteUtil.BytesToInt(buffer, offset);
			offset += 4;
			expiration = ByteUtil.BytesToInt(buffer, offset);
			offset += 4;
			batchIndex = ByteUtil.BytesToInt(buffer, offset);
			offset += 4;
			fieldCount = ByteUtil.BytesToShort(buffer, offset);
			offset += 2;
			opCount = ByteUtil.BytesToShort(buffer, offset);
			offset += 2;
			return resultCode;
		}

		internal Record ParseRecordResult(Policy policy)
		{
			Record record = null;
			int resultCode = ParseHeader();

			switch (resultCode)
			{
				case ResultCode.OK:
					SkipKey();
					if (opCount == 0)
					{
						// Bin data was not returned.
						record = new Record(null, generation, expiration);
					}
					else
					{
						record = ParseRecord(buffer, ref offset, opCount, generation, expiration, false);
					}
					break;

				case ResultCode.KEY_NOT_FOUND_ERROR:
					//handleNotFound(resultCode);
					break;

				case ResultCode.FILTERED_OUT:
					if (policy.failOnFilteredOut)
					{
						throw new AerospikeException(resultCode);
					}
					break;

				case ResultCode.UDF_BAD_RESPONSE:
					SkipKey();
					record = ParseRecord(buffer, ref offset, opCount, generation, expiration, false);
					//handleUdfError(record, resultCode);
					break;

				default:
					throw new AerospikeException(resultCode);
			}

			return record;
		}

		public Record ParseRecord(byte[] dataBuffer, ref int dataOffsetRef, int opCount, int generation, int expiration, bool isOperation)
		{
			Dictionary<String, Object> bins = new Dictionary<string, object>();
			int dataOffset = dataOffsetRef;

			for (int i = 0; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				byte particleType = dataBuffer[dataOffset + 5];
				byte nameSize = dataBuffer[dataOffset + 7];
				String name = ByteUtil.Utf8ToString(dataBuffer, dataOffset + 8, nameSize);
				dataOffset += 4 + 4 + nameSize;

				int particleBytesSize = opSize - (4 + nameSize);
				Object value = ByteUtil.BytesToParticle((ParticleType)particleType, dataBuffer, dataOffset, particleBytesSize);
				dataOffset += particleBytesSize;

				if (isOperation)
				{
					object prev;

					if (bins.TryGetValue(name, out prev))
					{
						// Multiple values returned for the same bin. 
						if (prev is RecordParser.OpResults)
						{
							// List already exists.  Add to it.
							RecordParser.OpResults list = (RecordParser.OpResults)prev;
							list.Add(value);
						}
						else
						{
							// Make a list to store all values.
							RecordParser.OpResults list = new RecordParser.OpResults();
							list.Add(prev);
							list.Add(value);
							bins[name] = list;
						}
					}
					else
					{
						bins[name] = value;
					}
				}
				else
				{
					bins[name] = value;
				}
			}
			dataOffsetRef = dataOffset;
			return new Record(bins, generation, expiration);
		}

		public void SkipKey()
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(buffer, offset);
				offset += 4 + fieldlen;
			}
		}

		public void SetRead(Policy policy, Key key, string[] binNames)
		{
			if (binNames != null)
			{
				Begin();
				int fieldCount = EstimateKeySize(policy, key);

				if (policy.filterExp != null)
				{
					offset += policy.filterExp.Size();
					fieldCount++;
				}

				foreach (string binName in binNames)
				{
					EstimateOperationSize(binName);
				}
				SizeBuffer();
				WriteHeaderRead(policy, serverTimeout, Command.INFO1_READ, 0, fieldCount, binNames.Length);
				WriteKey(policy, key);

				/*if (policy.filterExp != null)
				{
					policy.filterExp.Write(this);
				}*/

				foreach (string binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
				End();
			}
			else
			{
				SetRead(policy, key);
			}
		}

		private void WriteOperation(string name, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(name, buffer, offset + OPERATION_HEADER_SIZE);

			ByteUtil.IntToBytes((uint)(nameLength + 4), buffer, offset);
			offset += 4;
			buffer[offset++] = Operation.GetProtocolType(operationType);
			buffer[offset++] = (byte)0;
			buffer[offset++] = (byte)0;
			buffer[offset++] = (byte)nameLength;
			offset += nameLength;
		}

		private void EstimateOperationSize(string binName)
		{
			offset += ByteUtil.EstimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
		}

		public void SetRead(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				offset += policy.filterExp.Size();
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderRead(policy, serverTimeout, Command.INFO1_READ | Command.INFO1_GET_ALL, 0, fieldCount, 0);
			WriteKey(policy, key);

			/*if (policy.filterExp != null)
			{
				policy.filterExp.Write(this);
			}*/
			End();
		}

		public void SetReadHeader(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				offset += policy.filterExp.Size();
				fieldCount++;
			}
			EstimateOperationSize((string)null);
			SizeBuffer();
			WriteHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(policy, key);

			/*if (policy.filterExp != null)
			{
				policy.filterExp.Write(this);
			}*/
			End();
		}

		private void WriteHeaderReadHeader(Policy policy, int readAttr, int fieldCount, int operationCount)
		{
			int infoAttr = 0;

			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= Command.INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= Command.INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			offset += 8;

			// Write all header data except total size which must be written last. 
			buffer[offset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			buffer[offset++] = (byte)readAttr;
			buffer[offset++] = (byte)0;
			buffer[offset++] = (byte)infoAttr;

			for (int i = 0; i < 10; i++)
			{
				buffer[offset++] = 0;
			}
			offset += ByteUtil.IntToBytes((uint)serverTimeout, buffer, offset);
			offset += ByteUtil.ShortToBytes((ushort)fieldCount, buffer, offset);
			offset += ByteUtil.ShortToBytes((ushort)operationCount, buffer, offset);
		}

		private void WriteHeaderRead
		(
			Policy policy,
			int timeout,
			int readAttr,
			int infoAttr,
			int fieldCount,
			int operationCount
		)
		{
			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= Command.INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= Command.INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			if (policy.compress)
			{
				readAttr |= Command.INFO1_COMPRESS_RESPONSE;
			}

			offset += 8;

			// Write all header data except total size which must be written last. 
			buffer[offset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			buffer[offset++] = (byte)readAttr;
			buffer[offset++] = (byte)0;
			buffer[offset++] = (byte)infoAttr;

			for (int i = 0; i < 10; i++)
			{
				buffer[offset++] = 0;
			}
			offset += ByteUtil.IntToBytes((uint)timeout, buffer, offset);
			offset += ByteUtil.ShortToBytes((ushort)fieldCount, buffer, offset);
			offset += ByteUtil.ShortToBytes((ushort)operationCount, buffer, offset);
		}

		/// <summary>
		/// Asynchronously read entire record for specified key.
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)
		{
			Get(eventLoop, listener, policy, key, (String[])null);
		}*/

		/// <summary>
		/// Read record header and bins for specified key.
		/// If found, return record instance.  If not found, return null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		/*public Record Get(Policy policy, Key key, params string[] binNames)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			//ReadCommand command = new ReadCommand(cluster, policy, key, binNames);
			//command.Execute();
			//return command.Record;
		}*/

		/// <summary>
		/// Asynchronously read record header and bins for specified key.
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <param name="binNames">bins to retrieve</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String...binNames)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			ReadCommandProxy command = new ReadCommandProxy(executor, listener, policy, key, binNames);
			command.execute();
		}*/

		/// <summary>
		/// Read record generation and expiration only for specified key.  Bins are not read.
		/// If found, return record instance.  If not found, return null.
		/// The policy can be used to specify timeouts.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if read fails</exception>
		/*public Record GetHeader(Policy policy, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			//ReadHeaderCommand command = new ReadHeaderCommand(cluster, policy, key);
			//command.Execute();
			//return command.Record;
		}*/

		/// <summary>
		/// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique record identifier</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void GetHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)
		{
			if (policy == null)
			{
				policy = readPolicyDefault;
			}
			ReadHeaderCommandProxy command = new ReadHeaderCommandProxy(executor, listener, policy, key);
			command.execute();
		}*/

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
		/// Asynchronously read multiple records for specified batch keys in one batch call.
		/// <p>
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// The returned records are located in the same list.
		/// If the BatchRead key field is not found, the corresponding record field will be null.
		/// </p>
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="records">list of unique record identifiers and the bins to retrieve.
		///	The returned records are located in the same list.</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void Get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records)
		{
			if (records.size() == 0)
			{
				listener.onSuccess(records);
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}
			CommandProxy command = new BatchProxy.ReadListCommand(executor, policy, listener, records);
			command.execute();
		}*/

		/// <summary>
		/// Asynchronously read multiple records for specified batch keys in one batch call.
		/// <p>
		/// This method allows different namespaces/bins to be requested for each key in the batch.
		/// Each record result is returned in separate onRecord() calls.
		/// If the BatchRead key field is not found, the corresponding record field will be null.
		/// </p>
		/// </summary>
		/// <param name="eventLoop">ignored, pass in null</param>
		/// <param name="listener">where to send results</param>
		/// <param name="policy">batch configuration parameters, pass in null for defaults</param>
		/// <param name="records">list of unique record identifiers and the bins to retrieve.
		///	The returned records are located in the same list.</param>
		/// <exception cref="AerospikeException">if event loop registration fails</exception>
		/*public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) {
			if (records.size() == 0) {
				listener.onSuccess();
				return;
			}

			if (policy == null) {
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.ReadSequenceCommand(executor, policy, listener, records);
			command.execute();
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

		/// Asynchronously read multiple records for specified keys in one batch call.
		/// <p>
		/// The returned records are in positional order with the original key array order.
		/// If a key is not found, the positional record will be null.
		///
		/// @param eventLoop		ignored, pass in null
		/// @param listener		where to send results
		/// @param policy		batch configuration parameters, pass in null for defaults
		/// @param keys			array of unique record identifiers
		/// @throws AerospikeException	if event loop registration fails
		/*public void Get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)
		{
			if (keys.length == 0)
			{
				listener.onSuccess(keys, new Record[0]);
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.GetArrayCommand(executor, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
			command.execute();
		}*/

		/// Asynchronously read multiple records for specified keys in one batch call.
		/// <p>
		/// Each record result is returned in separate onRecord() calls.
		/// If a key is not found, the record will be null.
		///
		/// @param eventLoop		ignored, pass in null
		/// @param listener		where to send results
		/// @param policy		batch configuration parameters, pass in null for defaults
		/// @param keys			array of unique record identifiers
		/// @throws AerospikeException	if event loop registration fails
		/*public void Get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)
		{
			if (keys.length == 0)
			{
				listener.onSuccess();
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.GetSequenceCommand(executor, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_GET_ALL, false);
			command.execute();
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

		/**
		 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
		 * <p>
		 * The returned records are in positional order with the original key array order.
		 * If a key is not found, the positional record will be null.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param policy		batch configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @param binNames		array of bins to retrieve
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String...binNames)
		{
			if (keys.length == 0)
			{
				listener.onSuccess(keys, new Record[0]);
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.GetArrayCommand(executor, policy, listener, keys, binNames, null, Command.INFO1_READ, false);
			command.execute();
		}*/

		/**
		 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
		 * <p>
		 * Each record result is returned in separate onRecord() calls.
		 * If a key is not found, the record will be null.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param policy		batch configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @param binNames		array of bins to retrieve
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String...binNames)
		{
			if (keys.length == 0)
			{
				listener.onSuccess();
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.GetSequenceCommand(executor, policy, listener, keys, binNames, null, Command.INFO1_READ, false);
			command.execute();
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

		/**
		 * Asynchronously read multiple records for specified keys using read operations in one batch call.
		 * <p>
		 * The returned records are in positional order with the original key array order.
		 * If a key is not found, the positional record will be null.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param policy		batch configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @param ops			array of read operations on record
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, Operation...ops)
		{
			if (keys.length == 0)
			{
				listener.onSuccess(keys, new Record[0]);
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.GetArrayCommand(executor, policy, listener, keys, null, ops, Command.INFO1_READ, true);
			command.execute();
		}*/

		/**
		 * Asynchronously read multiple records for specified keys using read operations in one batch call.
		 * <p>
		 * Each record result is returned in separate onRecord() calls.
		 * If a key is not found, the record will be null.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param policy		batch configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @param ops			array of read operations on record
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void Get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, Operation...ops)
		{
			if (keys.length == 0)
			{
				listener.onSuccess();
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.GetSequenceCommand(executor, policy, listener, keys, null, ops, Command.INFO1_READ, true);
			command.execute();
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

		/**
		 * Asynchronously read multiple record header data for specified keys in one batch call.
		 * <p>
		 * The returned records are in positional order with the original key array order.
		 * If a key is not found, the positional record will be null.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param policy		batch configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void GetHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)
		{
			if (keys.length == 0)
			{
				listener.onSuccess(keys, new Record[0]);
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.GetArrayCommand(executor, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA, false);
			command.execute();
		}*/

		/**
		 * Asynchronously read multiple record header data for specified keys in one batch call.
		 * <p>
		 * Each record result is returned in separate onRecord() calls.
		 * If a key is not found, the record will be null.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param policy		batch configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void GetHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)
		{
			if (keys.length == 0)
			{
				listener.onSuccess();
				return;
			}

			if (policy == null)
			{
				policy = batchPolicyDefault;
			}

			CommandProxy command = new BatchProxy.GetSequenceCommand(executor, policy, listener, keys, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA, false);
			command.execute();
		}*/

		//-------------------------------------------------------
		// Join methods
		//-------------------------------------------------------
		// TODO: Confirm with Brian that Join is not supported
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
		}*/

		/// <summary>
		/// Read all bins in left record and then join with right records.  Each join bin name
		/// (Join.binNameKeys) must exist in the left record.  The join bin must contain a list of 
		/// keys. Those key are used to retrieve other records using a separate batch get.
		/// </summary>
		/// <param name="policy">generic configuration parameters, pass in null for defaults</param>
		/// <param name="key">unique main record identifier</param>
		/// <param name="joins">array of join definitions</param>
		/// <exception cref="AerospikeException">if main read or join reads fail</exception>
		/*public Record Join(BatchPolicy policy, Key key, params Join[] joins)
		{
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
		/// <exception cref="AerospikeException">if command fails</exception>
		/*public Record Operate(WritePolicy policy, Key key, params Operation[] operations)
		{
			//OperateArgs args = new OperateArgs(cluster, policy, writePolicyDefault, operatePolicyReadDefault, key, operations);
			//OperateCommand command = new OperateCommand(cluster, key, args);
			//command.Execute();
			//return command.Record;
		}*/

		/**
		 * Asynchronously perform multiple read/write operations on a single key in one batch call.
		 * <p>
		 * An example would be to add an integer value to an existing record and then
		 * read the result, all in one database call.
		 * <p>
		 * The server executes operations in the same order as the operations array.
		 * Both scalar bin operations (Operation) and CDT bin operations (ListOperation,
		 * MapOperation) can be performed in same call.
		 *
		 * @param eventLoop				ignored, pass in null
		 * @param listener				where to send results, pass in null for fire and forget
		 * @param policy				write configuration parameters, pass in null for defaults
		 * @param key					unique record identifier
		 * @param operations			database operations to perform
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void Operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation...operations)
		{
			OperateArgs args = new OperateArgs(policy, writePolicyDefault, operatePolicyReadDefault, key, operations);
			OperateCommandProxy command = new OperateCommandProxy(executor, listener, args.writePolicy, key, args);
			command.execute();
		}*/

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
		/*public bool Operate(BatchPolicy policy, List<BatchRecord> records)
		{
			if (records.Count == 0)
			{
				return true;
			}

			if (policy == null)
			{
				policy = batchParentPolicyWriteDefault;
			}

			BatchStatus status = new BatchStatus(true);
			List<BatchNode> batchNodes = BatchNode.GenerateList(cluster, policy, records, status);
			BatchCommand[] commands = new BatchCommand[batchNodes.Count];
			int count = 0;

			foreach (BatchNode batchNode in batchNodes)
			{
				commands[count++] = new BatchOperateListCommand(cluster, batchNode, policy, records, status);
			}
			BatchExecutor.Execute(policy, commands, status);
			return status.GetStatus();
		}*/

		/**
		 * Asynchronously read/write multiple records for specified batch keys in one batch call.
		 * <p>
		 * This method allows different namespaces/bins to be requested for each key in the batch.
		 * The returned records are located in the same list.
		 * <p>
		 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
		 * {@link BatchUDF}.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param policy		batch configuration parameters, pass in null for defaults
		 * @param records		list of unique record identifiers and read/write operations
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void Operate(
			EventLoop eventLoop,
			BatchOperateListListener listener,
			BatchPolicy policy,
			List<BatchRecord> records
		)
		{
			if (records.size() == 0)
			{
				listener.onSuccess(records, true);
				return;
			}

			if (policy == null)
			{
				policy = batchParentPolicyWriteDefault;
			}

			CommandProxy command = new BatchProxy.OperateListCommand(executor, policy, listener, records);
			command.execute();
		}*/

		/**
		 * Asynchronously read/write multiple records for specified batch keys in one batch call.
		 * <p>
		 * This method allows different namespaces/bins to be requested for each key in the batch.
		 * Each record result is returned in separate onRecord() calls.
		 * <p>
		 * {@link BatchRecord} can be {@link BatchRead}, {@link BatchWrite}, {@link BatchDelete} or
		 * {@link BatchUDF}.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param policy		batch configuration parameters, pass in null for defaults
		 * @param records		list of unique record identifiers and read/write operations
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void Operate(
			EventLoop eventLoop,
			BatchRecordSequenceListener listener,
			BatchPolicy policy,
			List<BatchRecord> records
		)
		{
			if (records.size() == 0)
			{
				listener.onSuccess();
				return;
			}

			if (policy == null)
			{
				policy = batchParentPolicyWriteDefault;
			}

			CommandProxy command = new BatchProxy.OperateSequenceCommand(executor, policy, listener, records);
			command.execute();
		}*/

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

		/**
		 * Asynchronously perform read/write operations on multiple keys.
		 * <p>
		 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
		 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param batchPolicy	batch configuration parameters, pass in null for defaults
		 * @param writePolicy	write configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @param ops
		 * read/write operations to perform. {@link Operation#get()} is not allowed because it returns a
		 * variable number of bins and makes it difficult (sometimes impossible) to lineup operations
		 * with results. Instead, use {@link Operation#get(String)} for each bin name.
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void operate(
			EventLoop eventLoop,
			BatchRecordArrayListener listener,
			BatchPolicy batchPolicy,
			BatchWritePolicy writePolicy,
			Key[] keys,
			Operation...ops
		)
		{
			if (keys.length == 0)
			{
				listener.onSuccess(new BatchRecord[0], true);
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

			CommandProxy command = new BatchProxy.OperateRecordArrayCommand(executor,
				batchPolicy, keys, ops, listener, attr);

			command.execute();
		}*/

		/**
		 * Asynchronously perform read/write operations on multiple keys.
		 * <p>
		 * Each record result is returned in separate onRecord() calls.
		 * If a key is not found, the corresponding result {@link BatchRecord#resultCode} will be
		 * {@link ResultCode#KEY_NOT_FOUND_ERROR}.
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param batchPolicy	batch configuration parameters, pass in null for defaults
		 * @param writePolicy	write configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @param ops
		 * read/write operations to perform. {@link Operation#get()} is not allowed because it returns a
		 * variable number of bins and makes it difficult (sometimes impossible) to lineup operations
		 * with results. Instead, use {@link Operation#get(String)} for each bin name.
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void operate(
			EventLoop eventLoop,
			BatchRecordSequenceListener listener,
			BatchPolicy batchPolicy,
			BatchWritePolicy writePolicy,
			Key[] keys,
			Operation...ops
		)
		{
			if (keys.length == 0)
			{
				listener.onSuccess();
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

			CommandProxy command = new BatchProxy.OperateRecordSequenceCommand(executor,
				batchPolicy, keys, ops, listener, attr);

			command.execute();
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

		/**
		 * Asynchronously read all records in specified namespace and set.
		 *
		 * @param eventLoop				ignored, pass in null
		 * @param listener				where to send results
		 * @param policy				scan configuration parameters, pass in null for defaults
		 * @param namespace				namespace - equivalent to database name
		 * @param setName				optional set name - equivalent to database table
		 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void scanAll(
			EventLoop eventLoop,
			RecordSequenceListener listener,
			ScanPolicy policy,
			String namespace,
			String setName,
			String... binNames
		) {
			scanPartitions(eventLoop, listener, policy, null, namespace, setName, binNames);
		}*/

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

		/**
		 * Asynchronously read records in specified namespace, set and partition filter.
		 *
		 * @param eventLoop				ignored, pass in null
		 * @param listener				where to send results
		 * @param policy				scan configuration parameters, pass in null for defaults
		 * @param partitionFilter		filter on a subset of data partitions
		 * @param namespace				namespace - equivalent to database name
		 * @param setName				optional set name - equivalent to database table
		 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void scanPartitions(
			EventLoop eventLoop,
			RecordSequenceListener listener,
			ScanPolicy policy,
			PartitionFilter partitionFilter,
			String namespace,
			String setName,
			String... binNames
		) {
			if (policy == null) {
				policy = scanPolicyDefault;
			}

			PartitionTracker tracker = null;

			if (partitionFilter != null) {
					tracker = new PartitionTracker(policy, 1, partitionFilter);
			}

			ScanCommandProxy command = new ScanCommandProxy(executor, policy, listener, namespace,
				setName, binNames, partitionFilter, tracker);
			command.execute();
		}*/

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
		/*public object Execute(WritePolicy policy, Key key, string packageName, string functionName, params Value[] args)
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
		}*/

		/**
		 * Asynchronously execute user defined function on server.
		 * <p>
		 * The function operates on a single record.
		 * The package name is used to locate the udf file location:
		 * <p>
		 * {@code udf file = <server udf dir>/<package name>.lua}
		 *
		 * @param eventLoop				ignored, pass in null
		 * @param listener				where to send results, pass in null for fire and forget
		 * @param policy				write configuration parameters, pass in null for defaults
		 * @param key					unique record identifier
		 * @param packageName			server package name where user defined function resides
		 * @param functionName			user defined function
		 * @param functionArgs			arguments passed in to user defined function
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void execute(
			EventLoop eventLoop,
			ExecuteListener listener,
			WritePolicy policy,
			Key key,
			String packageName,
			String functionName,
			Value...functionArgs
		)
		{
			if (policy == null)
			{
				policy = writePolicyDefault;
			}
			ExecuteCommandProxy command = new ExecuteCommandProxy(executor, listener, policy, key,
				packageName, functionName, functionArgs);
			command.execute();
		}*/

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

				/**
		 * Asynchronously execute user defined function on server for each key and return results.
		 * <p>
		 * The package name is used to locate the udf file location:
		 * <p>
		 * {@code udf file = <server udf dir>/<package name>.lua}
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param batchPolicy	batch configuration parameters, pass in null for defaults
		 * @param udfPolicy		udf configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @param packageName	server package name where user defined function resides
		 * @param functionName	user defined function
		 * @param functionArgs	arguments passed in to user defined function
		 * @throws AerospikeException	if command fails
		 */
		/*public void execute(
			EventLoop eventLoop,
			BatchRecordArrayListener listener,
			BatchPolicy batchPolicy,
			BatchUDFPolicy udfPolicy,
			Key[] keys,
			String packageName,
			String functionName,
			Value...functionArgs
		)
			{
			if (keys.length == 0)
			{
				listener.onSuccess(new BatchRecord[0], true);
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

			byte[] argBytes = Packer.pack(functionArgs);

			BatchAttr attr = new BatchAttr();
			attr.setUDF(udfPolicy);

			CommandProxy command = new BatchProxy.UDFArrayCommand(executor, batchPolicy,
				listener, keys, packageName, functionName, argBytes, attr);

			command.execute();
		}*/

		/**
		 * Asynchronously execute user defined function on server for each key and return results.
		 * Each record result is returned in separate onRecord() calls.
		 * <p>
		 * The package name is used to locate the udf file location:
		 * <p>
		 * {@code udf file = <server udf dir>/<package name>.lua}
		 *
		 * @param eventLoop		ignored, pass in null
		 * @param listener		where to send results
		 * @param batchPolicy	batch configuration parameters, pass in null for defaults
		 * @param udfPolicy		udf configuration parameters, pass in null for defaults
		 * @param keys			array of unique record identifiers
		 * @param packageName	server package name where user defined function resides
		 * @param functionName	user defined function
		 * @param functionArgs	arguments passed in to user defined function
		 * @throws AerospikeException	if command fails
		 */
		/*public void execute(
			EventLoop eventLoop,
			BatchRecordSequenceListener listener,
			BatchPolicy batchPolicy,
			BatchUDFPolicy udfPolicy,
			Key[] keys,
			String packageName,
			String functionName,
			Value...functionArgs
		)
		{
			if (keys.length == 0)
			{
				listener.onSuccess();
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

			byte[] argBytes = Packer.pack(functionArgs);

			BatchAttr attr = new BatchAttr();
			attr.setUDF(udfPolicy);

			CommandProxy command = new BatchProxy.UDFSequenceCommand(executor, batchPolicy,
				listener, keys, packageName, functionName, argBytes, attr);

			command.execute();
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

				/**
		 * Asynchronously execute query on all server nodes.
		 * <p>
		 * Each record result is returned in separate onRecord() calls.
		 *
		 * @param eventLoop				ignored, pass in null
		 * @param listener				where to send results
		 * @param policy				query configuration parameters, pass in null for defaults
		 * @param statement				query definition
		 * @throws AerospikeException	if event loop registration fails
		 */
		/*public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}

			long taskId = statement.prepareTaskId();
			QueryCommandProxy command = new QueryCommandProxy(executor, listener,
				policy, statement, taskId, null, null);
			command.execute();
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

		/**
		 * Asynchronously execute query for specified partitions.
		 * <p>
		 * Each record result is returned in separate onRecord() calls.
		 *
		 * @param eventLoop				ignored, pass in null
		 * @param listener				where to send results
		 * @param policy				query configuration parameters, pass in null for defaults
		 * @param statement				query definition
		 * @param partitionFilter		filter on a subset of data partitions
		 * @throws AerospikeException	if query fails
		 */
		/*public void queryPartitions(
			EventLoop eventLoop,
			RecordSequenceListener listener,
			QueryPolicy policy,
			Statement statement,
			PartitionFilter partitionFilter
		)
		{
			if (policy == null)
			{
				policy = queryPolicyDefault;
			}

			long taskId = statement.prepareTaskId();
			PartitionTracker tracker = new PartitionTracker(policy, statement, 1, partitionFilter);
			QueryCommandProxy command = new QueryCommandProxy(executor, listener, policy,
				statement, taskId, partitionFilter, tracker);
			command.execute();
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

		//-------------------------------------------------------
		// Internal Methods
		//-------------------------------------------------------

		
	}
}
