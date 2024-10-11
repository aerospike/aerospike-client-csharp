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
using Microsoft.VisualBasic;
using System.Buffers;
using System.Collections;
using static Aerospike.Client.Latency;
using System.Net.Sockets;
using Neo.IronLua;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;
using System.Numerics;
using System.Threading.Channels;
using System;

namespace Aerospike.Client
{
	internal static class CommandHelpers
	{
		public static readonly int INFO1_READ = (1 << 0); // Contains a read operation.
		public static readonly int INFO1_GET_ALL = (1 << 1); // Get all bins.
		public static readonly int INFO1_SHORT_QUERY = (1 << 2); // Short query.
		public static readonly int INFO1_BATCH = (1 << 3); // Batch read or exists.
		public static readonly int INFO1_NOBINDATA = (1 << 5); // Do not read the bins.
		public static readonly int INFO1_READ_MODE_AP_ALL = (1 << 6); // Involve all replicas in read operation.
		public static readonly int INFO1_COMPRESS_RESPONSE = (1 << 7); // Tell server to compress it's response.

		public static readonly int INFO2_WRITE = (1 << 0); // Create or update record
		public static readonly int INFO2_DELETE = (1 << 1); // Fling a record into the belly of Moloch.
		public static readonly int INFO2_GENERATION = (1 << 2); // Update if expected generation == old.
		public static readonly int INFO2_GENERATION_GT = (1 << 3); // Update if new generation >= old, good for restore.
		public static readonly int INFO2_DURABLE_DELETE = (1 << 4); // Transaction resulting in record deletion leaves tombstone (Enterprise only).
		public static readonly int INFO2_CREATE_ONLY = (1 << 5); // Create only. Fail if record already exists.
		public static readonly int INFO2_RELAX_AP_LONG_QUERY = (1 << 6); // Treat as long query, but relac read consistency
		public static readonly int INFO2_RESPOND_ALL_OPS = (1 << 7); // Return a result for every operation.

		public static readonly int INFO3_LAST = (1 << 0); // This is the last of a multi-part message.
		public static readonly int INFO3_COMMIT_MASTER = (1 << 1); // Commit to master only before declaring success.
																   // On send: Do not return partition done in scan/query.
																   // On receive: Specified partition is done in scan/query.
		public static readonly int INFO3_PARTITION_DONE = (1 << 2);
		public static readonly int INFO3_UPDATE_ONLY = (1 << 3); // Update only. Merge bins.
		public static readonly int INFO3_CREATE_OR_REPLACE = (1 << 4); // Create or completely replace record.
		public static readonly int INFO3_REPLACE_ONLY = (1 << 5); // Completely replace existing record only.
		public static readonly int INFO3_SC_READ_TYPE = (1 << 6); // See below.
		public static readonly int INFO3_SC_READ_RELAX = (1 << 7); // See below.

		// Interpret SC_READ bits in info3.
		//
		// RELAX   TYPE
		//	              strict
		//	              ------
		//   0      0     sequential (default)
		//   0      1     linearize
		//
		//	              relaxed
		//	              -------
		//   1      0     allow replica
		//   1      1     allow unavailable

		public const byte BATCH_MSG_READ = 0x0;
		public const byte BATCH_MSG_REPEAT = 0x1;
		public const byte BATCH_MSG_INFO = 0x2;
		public const byte BATCH_MSG_GEN = 0x4;
		public const byte BATCH_MSG_TTL = 0x8;

		public const int MSG_TOTAL_HEADER_SIZE = 30;
		public const int FIELD_HEADER_SIZE = 5;
		public const int OPERATION_HEADER_SIZE = 8;
		public const int MSG_REMAINING_HEADER_SIZE = 22;
		public const int DIGEST_SIZE = 20;
		public const int COMPRESS_THRESHOLD = 128;
		public const ulong CL_MSG_VERSION = 2UL;
		public const ulong AS_MSG_TYPE = 3UL;
		public const ulong MSG_TYPE_COMPRESSED = 4UL;
		public const int MAX_BUFFER_SIZE = 1024 * 1024 * 128;  // 128 MB

		public static void SetCommonProperties(this ICommand command, ArrayPool<byte> bufferPool, Cluster cluster, Policy policy)
		{
			command.Cluster = cluster;
			command.Policy = policy;
			command.MaxRetries = policy.maxRetries;
			command.TotalTimeout = policy.totalTimeout;

			if (command.TotalTimeout > 0)
			{
				command.SocketTimeout = (policy.socketTimeout < command.TotalTimeout && policy.socketTimeout > 0) ? policy.socketTimeout : command.TotalTimeout;
				command.ServerTimeout = command.SocketTimeout;
			}
			else
			{
				command.SocketTimeout = policy.socketTimeout;
				command.ServerTimeout = 0;
			}

			command.BufferPool = bufferPool;
			command.Iteration = 1;
		}

		public static async Task Execute(this ICommand command, CancellationToken token)
		{
			if (command.TotalTimeout > 0)
			{
				command.Deadline = DateTime.UtcNow.AddMilliseconds(command.TotalTimeout);
			}
			await ExecuteCommand(command, token);
		}

		public static async Task Execute(this ICommand command, QueryPartitionCommandNew queryCommand, CancellationToken token)
		{
			try
			{
				await ExecuteCommand(queryCommand, token);
			}
			catch (AerospikeException ae)
			{
				if (!queryCommand.Tracker.ShouldRetry(queryCommand.NodePartitions, ae))
				{
					throw ae;
				}
			}
		}

		public static async Task Execute(this ICommand command, Cluster cluster, BatchPolicy policy, BatchCommandNew[] commands, BatchStatus status, CancellationToken token)
		{
			cluster.AddTran();

			if (policy.maxConcurrentThreads == 1 || commands.Length <= 1)
			{

				//await foreach (BatchCommandNew batchCommand in commands)
				await Parallel.ForEachAsync(commands,
										   token,
					async (batchCommand, token) =>
				{

					//token.ThrowIfCancellationRequested(); handled Parallel.ForEachAsync

					try
					{
						await batchCommand.Execute(token);
					}
					catch (AerospikeException ae)
					{
						// Set error/inDoubt for keys associated this batch command when
						// the command was not retried and split. If a split retry occurred,
						// those new subcommands have already set error/inDoubt on the affected
						// subset of keys.
						if (!batchCommand.splitRetry)
						{
							batchCommand.SetInDoubt(ae.InDoubt);
						}
						status.SetException(ae);

						if (!policy.respondAllKeys)
						{
							throw;
						}
					}
					catch (Exception e)
					{
						if (!batchCommand.splitRetry)
						{
							batchCommand.SetInDoubt(true);
						}
						status.SetException(e);

						if (!policy.respondAllKeys)
						{
							throw;
						}
					}
				});
				status.CheckException();
				return;
			}
		}

		private static async Task ExecuteCommand(ICommand command, CancellationToken token)
		{
			Node node;
			AerospikeException exception = null;
			ValueStopwatch metricsWatch = new();
			LatencyType latencyType = command.Cluster.MetricsEnabled ? command.GetLatencyType() : LatencyType.NONE;
			bool isClientTimeout;

			// Execute command until successful, timed out or maximum iterations have been reached.
			while (true)
			{
				token.ThrowIfCancellationRequested();

				try
				{
					node = command.GetNode();
				}
				catch (AerospikeException ae)
				{
					ae.Policy = command.Policy;
					ae.Iteration = command.Iteration;
					ae.SetInDoubt(command.IsWrite(), command.CommandSentCounter);
					throw;
				}

				try
				{
					node.ValidateErrorCount();
					if (latencyType != LatencyType.NONE)
					{
						metricsWatch = ValueStopwatch.StartNew();
					}
					//Connection conn = await node.GetConnection(SocketTimeout, token);
					Connection conn = node.GetConnection(command.SocketTimeout);

					try
					{
						// Set command buffer.
						command.WriteBuffer();

						// Send command.
						await conn.Write(command.DataBuffer, command.DataOffset, token);
						command.CommandSentCounter++;

						// Parse results.
						await command.ParseResult(conn, token);

						// Put connection back in pool.
						node.PutConnection(conn);

						if (latencyType != LatencyType.NONE)
						{
							node.AddLatency(latencyType, metricsWatch.Elapsed.TotalMilliseconds);
						}

						// Command has completed successfully.  Exit method.
						return;
					}
					catch (AerospikeException ae)
					{
						if (ae.KeepConnection())
						{
							// Put connection back in pool.
							node.PutConnection(conn);
						}
						else
						{
							// Close socket to flush out possible garbage.  Do not put back in pool.
							node.CloseConnectionOnError(conn);
						}

						if (ae.Result == ResultCode.TIMEOUT)
						{
							// Retry on server timeout.
							exception = new AerospikeException.Timeout(command.Policy, false);
							isClientTimeout = false;
							node.IncrErrorRate();
							node.AddTimeout();
						}
						else if (ae.Result == ResultCode.DEVICE_OVERLOAD)
						{
							// Add to circuit breaker error count and retry.
							exception = ae;
							isClientTimeout = false;
							node.IncrErrorRate();
							node.AddError();
						}
						else
						{
							node.AddError();
							throw;
						}
					}
					catch (SocketException se)
					{
						// Socket errors are considered temporary anomalies.
						// Retry after closing connection.
						node.CloseConnectionOnError(conn);

						if (se.SocketErrorCode == SocketError.TimedOut)
						{
							isClientTimeout = true;
							node.AddTimeout();
						}
						else
						{
							exception = new AerospikeException.Connection(se);
							isClientTimeout = false;
							node.AddError();
						}
					}
					catch (IOException ioe)
					{
						// IO errors are considered temporary anomalies.  Retry.
						// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
						node.CloseConnection(conn);
						exception = new AerospikeException.Connection(ioe);
						isClientTimeout = false;
						node.AddError();
					}
					catch (Exception)
					{
						// All other exceptions are considered fatal.  Do not retry.
						// Close socket to flush out possible garbage.  Do not put back in pool.
						node.CloseConnectionOnError(conn);
						node.AddError();
						throw;
					}
				}
				catch (SocketException se)
				{
					// This exception might happen after initial connection succeeded, but
					// user login failed with a socket error.  Retry.
					if (se.SocketErrorCode == SocketError.TimedOut)
					{
						isClientTimeout = true;
						node.AddTimeout();
					}
					else
					{
						exception = new AerospikeException.Connection(se);
						isClientTimeout = false;
						node.AddError();
					}
				}
				catch (IOException ioe)
				{
					// IO errors are considered temporary anomalies.  Retry.
					// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					exception = new AerospikeException.Connection(ioe);
					isClientTimeout = false;
					node.AddError();
				}
				catch (AerospikeException.Connection ce)
				{
					// Socket connection error has occurred. Retry.
					exception = ce;
					isClientTimeout = false;
					node.AddError();
				}
				catch (AerospikeException.Backoff be)
				{
					// Node is in backoff state. Retry, hopefully on another node.
					exception = be;
					isClientTimeout = false;
					node.AddError();
				}
				catch (AerospikeException ae)
				{
					ae.Node = node;
					ae.Policy = command.Policy;
					ae.Iteration = command.Iteration;
					ae.SetInDoubt(command.IsWrite(), command.CommandSentCounter);
					node.AddError();
					throw;
				}
				catch (Exception)
				{
					node.AddError();
					throw;
				}

				// Check maxRetries.
				if (command.Iteration > command.MaxRetries)
				{
					break;
				}

				if (command.TotalTimeout > 0)
				{
					// Check for total timeout.
					long remaining = (long)command.Deadline.Subtract(DateTime.UtcNow).TotalMilliseconds - command.Policy.sleepBetweenRetries;

					if (remaining <= 0)
					{
						break;
					}

					if (remaining < command.TotalTimeout)
					{
						command.TotalTimeout = (int)remaining;

						if (command.SocketTimeout > command.TotalTimeout)
						{
							command.SocketTimeout = command.TotalTimeout;
						}
					}
				}

				if (!isClientTimeout && command.Policy.sleepBetweenRetries > 0)
				{
					// Sleep before trying again.
					Util.Sleep(command.Policy.sleepBetweenRetries);
				}

				command.Iteration++;

				if (!command.PrepareRetry(isClientTimeout || exception.Result != ResultCode.SERVER_NOT_AVAILABLE))
				{
					// Batch may be retried in separate commands.
					if (command.RetryBatch(command.Cluster, command.SocketTimeout, command.TotalTimeout, command.Deadline, command.Iteration, command.CommandSentCounter))
					{
						// Batch was retried in separate commands.  Complete this command.
						return;
					}
				}

				command.Cluster.AddRetry();
			}

			// Retries have been exhausted.  Throw last exception.
			if (isClientTimeout)
			{
				exception = new AerospikeException.Timeout(command.Policy, true);
			}
			exception.Node = node;
			exception.Policy = command.Policy;
			exception.Iteration = command.Iteration;
			exception.SetInDoubt(command.IsWrite(), command.CommandSentCounter);
			throw exception;
		}

		public static async Task<RecordSetNew> ParseResult(this ICommand command, IConnection conn, CancellationToken token)
		{
			// Read blocks of records.  Do not use thread local receive buffer because each
			// block will likely be too big for a cache.  Also, scan callbacks can nest
			// further database commands which would contend with the thread local receive buffer.
			// Instead, use separate heap allocated buffers.
			byte[] protoBuf = new byte[8];
			byte[] buf = null;
			byte[] ubuf = null;
			int receiveSize;

			while (true)
			{
				// Read header
				await conn.ReadFully(protoBuf, 8, token);

				long proto = ByteUtil.BytesToLong(protoBuf, 0);
				int size = (int)(proto & 0xFFFFFFFFFFFFL);

				if (size <= 0)
				{
					continue;
				}

				// Prepare buffer
				if (buf == null || size > buf.Length)
				{
					// Corrupted data streams can result in a huge length.
					// Do a sanity check here.
					if (size > MAX_BUFFER_SIZE)
					{
						throw new AerospikeException("Invalid proto size: " + size);
					}

					int capacity = (size + 16383) & ~16383; // Round up in 16KB increments.
					buf = new byte[capacity];
				}

				// Read remaining message bytes in group.
				await conn.ReadFully(buf, size, token);
				conn.UpdateLastUsed();

				ulong type = (ulong)((proto >> 48) & 0xff);

				if (type == Command.AS_MSG_TYPE)
				{
					command.DataBuffer = buf;
					command.DataOffset = 0;
					receiveSize = size;
				}
				else if (type == Command.MSG_TYPE_COMPRESSED)
				{
					int usize = (int)ByteUtil.BytesToLong(buf, 0);

					if (ubuf == null || usize > ubuf.Length)
					{
						if (usize > MAX_BUFFER_SIZE)
						{
							throw new AerospikeException("Invalid proto size: " + usize);
						}

						int capacity = (usize + 16383) & ~16383; // Round up in 16KB increments.
						ubuf = new byte[capacity];
					}

					ByteUtil.Decompress(buf, 8, size, ubuf, usize);
					command.DataBuffer = ubuf;
					command.DataOffset = 8;
					receiveSize = usize;
				}
				else
				{
					throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
				}

				var keyRecord = command.ParseGroup(receiveSize);
				if (keyRecord == null)
				{
					break;
				}

				yield return keyRecord;
			}
		}

		public static KeyRecord ParseGroup(this ICommand command, int receiveSize)
		{
			KeyRecord keyRecord = null;

			while (command.DataOffset < receiveSize)
			{
				command.DataOffset += 3;
				command.Info3 = command.DataBuffer[command.DataOffset];
				command.DataOffset += 2;
				command.ResultCode = command.DataBuffer[command.DataOffset];

				// If this is the end marker of the response, do not proceed further.
				if ((command.Info3 & Command.INFO3_LAST) != 0)
				{
					if (command.ResultCode != 0)
					{
						// The server returned a fatal error.
						throw new AerospikeException(command.ResultCode);
					}
					return null;
				}

				command.DataOffset++;
				command.Generation = ByteUtil.BytesToInt(command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;
				command.Expiration = ByteUtil.BytesToInt(command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;
				command.BatchIndex = ByteUtil.BytesToInt(command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;
				command.FieldCount = ByteUtil.BytesToShort(command.DataBuffer, command.DataOffset);
				command.DataOffset += 2;
				command.OpCount = ByteUtil.BytesToShort(command.DataBuffer, command.DataOffset);
				command.DataOffset += 2;

				// Note: ParseRow() also handles sync error responses.

				keyRecord = command.ParseRow();
			}
			return keyRecord;
		}

		public static Record ParseRecord(this ICommand command)
		{
			if (command.OpCount <= 0)
			{
				return new Record(null, command.Generation, command.Expiration);
			}

			(Record record, command.DataOffset) = command.Policy.recordParser.ParseRecord(
				command.DataBuffer, command.DataOffset, command.OpCount, command.Generation, 
				command.Expiration, command.IsOperation);
			return record;
		}

		public static int SizeBuffer(this ICommand command)
		{
			if (command.DataBuffer == null || command.DataOffset > command.DataBuffer.Length)
			{
				command.DataBuffer = command.BufferPool.Rent(command.DataOffset);
			}
			command.DataOffset = 0;
			return command.DataBuffer.Length;
		}

		public static void SizeBuffer(this ICommand command, int size)
		{
			if (size > command.DataBuffer.Length)
			{
				command.DataBuffer = command.BufferPool.Rent(size);
			}
		}

		public static void End(this ICommand command)
		{
			// Write total size of message.
			ulong size = ((ulong)command.DataOffset - 8) | (CommandHelpers.CL_MSG_VERSION << 56) | (CommandHelpers.AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, command.DataBuffer, 0);
		}

		public static void SetLength(this ICommand command, int length)
		{
			command.DataOffset = length;
		}

		//--------------------------------------------------
		// Writes
		//--------------------------------------------------

		public static void SetWrite(this ICommand command, WritePolicy policy, Operation.Type operation, Key key, Bin[] bins)
		{
			command.Begin();
			int fieldCount = command.EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			foreach (Bin bin in bins)
			{
				command.EstimateOperationSize(bin);
			}
			
			bool compress = command.SizeBuffer(policy);

			command.WriteHeaderWrite(policy, INFO2_WRITE, fieldCount, bins.Length);
			command.WriteKey(policy, key);

			policy.filterExp?.Write(command);

			foreach (Bin bin in bins)
			{
				command.WriteOperation(bin, operation);
			}
			command.End(compress);
		}

		public static void SetDelete(this ICommand command, WritePolicy policy, Key key)
		{
			command.Begin();
			int fieldCount = command.EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.SizeBuffer();
			command.WriteHeaderWrite(policy, INFO2_WRITE | INFO2_DELETE, fieldCount, 0);
			command.WriteKey(policy, key);

			policy.filterExp?.Write(command);
			command.End();
		}

		public static void SetTouch(this ICommand command, WritePolicy policy, Key key)
		{
			command.Begin();
			int fieldCount = command.EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.EstimateOperationSize();
			command.SizeBuffer();
			command.WriteHeaderWrite(policy, INFO2_WRITE, fieldCount, 1);
			command.WriteKey(policy, key);

			policy.filterExp?.Write(command);
			command.WriteOperation(Operation.Type.TOUCH);
			command.End();
		}

		//--------------------------------------------------
		// Reads
		//--------------------------------------------------

		public static void SetExists(this ICommand command, Policy policy, Key key)
		{
			command.Begin();
			int fieldCount = command.EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.SizeBuffer();
			command.WriteHeaderReadHeader(policy, INFO1_READ | INFO1_NOBINDATA, fieldCount, 0);
			command.WriteKey(policy, key);

			policy.filterExp?.Write(command);
			command.End();
		}

		public static void SetRead(this ICommand command, Policy policy, Key key)
		{
			command.Begin();
			int fieldCount = command.EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.SizeBuffer();
			command.WriteHeaderRead(policy, command.ServerTimeout, INFO1_READ | INFO1_GET_ALL, 0, 0, fieldCount, 0);
			command.WriteKey(policy, key);

			policy.filterExp?.Write(command);
			command.End();
		}

		public static void SetRead(this ICommand command, Policy policy, Key key, string[] binNames)
		{
			if (binNames != null)
			{
				command.Begin();
				int fieldCount = command.EstimateKeySize(policy, key);

				if (policy.filterExp != null)
				{
					command.DataOffset += policy.filterExp.Size();
					fieldCount++;
				}

				foreach (string binName in binNames)
				{
					command.EstimateOperationSize(binName);
				}
				command.SizeBuffer();
				command.WriteHeaderRead(policy, command.ServerTimeout, INFO1_READ, 0, 0, fieldCount, binNames.Length);
				command.WriteKey(policy, key);

				policy.filterExp?.Write(command);

				foreach (string binName in binNames)
				{
					command.WriteOperation(binName, Operation.Type.READ);
				}
				command.End();
			}
			else
			{
				command.SetRead(policy, key);
			}
		}

		public static void SetReadHeader(this ICommand command, Policy policy, Key key)
		{
			command.Begin();
			int fieldCount = command.EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.EstimateOperationSize((string)null);
			command.SizeBuffer();
			command.WriteHeaderReadHeader(policy, INFO1_READ |	INFO1_NOBINDATA, fieldCount, 0);
			command.WriteKey(policy, key);

			policy.filterExp?.Write(command);
			command.End();
		}

		//--------------------------------------------------
		// Operate
		//--------------------------------------------------

		public static void SetOperate(this ICommand command, WritePolicy policy, Key key, OperateArgs args)
		{
			command.Begin();
			int fieldCount = command.EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.DataOffset += args.size;

			bool compress = command.SizeBuffer(policy);

			command.WriteHeaderReadWrite(policy, args, fieldCount);
			command.WriteKey(policy, key);

			policy.filterExp?.Write(command);

			foreach (Operation operation in args.operations)
			{
				command.WriteOperation(operation);
			}
			command.End(compress);
		}

		//--------------------------------------------------
		// UDF
		//--------------------------------------------------

		public static void SetUdf(this ICommand command, WritePolicy policy, Key key, string packageName, string functionName, Value[] args)
		{
			command.Begin();
			int fieldCount = command.EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			byte[] argBytes = Packer.Pack(args);
			fieldCount += command.EstimateUdfSize(packageName, functionName, argBytes);

			bool compress = command.SizeBuffer(policy);

			command.WriteHeaderWrite(policy, INFO2_WRITE, fieldCount, 0);
			command.WriteKey(policy, key);

			policy.filterExp?.Write(command);
			command.WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
			command.WriteField(functionName, FieldType.UDF_FUNCTION);
			command.WriteField(argBytes, FieldType.UDF_ARGLIST);
			command.End(compress);
		}

		//--------------------------------------------------
		// Batch Read Only
		//--------------------------------------------------

		public static void SetBatchRead(this ICommand command, BatchPolicy policy, List<BatchRead> records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			BatchRead prev = null;

			command.Begin();
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			command.DataOffset += FIELD_HEADER_SIZE + 5;

			for (int i = 0; i < max; i++)
			{
				BatchRead record = records[offsets[i]];
				Key key = record.key;
				string[] binNames = record.binNames;
				Operation[] ops = record.ops;

				command.DataOffset += key.digest.Length + 4;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from 
				// fixed variables.  It's fine if equality not determined correctly because it just 
				// results in more space used. The batch will still be correct.
				if (prev != null && prev.key.ns == key.ns && prev.key.setName == key.setName &&
					prev.binNames == binNames && prev.readAllBins == record.readAllBins &&
					prev.ops == ops)
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataOffset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE + 6;
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							command.EstimateOperationSize(binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							command.EstimateReadOperationSize(op);
						}
					}
					prev = record;
				}
			}

			bool compress = command.SizeBuffer(policy);

			int readAttr = INFO1_READ;

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			command.WriteHeaderRead(policy, command.TotalTimeout, readAttr | INFO1_BATCH, 0, 0, fieldCount, 0);

			policy.filterExp?.Write(command);

			int fieldSizeOffset = command.DataOffset;
			command.WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = (policy.allowInline) ? (byte)1 : (byte)0;
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;

				BatchRead record = records[index];
				Key key = record.key;
				string[] binNames = record.binNames;
				Operation[] ops = record.ops;
				byte[] digest = key.digest;
				Array.Copy(digest, 0, command.DataBuffer, command.DataOffset, digest.Length);
				command.DataOffset += digest.Length;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from 
				// fixed variables.  It's fine if equality not determined correctly because it just 
				// results in more space used. The batch will still be correct.		
				if (prev != null && prev.key.ns == key.ns && prev.key.setName == key.setName &&
					prev.binNames == binNames && prev.readAllBins == record.readAllBins &&
					prev.ops == ops)
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataBuffer[command.DataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full header, namespace and bin names.
					command.DataBuffer[command.DataOffset++] = BATCH_MSG_READ;

					if (binNames != null && binNames.Length != 0)
					{
						command.DataBuffer[command.DataOffset++] = (byte)readAttr;
						command.WriteBatchFields(key, 0, binNames.Length);

						foreach (string binName in binNames)
						{
							command.WriteOperation(binName, Operation.Type.READ);
						}
					}
					else if (ops != null)
					{
						int offset = command.DataOffset++;
						command.WriteBatchFields(key, 0, ops.Length);
						command.DataBuffer[offset] = (byte)command.WriteReadOnlyOperations(ops, readAttr);
					}
					else
					{
						command.DataBuffer[command.DataOffset++] = (byte)(readAttr | (record.readAllBins ? INFO1_GET_ALL : INFO1_NOBINDATA));
						command.WriteBatchFields(key, 0, 0);
					}
					prev = record;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(command.DataOffset - MSG_TOTAL_HEADER_SIZE - 4), command.DataBuffer, fieldSizeOffset);
			command.End(compress);
		}

		public static void SetBatchRead
		(
			this ICommand command,
			BatchPolicy policy,
			Key[] keys,
			BatchNode batch,
			string[] binNames,
			Operation[] ops,
			int readAttr
		)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			// Estimate DataBuffer size.
			command.Begin();
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}
			command.DataOffset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				command.DataOffset += key.digest.Length + 4;

				// Try reference equality in hope that namespace for all keys is set from a fixed variable.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName) 
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataOffset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE + 6;
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (binNames != null)
					{
						foreach (String binName in binNames)
						{
							command.EstimateOperationSize(binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							command.EstimateReadOperationSize(op);
						}
					}
					prev = key;
				}
			}

			bool compress = command.SizeBuffer(policy);

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			command.WriteHeaderRead(policy, command.TotalTimeout, readAttr | INFO1_BATCH, 0, 0, fieldCount, 0);

			policy.filterExp?.Write(command);

			int fieldSizeOffset = command.DataOffset;
			command.WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = (policy.allowInline) ? (byte)1 : (byte)0;
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;

				Key key = keys[index];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, command.DataBuffer, command.DataOffset, digest.Length);
				command.DataOffset += digest.Length;

				// Try reference equality in hope that namespace for all keys is set from a fixed variable.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataBuffer[command.DataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full header, namespace and bin names.
					command.DataBuffer[command.DataOffset++] = BATCH_MSG_READ;

					if (binNames != null && binNames.Length != 0)
					{
						command.DataBuffer[command.DataOffset++] = (byte)readAttr;
						command.WriteBatchFields(key, 0, binNames.Length);

						foreach (String binName in binNames)
						{
							command.WriteOperation(binName, Operation.Type.READ);
						}
					}
					else if (ops != null)
					{
						int offset = command.DataOffset++;
						command.WriteBatchFields(key, 0, ops.Length);
						command.DataBuffer[offset] = (byte)command.WriteReadOnlyOperations(ops, readAttr);
					}
					else
					{
						command.DataBuffer[command.DataOffset++] = (byte)readAttr;
						command.WriteBatchFields(key, 0, 0);
					}
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(command.DataOffset - MSG_TOTAL_HEADER_SIZE - 4), command.DataBuffer, fieldSizeOffset);
			command.End(compress);
		}

		//--------------------------------------------------
		// Batch Read/Write Operations
		//--------------------------------------------------

		public static void SetBatchOperate(this ICommand command, BatchPolicy policy, IList records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			BatchRecord prev = null;

			command.Begin();
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			command.DataOffset += FIELD_HEADER_SIZE + 5;

			for (int i = 0; i < max; i++)
			{
				BatchRecord record = (BatchRecord)records[offsets[i]];
				Key key = record.key;

				command.DataOffset += key.digest.Length + 4;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from
				// fixed variables.  It's fine if equality not determined correctly because it just
				// results in more space used. The batch will still be correct.
				if (!policy.sendKey && prev != null && prev.key.ns == key.ns && 
					prev.key.setName == key.setName && record.Equals(prev))
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataOffset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					command.DataOffset += 12;
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
					command.DataOffset += record.Size(policy);
					prev = record;
				}
			}

			bool compress = command.SizeBuffer(policy);

			command.WriteBatchHeader(policy, command.TotalTimeout, fieldCount);

			policy.filterExp?.Write(command);

			int fieldSizeOffset = command.DataOffset;
			command.WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = GetBatchFlags(policy);

			BatchAttr attr = new();
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;

				BatchRecord record = (BatchRecord)records[index];
				Key key = record.key;
				byte[] digest = key.digest;
				Array.Copy(digest, 0, command.DataBuffer, command.DataOffset, digest.Length);
				command.DataOffset += digest.Length;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from
				// fixed variables.  It's fine if equality not determined correctly because it just
				// results in more space used. The batch will still be correct.
				if (!policy.sendKey && prev != null && prev.key.ns == key.ns &&
					prev.key.setName == key.setName && record.Equals(prev))
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataBuffer[command.DataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					switch (record.GetBatchType())
					{
						case BatchRecord.Type.BATCH_READ:
						{
							BatchRead br = (BatchRead)record;

							if (br.policy != null)
							{
								attr.SetRead(br.policy);
							}
							else
							{
								attr.SetRead(policy);
							}

							if (br.binNames != null)
							{
								command.WriteBatchBinNames(key, br.binNames, attr, attr.filterExp);
							}
							else if (br.ops != null)
							{
								attr.AdjustRead(br.ops);
									command.WriteBatchOperations(key, br.ops, attr, attr.filterExp);
							}
							else
							{
								attr.AdjustRead(br.readAllBins);
									command.WriteBatchRead(key, attr, attr.filterExp, 0);
							}
							break;
						}

						case BatchRecord.Type.BATCH_WRITE:
						{
							BatchWrite bw = (BatchWrite)record;

							if (bw.policy != null)
							{
								attr.SetWrite(bw.policy);
							}
							else
							{
								attr.SetWrite(policy);
							}
							attr.AdjustWrite(bw.ops);
							command.WriteBatchOperations(key, bw.ops, attr, attr.filterExp);
							break;
						}

						case BatchRecord.Type.BATCH_UDF:
						{
							BatchUDF bu = (BatchUDF)record;

							if (bu.policy != null)
							{
								attr.SetUDF(bu.policy);
							}
							else
							{
								attr.SetUDF(policy);
							}
							command.WriteBatchWrite(key, attr, attr.filterExp, 3, 0);
							command.WriteField(bu.packageName, FieldType.UDF_PACKAGE_NAME);
							command.WriteField(bu.functionName, FieldType.UDF_FUNCTION);
							command.WriteField(bu.argBytes, FieldType.UDF_ARGLIST);
							break;
						}

						case BatchRecord.Type.BATCH_DELETE:
						{
							BatchDelete bd = (BatchDelete)record;

							if (bd.policy != null)
							{
								attr.SetDelete(bd.policy);
							}
							else
							{
								attr.SetDelete(policy);
							}
							command.WriteBatchWrite(key, attr, attr.filterExp, 0, 0);
							break;
						}
					}
					prev = record;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(command.DataOffset - MSG_TOTAL_HEADER_SIZE - 4), command.DataBuffer, fieldSizeOffset);
			command.End(compress);
		}

		public static void SetBatchOperate
		(
			this ICommand command,
			BatchPolicy policy,
			Key[] keys,
			BatchNode batch,
			string[] binNames,
			Operation[] ops,
			BatchAttr attr
		)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			// Estimate DataBuffer size.
			command.Begin();
			int fieldCount = 1;
			Expression exp = GetBatchExpression(policy, attr);

			if (exp != null)
			{
				command.DataOffset += exp.Size();
				fieldCount++;
			}

			command.DataOffset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				command.DataOffset += key.digest.Length + 4;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (!attr.sendKey && prev != null && prev.ns == key.ns && 
					prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataOffset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					command.DataOffset += 12; // header(4) + ttl(4) + fielCount(2) + opCount(2) = 12
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (attr.sendKey)
					{
						command.DataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
					}

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							command.EstimateOperationSize(binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							if (Operation.IsWrite(op.type))
							{
								if (!attr.hasWrite)
								{
									throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
								}
								command.DataOffset += 2; // Extra write specific fields.
							}
							command.EstimateOperationSize(op);
						}
					}
					else if ((attr.writeAttr & INFO2_DELETE) != 0)
					{
						command.DataOffset += 2; // Extra write specific fields.
					}
					prev = key;
				}
			}

			bool compress = command.SizeBuffer(policy);

			command.WriteBatchHeader(policy, command.TotalTimeout, fieldCount);

			exp?.Write(command);

			int fieldSizeOffset = command.DataOffset;
			command.WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = GetBatchFlags(policy);
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;

				Key key = keys[index];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, command.DataBuffer, command.DataOffset, digest.Length);
				command.DataOffset += digest.Length;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (!attr.sendKey && prev != null && prev.ns == key.ns && 
					prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataBuffer[command.DataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					if (binNames != null)
					{
						command.WriteBatchBinNames(key, binNames, attr, null);
					}
					else if (ops != null)
					{
						command.WriteBatchOperations(key, ops, attr, null);
					}
					else if ((attr.writeAttr & INFO2_DELETE) != 0)
					{
						command.WriteBatchWrite(key, attr, null, 0, 0);
					}
					else
					{
						command.WriteBatchRead(key, attr, null, 0);
					}
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(command.DataOffset - MSG_TOTAL_HEADER_SIZE - 4), command.DataBuffer, fieldSizeOffset);
			command.End(compress);
		}

		public static void SetBatchUDF
		(
			this ICommand command,
			BatchPolicy policy,
			Key[] keys,
			BatchNode batch,
			string packageName,
			string functionName,
			byte[] argBytes,
			BatchAttr attr
		)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			// Estimate DataBuffer size.
			command.Begin();
			int fieldCount = 1;
			Expression exp = GetBatchExpression(policy, attr);

			if (exp != null)
			{
				command.DataOffset += exp.Size();
				fieldCount++;
			}

			command.DataOffset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				command.DataOffset += key.digest.Length + 4;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (!attr.sendKey && prev != null && prev.ns == key.ns && 
					prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataOffset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					command.DataOffset += 12; // header(4) + ttl(4) + fielCount(2) + opCount(2) = 12
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					command.DataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (attr.sendKey)
					{
						command.DataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
					}
					command.DataOffset += 2; // gen(2) = 6
					command.EstimateUdfSize(packageName, functionName, argBytes);
					prev = key;
				}
			}

			bool compress = command.SizeBuffer(policy);

			command.WriteBatchHeader(policy, command.TotalTimeout, fieldCount);

			exp?.Write(command);

			int fieldSizeOffset = command.DataOffset;
			command.WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = GetBatchFlags(policy);
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;

				Key key = keys[index];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, command.DataBuffer, command.DataOffset, digest.Length);
				command.DataOffset += digest.Length;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (!attr.sendKey && prev != null && prev.ns == key.ns && 
					prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					command.DataBuffer[command.DataOffset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					command.WriteBatchWrite(key, attr, null, 3, 0);
					command.WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
					command.WriteField(functionName, FieldType.UDF_FUNCTION);
					command.WriteField(argBytes, FieldType.UDF_ARGLIST);
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(command.DataOffset - MSG_TOTAL_HEADER_SIZE - 4), command.DataBuffer, fieldSizeOffset);
			command.End(compress);
		}

		private static Expression GetBatchExpression(Policy policy, BatchAttr attr)
		{
			return attr.filterExp ?? policy.filterExp;
		}

		private static byte GetBatchFlags(BatchPolicy policy)
		{
			byte flags = 0x8;

			if (policy.allowInline)
			{
				flags |= 0x1;
			}

			if (policy.allowInlineSSD)
			{
				flags |= 0x2;
			}

			if (policy.respondAllKeys)
			{
				flags |= 0x4;
			}
			return flags;
		}

		private static void WriteBatchHeader(this ICommand command, Policy policy, int timeout, int fieldCount)
		{
			int readAttr = INFO1_BATCH;

			if (policy.compress)
			{
				readAttr |= INFO1_COMPRESS_RESPONSE;
			}

			// Write all header data except total size which must be written last.
			command.DataOffset += 8;
			command.DataBuffer[command.DataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			command.DataBuffer[command.DataOffset++] = (byte)readAttr;

			Array.Clear(command.DataBuffer, command.DataOffset, 12);
			command.DataOffset += 12;

			command.DataOffset += ByteUtil.IntToBytes((uint)timeout, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes(0, command.DataBuffer, command.DataOffset);
		}

		private static void WriteBatchBinNames(this ICommand command, Key key, string[] binNames, BatchAttr attr, Expression filter)
		{
			command.WriteBatchRead(key, attr, filter, binNames.Length);

			foreach (string binName in binNames)
			{
				command.WriteOperation(binName, Operation.Type.READ);
			}
		}

		private static void WriteBatchOperations(this ICommand command, Key key, Operation[] ops, BatchAttr attr, Expression filter)
		{
			if (attr.hasWrite)
			{
				command.WriteBatchWrite(key, attr, filter, 0, ops.Length);
			}
			else
			{
				command.WriteBatchRead(key, attr, filter, ops.Length);
			}

			foreach (Operation op in ops)
			{
				command.WriteOperation(op);
			}
		}

		private static void WriteBatchRead(this ICommand command, Key key, BatchAttr attr, Expression filter, int opCount)
		{
			command.DataBuffer[command.DataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_TTL);
			command.DataBuffer[command.DataOffset++] = (byte)attr.readAttr;
			command.DataBuffer[command.DataOffset++] = (byte)attr.writeAttr;
			command.DataBuffer[command.DataOffset++] = (byte)attr.infoAttr;
			command.DataOffset += ByteUtil.IntToBytes((uint)attr.expiration, command.DataBuffer, command.DataOffset);
			command.WriteBatchFields(key, filter, 0, opCount);
		}

		private static void WriteBatchWrite(this ICommand command, Key key, BatchAttr attr, Expression filter, int fieldCount, int opCount)
		{
			command.DataBuffer[command.DataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_GEN | BATCH_MSG_TTL);
			command.DataBuffer[command.DataOffset++] = (byte)attr.readAttr;
			command.DataBuffer[command.DataOffset++] = (byte)attr.writeAttr;
			command.DataBuffer[command.DataOffset++] = (byte)attr.infoAttr;
			command.DataOffset += ByteUtil.ShortToBytes((ushort)attr.generation, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.IntToBytes((uint)attr.expiration, command.DataBuffer, command.DataOffset);

			if (attr.sendKey)
			{
				fieldCount++;
				command.WriteBatchFields(key, filter, fieldCount, opCount);
				command.WriteField(key.userKey, FieldType.KEY);
			}
			else
			{
				command.WriteBatchFields(key, filter, fieldCount, opCount);
			}
		}

		private static void WriteBatchFields(this ICommand command, Key key, Expression filter, int fieldCount, int opCount)
		{
			if (filter != null)
			{
				fieldCount++;
				command.WriteBatchFields(key, fieldCount, opCount);
				filter.Write(command);
			}
			else
			{
				command.WriteBatchFields(key, fieldCount, opCount);
			}
		}

		private static void WriteBatchFields(this ICommand command, Key key, int fieldCount, int opCount)
		{
			fieldCount += 2;
			command.DataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)opCount, command.DataBuffer, command.DataOffset);
			command.WriteField(key.ns, FieldType.NAMESPACE);
			command.WriteField(key.setName, FieldType.TABLE);
		}

		//--------------------------------------------------
		// Scan
		//--------------------------------------------------

		public static void SetScan
		(
			this ICommand command,
			Cluster cluster,
			ScanPolicy policy,
			string ns,
			string setName,
			string[] binNames,
			ulong taskId,
			NodePartitions nodePartitions
		)
		{
			command.Begin();
			int fieldCount = 0;
			int partsFullSize = nodePartitions.partsFull.Count * 2;
			int partsPartialSize = nodePartitions.partsPartial.Count * 20;
			long maxRecords = nodePartitions.recordMax;

			if (ns != null)
			{
				command.DataOffset += ByteUtil.EstimateSizeUtf8(ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (setName != null)
			{
				command.DataOffset += ByteUtil.EstimateSizeUtf8(setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsFullSize > 0)
			{
				command.DataOffset += partsFullSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsPartialSize > 0)
			{
				command.DataOffset += partsPartialSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (maxRecords > 0)
			{
				command.DataOffset += 8 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (policy.recordsPerSecond > 0)
			{
				command.DataOffset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			// Estimate scan timeout size.
			command.DataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate taskId size.
			command.DataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					command.EstimateOperationSize(binName);
				}
			}

			command.SizeBuffer();
			int readAttr = INFO1_READ;

			if (!policy.includeBinData)
			{
				readAttr |=	INFO1_NOBINDATA;
			}

			// Clusters that support partition queries also support not sending partition done messages.
			int infoAttr = cluster.hasPartitionQuery ? INFO3_PARTITION_DONE : 0;
			int operationCount = (binNames == null) ? 0 : binNames.Length;
			command.WriteHeaderRead(policy, command.TotalTimeout, readAttr, 0, infoAttr, fieldCount, operationCount);

			if (ns != null)
			{
				command.WriteField(ns, FieldType.NAMESPACE);
			}

			if (setName != null)
			{
				command.WriteField(setName, FieldType.TABLE);
			}

			if (partsFullSize > 0)
			{
				command.WriteFieldHeader(partsFullSize, FieldType.PID_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsFull)
				{
					ByteUtil.ShortToLittleBytes((ushort)part.id, command.DataBuffer, command.DataOffset);
					command.DataOffset += 2;
				}
			}

			if (partsPartialSize > 0)
			{
				command.WriteFieldHeader(partsPartialSize, FieldType.DIGEST_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial) {
					Array.Copy(part.digest, 0, command.DataBuffer, command.DataOffset, 20);
					command.DataOffset += 20;
				}
			}

			if (maxRecords > 0)
			{
				command.WriteField((ulong)maxRecords, FieldType.MAX_RECORDS);
			}

			if (policy.recordsPerSecond > 0)
			{
				command.WriteField(policy.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			policy.filterExp?.Write(command);

			// Write scan timeout
			command.WriteField(policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

			// Write taskId field
			command.WriteField(taskId, FieldType.TRAN_ID);

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					command.WriteOperation(binName, Operation.Type.READ);
				}
			}
			command.End();
		}

		//--------------------------------------------------
		// Query
		//--------------------------------------------------

		public static void SetQuery
		(
			this ICommand command,
			Cluster cluster,
			Policy policy,
			Statement statement,
			ulong taskId,
			bool background,
			NodePartitions nodePartitions
		)
		{
			byte[] functionArgBuffer = null;
			int fieldCount = 0;
			int filterSize = 0;
			int binNameSize = 0;
			bool isNew = cluster.hasPartitionQuery;

			command.Begin();

			if (statement.ns != null)
			{
				command.DataOffset += ByteUtil.EstimateSizeUtf8(statement.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.setName != null)
			{
				command.DataOffset += ByteUtil.EstimateSizeUtf8(statement.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate recordsPerSecond field size. This field is used in new servers and not used
			// (but harmless to add) in old servers.
			if (statement.recordsPerSecond > 0)
			{
				command.DataOffset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate socket timeout field size. This field is used in new servers and not used
			// (but harmless to add) in old servers.
			command.DataOffset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate taskId field.
			command.DataOffset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			byte[] packedCtx = null;

			if (statement.filter != null)
			{
				IndexCollectionType type = statement.filter.CollectionType;

				// Estimate INDEX_TYPE field.
				if (type != IndexCollectionType.DEFAULT)
				{
					command.DataOffset += FIELD_HEADER_SIZE + 1;
					fieldCount++;
				}

				// Estimate INDEX_RANGE field.
				command.DataOffset += FIELD_HEADER_SIZE;
				filterSize++; // num filters
				filterSize += statement.filter.EstimateSize();
				command.DataOffset += filterSize;
				fieldCount++;

				if (!isNew)
				{
					// Query bin names are specified as a field (Scan bin names are specified later as operations)
					// in old servers. Estimate size for selected bin names.
					if (statement.binNames != null && statement.binNames.Length > 0)
					{
						command.DataOffset += FIELD_HEADER_SIZE;
						binNameSize++; // num bin names

						foreach (string binName in statement.binNames)
						{
							binNameSize += ByteUtil.EstimateSizeUtf8(binName) + 1;
						}
						command.DataOffset += binNameSize;
						fieldCount++;
					}
				}

				packedCtx = statement.filter.PackedCtx;

				if (packedCtx != null)
				{
					command.DataOffset += FIELD_HEADER_SIZE + packedCtx.Length;
					fieldCount++;
				}
			}

			// Estimate aggregation/background function size.
			if (statement.functionName != null)
			{
				command.DataOffset += FIELD_HEADER_SIZE + 1; // udf type
				command.DataOffset += ByteUtil.EstimateSizeUtf8(statement.packageName) + FIELD_HEADER_SIZE;
				command.DataOffset += ByteUtil.EstimateSizeUtf8(statement.functionName) + FIELD_HEADER_SIZE;

				if (statement.functionArgs.Length > 0)
				{
					functionArgBuffer = Packer.Pack(statement.functionArgs);
				}
				else
				{
					functionArgBuffer = Array.Empty<byte>();
				}
				command.DataOffset += FIELD_HEADER_SIZE + functionArgBuffer.Length;
				fieldCount += 4;
			}

			if (policy.filterExp != null)
			{
				command.DataOffset += policy.filterExp.Size();
				fieldCount++;
			}

			long maxRecords = 0;
			int partsFullSize = 0;
			int partsPartialDigestSize = 0;
			int partsPartialBValSize = 0;

			if (nodePartitions != null)
			{
				partsFullSize = nodePartitions.partsFull.Count * 2;
				partsPartialDigestSize = nodePartitions.partsPartial.Count * 20;

				if (statement.filter != null)
				{
					partsPartialBValSize = nodePartitions.partsPartial.Count * 8;
				}
				maxRecords = nodePartitions.recordMax;
			}

			if (partsFullSize > 0)
			{
				command.DataOffset += partsFullSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsPartialDigestSize > 0)
			{
				command.DataOffset += partsPartialDigestSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (partsPartialBValSize > 0)
			{
				command.DataOffset += partsPartialBValSize + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate max records field size. This field is used in new servers and not used
			// (but harmless to add) in old servers.
			if (maxRecords > 0)
			{
				command.DataOffset += 8 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Operations (used in query execute) and bin names (used in scan/query) are mutually exclusive.
			int operationCount = 0;

			if (statement.operations != null)
			{
				// Estimate size for background operations.
				if (!background)
				{
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Operations not allowed in foreground query");
				}

				foreach (Operation operation in statement.operations)
				{
					if (!Operation.IsWrite(operation.type))
					{
						throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Read operations not allowed in background query");
					}
					command.EstimateOperationSize(operation);
				}
				operationCount = statement.operations.Length;
			}
			else if (statement.binNames != null && (isNew || statement.filter == null))
			{
				// Estimate size for selected bin names (query bin names already handled for old servers).
				foreach (string binName in statement.binNames)
				{
					command.EstimateOperationSize(binName);
				}
				operationCount = statement.binNames.Length;
			}

			command.SizeBuffer();

			if (background)
			{
				command.WriteHeaderWrite((WritePolicy)policy, INFO2_WRITE, fieldCount, operationCount);
			}
			else
			{
				QueryPolicy qp = (QueryPolicy)policy;
				int readAttr = INFO1_READ;
				int writeAttr = 0;

				if (!qp.includeBinData)
				{
					readAttr |= INFO1_NOBINDATA;
				}

				if (qp.shortQuery || qp.expectedDuration == QueryDuration.SHORT)
				{
					readAttr |= INFO1_SHORT_QUERY;
				}
				else if (qp.expectedDuration == QueryDuration.LONG_RELAX_AP)
				{
					writeAttr |= INFO2_RELAX_AP_LONG_QUERY;
				}

				int infoAttr = isNew ? INFO3_PARTITION_DONE : 0;

				command.WriteHeaderRead(policy, command.TotalTimeout, readAttr, writeAttr, infoAttr, fieldCount, operationCount);
			}

			if (statement.ns != null)
			{
				command.WriteField(statement.ns, FieldType.NAMESPACE);
			}

			if (statement.setName != null)
			{
				command.WriteField(statement.setName, FieldType.TABLE);
			}

			// Write records per second.
			if (statement.recordsPerSecond > 0)
			{
				command.WriteField(statement.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			// Write socket idle timeout.
			command.WriteField(policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

			// Write taskId field
			command.WriteField(taskId, FieldType.TRAN_ID);

			if (statement.filter != null)
			{
				IndexCollectionType type = statement.filter.CollectionType;

				if (type != IndexCollectionType.DEFAULT)
				{
					command.WriteFieldHeader(1, FieldType.INDEX_TYPE);
					command.DataBuffer[command.DataOffset++] = (byte)type;
				}

				command.WriteFieldHeader(filterSize, FieldType.INDEX_RANGE);
				command.DataBuffer[command.DataOffset++] = (byte)1;
				command.DataOffset = statement.filter.Write(command.DataBuffer, command.DataOffset);

				if (!isNew)
				{
					// Query bin names are specified as a field (Scan bin names are specified later as operations)
					// in old servers.
					if (statement.binNames != null && statement.binNames.Length > 0)
					{
						command.WriteFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
						command.DataBuffer[command.DataOffset++] = (byte)statement.binNames.Length;

						foreach (string binName in statement.binNames)
						{
							int len = ByteUtil.StringToUtf8(binName, command.DataBuffer, command.DataOffset + 1);
							command.DataBuffer[command.DataOffset] = (byte)len;
							command.DataOffset += len + 1;
						}
					}
				}

				if (packedCtx != null)
				{
					command.WriteFieldHeader(packedCtx.Length, FieldType.INDEX_CONTEXT);
					Array.Copy(packedCtx, 0, command.DataBuffer, command.DataOffset, packedCtx.Length);
					command.DataOffset += packedCtx.Length;
				}
			}

			if (statement.functionName != null)
			{
				command.WriteFieldHeader(1, FieldType.UDF_OP);
				command.DataBuffer[command.DataOffset++] = background ? (byte)2 : (byte)1;
				command.WriteField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
				command.WriteField(statement.functionName, FieldType.UDF_FUNCTION);
				command.WriteField(functionArgBuffer, FieldType.UDF_ARGLIST);
			}

			policy.filterExp?.Write(command);

			if (partsFullSize > 0)
			{
				command.WriteFieldHeader(partsFullSize, FieldType.PID_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsFull)
				{
					ByteUtil.ShortToLittleBytes((ushort)part.id, command.DataBuffer, command.DataOffset);
					command.DataOffset += 2;
				}
			}

			if (partsPartialDigestSize > 0)
			{
				command.WriteFieldHeader(partsPartialDigestSize, FieldType.DIGEST_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial)
				{
					Array.Copy(part.digest, 0, command.DataBuffer, command.DataOffset, 20);
					command.DataOffset += 20;
				}
			}

			if (partsPartialBValSize > 0)
			{
				command.WriteFieldHeader(partsPartialBValSize, FieldType.BVAL_ARRAY);

				foreach (PartitionStatus part in nodePartitions.partsPartial)
				{
					ByteUtil.LongToLittleBytes(part.bval, command.DataBuffer, command.DataOffset);
					command.DataOffset += 8;
				}
			}

			if (maxRecords > 0)
			{
				command.WriteField((ulong)maxRecords, FieldType.MAX_RECORDS);
			}

			if (statement.operations != null)
			{
				foreach (Operation operation in statement.operations)
				{
					command.WriteOperation(operation);
				}
			}
			else if (statement.binNames != null && (isNew || statement.filter == null))
			{
				foreach (string binName in statement.binNames)
				{
					command.WriteOperation(binName, Operation.Type.READ);
				}
			}
			command.End();
		}

		//--------------------------------------------------
		// ICommand Sizing
		//--------------------------------------------------

		private static int EstimateKeySize(this ICommand command, Policy policy, Key key)
		{
			int fieldCount = 0;

			if (key.ns != null)
			{
				command.DataOffset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (key.setName != null)
			{
				command.DataOffset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			command.DataOffset += key.digest.Length + FIELD_HEADER_SIZE;
			fieldCount++;

			if (policy.sendKey)
			{
				command.DataOffset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}
			return fieldCount;
		}

		private static int EstimateUdfSize(this ICommand command, string packageName, string functionName, byte[] bytes)
		{
			command.DataOffset += ByteUtil.EstimateSizeUtf8(packageName) + FIELD_HEADER_SIZE;
			command.DataOffset += ByteUtil.EstimateSizeUtf8(functionName) + FIELD_HEADER_SIZE;
			command.DataOffset += bytes.Length + FIELD_HEADER_SIZE;
			return 3;
		}

		private static void EstimateOperationSize(this ICommand command, Bin bin)
		{
			command.DataOffset += ByteUtil.EstimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
			command.DataOffset += bin.value.EstimateSize();
		}

		private static void EstimateOperationSize(this ICommand command, Operation operation)
		{
			command.DataOffset += ByteUtil.EstimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
			command.DataOffset += operation.value.EstimateSize();
		}

		private static void EstimateReadOperationSize(this ICommand command, Operation operation)
		{
			if (Operation.IsWrite(operation.type))
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
			}
			command.DataOffset += ByteUtil.EstimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
			command.DataOffset += operation.value.EstimateSize();
		}

		private static void EstimateOperationSize(this ICommand command, string binName)
		{
			command.DataOffset += ByteUtil.EstimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
		}

		private static void EstimateOperationSize(this ICommand command)
		{
			command.DataOffset += OPERATION_HEADER_SIZE;
		}

		//--------------------------------------------------
		// ICommand Writes
		//--------------------------------------------------

		/// <summary>
		/// Header write for write commands.
		/// </summary>
		private static void WriteHeaderWrite(this ICommand command, WritePolicy policy, int writeAttr, int fieldCount, int operationCount)
		{
			// Set flags.
			int generation = 0;
			int infoAttr = 0;

			switch (policy.recordExistsAction)
			{
			case RecordExistsAction.UPDATE:
				break;
			case RecordExistsAction.UPDATE_ONLY:
				infoAttr |= INFO3_UPDATE_ONLY;
				break;
			case RecordExistsAction.REPLACE:
				infoAttr |= INFO3_CREATE_OR_REPLACE;
				break;
			case RecordExistsAction.REPLACE_ONLY:
				infoAttr |= INFO3_REPLACE_ONLY;
				break;
			case RecordExistsAction.CREATE_ONLY:
				writeAttr |= INFO2_CREATE_ONLY;
				break;
			}

			switch (policy.generationPolicy)
			{
			case GenerationPolicy.NONE:
				break;
			case GenerationPolicy.EXPECT_GEN_EQUAL:
				generation = policy.generation;
				writeAttr |= INFO2_GENERATION;
				break;
			case GenerationPolicy.EXPECT_GEN_GT:
				generation = policy.generation;
				writeAttr |= INFO2_GENERATION_GT;
				break;
			}

			if (policy.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= INFO3_COMMIT_MASTER;
			}

			if (policy.durableDelete)
			{
				writeAttr |= INFO2_DURABLE_DELETE;
			}

			command.DataOffset += 8;

			// Write all header data except total size which must be written last. 
			command.DataBuffer[command.DataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			command.DataBuffer[command.DataOffset++] = (byte)0;
			command.DataBuffer[command.DataOffset++] = (byte)writeAttr;
			command.DataBuffer[command.DataOffset++] = (byte)infoAttr;
			command.DataBuffer[command.DataOffset++] = 0; // unused
			command.DataBuffer[command.DataOffset++] = 0; // clear the result code
			command.DataOffset += ByteUtil.IntToBytes((uint)generation, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.IntToBytes((uint)policy.expiration, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.IntToBytes((uint)command.ServerTimeout, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)operationCount, command.DataBuffer, command.DataOffset);
		}

		/// <summary>
		/// Header write for operate command.
		/// </summary>
		private static void WriteHeaderReadWrite
		(
			this ICommand command,
			WritePolicy policy,
			OperateArgs args,
			int fieldCount
		)
		{
			// Set flags.
			int generation = 0;
			int ttl = args.hasWrite ? policy.expiration : policy.readTouchTtlPercent;
			int readAttr = args.readAttr;
			int writeAttr = args.writeAttr;
			int infoAttr = 0;
			int operationCount = args.operations.Length;

			switch (policy.recordExistsAction)
			{
				case RecordExistsAction.UPDATE:
					break;
				case RecordExistsAction.UPDATE_ONLY:
					infoAttr |= INFO3_UPDATE_ONLY;
					break;
				case RecordExistsAction.REPLACE:
					infoAttr |= INFO3_CREATE_OR_REPLACE;
					break;
				case RecordExistsAction.REPLACE_ONLY:
					infoAttr |= INFO3_REPLACE_ONLY;
					break;
				case RecordExistsAction.CREATE_ONLY:
					writeAttr |= INFO2_CREATE_ONLY;
					break;
			}

			switch (policy.generationPolicy)
			{
				case GenerationPolicy.NONE:
					break;
				case GenerationPolicy.EXPECT_GEN_EQUAL:
					generation = policy.generation;
					writeAttr |= INFO2_GENERATION;
					break;
				case GenerationPolicy.EXPECT_GEN_GT:
					generation = policy.generation;
					writeAttr |= INFO2_GENERATION_GT;
					break;
			}

			if (policy.commitLevel == CommitLevel.COMMIT_MASTER)
			{
				infoAttr |= INFO3_COMMIT_MASTER;
			}

			if (policy.durableDelete)
			{
				writeAttr |= INFO2_DURABLE_DELETE;
			}

			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |=	INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			if (policy.compress)
			{
				readAttr |= INFO1_COMPRESS_RESPONSE;
			}

			command.DataOffset += 8;

			// Write all header data except total size which must be written last. 
			command.DataBuffer[command.DataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			command.DataBuffer[command.DataOffset++] = (byte)readAttr;
			command.DataBuffer[command.DataOffset++] = (byte)writeAttr;
			command.DataBuffer[command.DataOffset++] = (byte)infoAttr;
			command.DataBuffer[command.DataOffset++] = 0; // unused
			command.DataBuffer[command.DataOffset++] = 0; // clear the result code
			command.DataOffset += ByteUtil.IntToBytes((uint)generation, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.IntToBytes((uint)ttl, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.IntToBytes((uint)command.ServerTimeout, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)operationCount, command.DataBuffer, command.DataOffset);
		}

		/// <summary>
		/// Header write for read commands.
		/// </summary>
		private static void WriteHeaderRead
		(
			this ICommand command,
			Policy policy,
			int timeout,
			int readAttr,
			int writeAttr,
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
					infoAttr |= INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			if (policy.compress)
			{
				readAttr |= INFO1_COMPRESS_RESPONSE;
			}

			command.DataOffset += 8;

			// Write all header data except total size which must be written last. 
			command.DataBuffer[command.DataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			command.DataBuffer[command.DataOffset++] = (byte)readAttr;
			command.DataBuffer[command.DataOffset++] = (byte)writeAttr;
			command.DataBuffer[command.DataOffset++] = (byte)infoAttr;

			for (int i = 0; i < 6; i++)
			{
				command.DataBuffer[command.DataOffset++] = 0;
			}
			command.DataOffset += ByteUtil.IntToBytes((uint)policy.readTouchTtlPercent, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.IntToBytes((uint)timeout, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)operationCount, command.DataBuffer, command.DataOffset);
		}

		/// <summary>
		/// Header write for read header commands.
		/// </summary>
		private static void WriteHeaderReadHeader(this ICommand command, Policy policy, int readAttr, int fieldCount, int operationCount)
		{
			int infoAttr = 0;

			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					break;
				case ReadModeSC.LINEARIZE:
					infoAttr |= INFO3_SC_READ_TYPE;
					break;
				case ReadModeSC.ALLOW_REPLICA:
					infoAttr |= INFO3_SC_READ_RELAX;
					break;
				case ReadModeSC.ALLOW_UNAVAILABLE:
					infoAttr |= INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
					break;
			}

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= INFO1_READ_MODE_AP_ALL;
			}

			command.DataOffset += 8;

			// Write all header data except total size which must be written last. 
			command.DataBuffer[command.DataOffset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			command.DataBuffer[command.DataOffset++] = (byte)readAttr;
			command.DataBuffer[command.DataOffset++] = (byte)0;
			command.DataBuffer[command.DataOffset++] = (byte)infoAttr;

			for (int i = 0; i < 6; i++)
			{
				command.DataBuffer[command.DataOffset++] = 0;
			}
			command.DataOffset += ByteUtil.IntToBytes((uint)policy.readTouchTtlPercent, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.IntToBytes((uint)command.ServerTimeout, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)fieldCount, command.DataBuffer, command.DataOffset);
			command.DataOffset += ByteUtil.ShortToBytes((ushort)operationCount, command.DataBuffer, command.DataOffset);
		}

		private static void WriteKey(this ICommand command, Policy policy, Key key)
		{
			// Write key into DataBuffer.
			if (key.ns != null)
			{
				command.WriteField(key.ns, FieldType.NAMESPACE);
			}

			if (key.setName != null)
			{
				command.WriteField(key.setName, FieldType.TABLE);
			}

			command.WriteField(key.digest, FieldType.DIGEST_RIPE);

			if (policy.sendKey)
			{
				command.WriteField(key.userKey, FieldType.KEY);
			}
		}

		private static int WriteReadOnlyOperations(this ICommand command, Operation[] ops, int readAttr)
		{
			bool readBin = false;
			bool readHeader = false;

			foreach (Operation op in ops)
			{
				switch (op.type)
				{
					case Operation.Type.READ:
						// Read all bins if no bin is specified.
						if (op.binName == null)
						{
							readAttr |= INFO1_GET_ALL;
						}
						readBin = true;
						break;

					case Operation.Type.READ_HEADER:
						readHeader = true;
						break;

					default:
						break;
				}
				command.WriteOperation(op);
			}

			if (readHeader && !readBin)
			{
				readAttr |= INFO1_NOBINDATA;
			}
			return readAttr;
		}

		private static void WriteOperation(this ICommand command, Bin bin, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(bin.name, command.DataBuffer, command.DataOffset + OPERATION_HEADER_SIZE);
			int valueLength = bin.value.Write(command.DataBuffer, command.DataOffset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = Operation.GetProtocolType(operationType);
			command.DataBuffer[command.DataOffset++] = (byte) bin.value.Type;
			command.DataBuffer[command.DataOffset++] = (byte) 0;
			command.DataBuffer[command.DataOffset++] = (byte) nameLength;
			command.DataOffset += nameLength + valueLength;
		}

		private static void WriteOperation(this ICommand command, Operation operation)
		{
			int nameLength = ByteUtil.StringToUtf8(operation.binName, command.DataBuffer, command.DataOffset + OPERATION_HEADER_SIZE);
			int valueLength = operation.value.Write(command.DataBuffer, command.DataOffset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = Operation.GetProtocolType(operation.type);
			command.DataBuffer[command.DataOffset++] = (byte) operation.value.Type;
			command.DataBuffer[command.DataOffset++] = (byte) 0;
			command.DataBuffer[command.DataOffset++] = (byte) nameLength;
			command.DataOffset += nameLength + valueLength;
		}

		private static void WriteOperation(this ICommand command, string name, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(name, command.DataBuffer, command.DataOffset + OPERATION_HEADER_SIZE);

			ByteUtil.IntToBytes((uint)(nameLength + 4), command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = Operation.GetProtocolType(operationType);
			command.DataBuffer[command.DataOffset++] = (byte) 0;
			command.DataBuffer[command.DataOffset++] = (byte) 0;
			command.DataBuffer[command.DataOffset++] = (byte) nameLength;
			command.DataOffset += nameLength;
		}

		private static void WriteOperation(this ICommand command, Operation.Type operationType)
		{
			ByteUtil.IntToBytes(4, command.DataBuffer, command.DataOffset);
			command.DataOffset += 4;
			command.DataBuffer[command.DataOffset++] = Operation.GetProtocolType(operationType);
			command.DataBuffer[command.DataOffset++] = 0;
			command.DataBuffer[command.DataOffset++] = 0;
			command.DataBuffer[command.DataOffset++] = 0;
		}

		private static void WriteField(this ICommand command, Value value, int type)
		{
			int offset = command.DataOffset + FIELD_HEADER_SIZE;
			command.DataBuffer[offset++] = (byte)value.Type;
			int len = value.Write(command.DataBuffer, offset) + 1;
			command.WriteFieldHeader(len, type);
			command.DataOffset += len;
		}

		private static void WriteField(this ICommand command, string str, int type)
		{
			int len = ByteUtil.StringToUtf8(str, command.DataBuffer, command.DataOffset + FIELD_HEADER_SIZE);
			command.WriteFieldHeader(len, type);
			command.DataOffset += len;
		}

		private static void WriteField(this ICommand command, byte[] bytes, int type)
		{
			Array.Copy(bytes, 0, command.DataBuffer, command.DataOffset + FIELD_HEADER_SIZE, bytes.Length);
			command.WriteFieldHeader(bytes.Length, type);
			command.DataOffset += bytes.Length;
		}

		private static void WriteField(this ICommand command, int val, int type)
		{
			command.WriteFieldHeader(4, type);
			command.DataOffset += ByteUtil.IntToBytes((uint)val, command.DataBuffer, command.DataOffset);
		}

		private static void WriteField(this ICommand command, ulong val, int type)
		{
			command.WriteFieldHeader(8, type);
			command.DataOffset += ByteUtil.LongToBytes(val, command.DataBuffer, command.DataOffset);
		}

		private static void WriteFieldHeader(this ICommand command, int size, int type)
		{
			command.DataOffset += ByteUtil.IntToBytes((uint)size + 1, command.DataBuffer, command.DataOffset);
			command.DataBuffer[command.DataOffset++] = (byte)type;
		}

		internal static void WriteExpHeader(this ICommand command, int size)
		{
			command.WriteFieldHeader(size, FieldType.FILTER_EXP);
		}

		private static void Begin(this ICommand command)
		{
			command.DataOffset = MSG_TOTAL_HEADER_SIZE;
		}

		private static bool SizeBuffer(this ICommand command, Policy policy)
		{
			if (policy.compress && command.DataOffset > COMPRESS_THRESHOLD)
			{
				// ICommand will be compressed. First, write uncompressed command
				// into separate DataBuffer. Save normal DataBuffer for compressed command.
				// Normal DataBuffer in async mode is from DataBuffer pool that is used to
				// minimize memory pinning during socket operations.
				command.DataBuffer = command.BufferPool.Rent(command.DataOffset);
				command.DataOffset = 0;
				return true;
			}
			else
			{
				// ICommand will be uncompressed.
				command.SizeBuffer();
				return false;
			}
		}

		private static void End(this ICommand command, bool compress)
		{
			if (!compress)
			{
				command.End();
				return;
			}

			// Write proto header.
			ulong size = ((ulong)command.DataOffset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, command.DataBuffer, 0);

			byte[] srcBuf = command.DataBuffer;
			int srcSize = command.DataOffset;

			// Increase requested DataBuffer size in case compressed DataBuffer size is
			// greater than the uncompressed DataBuffer size.
			command.DataOffset += 16 + 100;

			// This method finds DataBuffer of requested size, resets DataOffset to segment offset
			// and returns DataBuffer max size;
			int trgBufSize = command.SizeBuffer();

			// Compress to target starting at new DataOffset plus new header.
			int trgSize = ByteUtil.Compress(srcBuf, srcSize, command.DataBuffer, command.DataOffset + 16, trgBufSize - 16) + 16;

			ulong proto = ((ulong)trgSize - 8) | (CL_MSG_VERSION << 56) | (MSG_TYPE_COMPRESSED << 48);
			ByteUtil.LongToBytes(proto, command.DataBuffer, command.DataOffset);
			ByteUtil.LongToBytes((ulong)srcSize, command.DataBuffer, command.DataOffset + 8);
			command.SetLength(trgSize);
		}

		
		//--------------------------------------------------
		// Response Parsing
		//--------------------------------------------------

		internal static void SkipKey(this ICommand command, int fieldCount)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(command.DataBuffer, command.DataOffset);
				command.DataOffset += 4 + fieldlen;
			}
		}

		internal static Key ParseKey(this ICommand command, int fieldCount, out ulong bval)
		{
			byte[] digest = null;
			string ns = null;
			string setName = null;
			Value userKey = null;
			bval = 0;

			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(command.DataBuffer, command.DataOffset);
				command.DataOffset += 4;

				int fieldtype = command.DataBuffer[command.DataOffset++];
				int size = fieldlen - 1;

				switch (fieldtype)
				{
					case FieldType.DIGEST_RIPE:
						digest = new byte[size];
						Array.Copy(command.DataBuffer, command.DataOffset, digest, 0, size);
						break;

					case FieldType.NAMESPACE:
						ns = ByteUtil.Utf8ToString(command.DataBuffer, command.DataOffset, size);
						break;

					case FieldType.TABLE:
						setName = ByteUtil.Utf8ToString(command.DataBuffer, command.DataOffset, size);
						break;

					case FieldType.KEY:
						int type = command.DataBuffer[command.DataOffset++];
						size--;
						userKey = ByteUtil.BytesToKeyValue((ParticleType)type, command.DataBuffer, command.DataOffset, size);
						break;

					case FieldType.BVAL_ARRAY:
						bval = (ulong)ByteUtil.LittleBytesToLong(command.DataBuffer, command.DataOffset);
						break;
				}
				command.DataOffset += size;
			}
			return new Key(ns, digest, setName, userKey);
		}
	}
}
