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
using Grpc.Core;
using Grpc.Net.Client;
using System.Collections;

#pragma warning disable 0618

namespace Aerospike.Client.Proxy
{
	public abstract class GRPCCommand : Command
	{

		//-------------------------------------------------------
		// Static variables.
		//-------------------------------------------------------

		internal static readonly String NotSupported = "Method not supported in proxy client: ";

		private const int MAX_BUFFER_SIZE = 1024 * 1024 * 128;  // 128 MB

		internal Buffer Buffer { get; set; }
		internal GrpcChannel Channel { get; set; }

		protected Policy Policy { get; }
		protected internal bool IsOperation { get; }

		protected internal String Ns { get; set; }
		protected internal int Info3 { get; set; }
		protected internal int ResultCode { get; set; }
		protected internal int Generation { get; set; }
		protected internal int Expiration { get; set; }
		protected internal int BatchIndex { get; set; }
		protected internal int FieldCount { get; set; }
		protected internal int OpCount { get; set; }
		protected internal volatile bool valid = true;

		protected Key Key { get; }

		public GRPCCommand(Buffer buffer, GrpcChannel channel, Policy policy)
			: base(policy.socketTimeout, policy.totalTimeout, 0)
		{
			Buffer = buffer;
			Channel = channel;
			this.Policy = policy;
			IsOperation = false;
		}

		public GRPCCommand(Buffer buffer, GrpcChannel channel, Policy policy, bool isOperation)
			: base(policy.socketTimeout, policy.totalTimeout, 0)
		{
			Buffer = buffer;
			Channel = channel;
			this.Policy = policy;
			this.IsOperation = isOperation;
		}

		public GRPCCommand(Buffer buffer, GrpcChannel channel, Policy policy, int socketTimeout, int totalTimeout)
			: base(socketTimeout, totalTimeout, 0)
		{
			Buffer = buffer;
			Channel = channel;
			this.Policy = policy;
			IsOperation = false;
		}

		public GRPCCommand(Buffer buffer, GrpcChannel channel, Policy policy, Key key)
			: base(policy.socketTimeout, policy.totalTimeout, 0)
		{
			Buffer = buffer;
			Channel = channel;
			this.Key = key;
			this.Policy = policy;
			IsOperation = false;
		}

		public GRPCCommand(Buffer buffer, GrpcChannel channel, Policy policy, Key key, bool isOperation)
			: base(policy.socketTimeout, policy.totalTimeout, 0)
		{
			Buffer = buffer;
			Channel = channel;
			this.Key = key;
			this.Policy = policy;
			this.IsOperation = isOperation;
		}

		protected internal abstract void WriteBuffer();

		protected internal virtual bool IsWrite()
		{
			return false;
		}

		protected internal sealed override int SizeBuffer()
		{
			if (Buffer.DataBuffer == null && Buffer.Offset > 0)
			{
				Buffer.Resize(Buffer.Offset);
			}
			else if (Buffer.DataBuffer != null && Buffer.Offset > Buffer.DataBuffer.Length)
			{
				Buffer.Resize(Buffer.Offset);
			}

			Buffer.Offset = 0;
			return Buffer.DataBuffer.Length;
		}

		protected internal void SizeBuffer(int size)
		{
			if (size > Buffer.DataBuffer.Length)
			{
				Buffer.Resize(size);
			}
		}

		protected internal sealed override void End()
		{
			// Write total size of message.
			ulong size = ((ulong)Buffer.Offset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, Buffer.DataBuffer, 0);
		}

		protected internal sealed override void SetLength(int length)
		{
			Buffer.Offset = length;
		}

		protected DateTime? GetDeadline()
		{
			if (totalTimeout > 0)
			{
				return DateTime.UtcNow.AddMilliseconds(totalTimeout);
			}
			else if (socketTimeout > 0)
			{
				return DateTime.UtcNow.AddMilliseconds(socketTimeout);
			}
			return null;
		}

		protected internal virtual void ParseResult(IConnection conn)
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
				conn.ReadFully(protoBuf, 8);

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
				conn.ReadFully(buf, size);
				conn.UpdateLastUsed();

				ulong type = (ulong)((proto >> 48) & 0xff);

				if (type == Command.AS_MSG_TYPE)
				{
					Buffer.DataBuffer = buf;
					Buffer.Offset = 0;
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
					Buffer.DataBuffer = ubuf;
					Buffer.Offset = 8;
					receiveSize = usize;
				}
				else
				{
					throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
				}

				if (!ParseGroup(receiveSize))
				{
					break;
				}
			}
		}

		protected internal async Task ParseResult(ConnectionProxyStream conn, CancellationToken token)
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
				token.ThrowIfCancellationRequested();

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
					Buffer.DataBuffer = buf;
					Buffer.Offset = 0;
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
					Buffer.DataBuffer = ubuf;
					Buffer.Offset = 8;
					receiveSize = usize;
				}
				else
				{
					throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
				}

				if (!ParseGroup(receiveSize))
				{
					break;
				}
			}
		}

		private bool ParseGroup(int receiveSize)
		{
			while (Buffer.Offset < receiveSize)
			{
				Buffer.Offset += 3;
				Info3 = Buffer.DataBuffer[Buffer.Offset];
				Buffer.Offset += 2;
				ResultCode = Buffer.DataBuffer[Buffer.Offset];

				Buffer.Offset++;
				Generation = ByteUtil.BytesToInt(Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;
				Expiration = ByteUtil.BytesToInt(Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;
				BatchIndex = ByteUtil.BytesToInt(Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;
				FieldCount = ByteUtil.BytesToShort(Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 2;
				OpCount = ByteUtil.BytesToShort(Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 2;

				// Note: ParseRow() also handles sync error responses.
				if (!ParseRow())
				{
					return false;
				}
			}
			return true;
		}

		protected internal abstract bool ParseRow();

		protected internal Record ParseRecord()
		{
			if (OpCount <= 0)
			{
				return new Record(null, Generation, Expiration);
			}

			return Policy.recordParser.ParseRecord(Buffer.DataBuffer, ref Buffer.Offset, OpCount, Generation, Expiration, IsOperation);
		}

		public void Stop()
		{
			valid = false;
		}

		//--------------------------------------------------
		// Writes
		//--------------------------------------------------

		public override void SetWrite(WritePolicy policy, Operation.Type operation, Key key, Bin[] bins)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}

			foreach (Bin bin in bins)
			{
				EstimateOperationSize(bin);
			}

			bool compress = SizeBuffer(policy);

			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, bins.Length);
			WriteKey(policy, key);

			policy.filterExp?.Write(this, Buffer);

			foreach (Bin bin in bins)
			{
				WriteOperation(bin, operation);
			}
			End(compress);
		}

		public override void SetDelete(WritePolicy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderWrite(policy, Command.INFO2_WRITE | Command.INFO2_DELETE, fieldCount, 0);
			WriteKey(policy, key);

			policy.filterExp?.Write(this, Buffer);
			End();
		}

		public override void SetTouch(WritePolicy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}
			EstimateOperationSize();
			SizeBuffer();
			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 1);
			WriteKey(policy, key);

			policy.filterExp?.Write(this, Buffer);
			WriteOperation(Operation.Type.TOUCH);
			End();
		}

		//--------------------------------------------------
		// Reads
		//--------------------------------------------------

		public override void SetExists(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(policy, key);

			policy.filterExp?.Write(this, Buffer);
			End();
		}

		public override void SetRead(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}
			SizeBuffer();
			WriteHeaderRead(policy, serverTimeout, Command.INFO1_READ | Command.INFO1_GET_ALL, 0, fieldCount, 0);
			WriteKey(policy, key);

			policy.filterExp?.Write(this, Buffer);
			End();
		}

		public override void SetRead(Policy policy, Key key, string[] binNames)
		{
			if (binNames != null)
			{
				Begin();
				int fieldCount = EstimateKeySize(policy, key);

				if (policy.filterExp != null)
				{
					Buffer.Offset += policy.filterExp.Size();
					fieldCount++;
				}

				foreach (string binName in binNames)
				{
					EstimateOperationSize(binName);
				}
				SizeBuffer();
				WriteHeaderRead(policy, serverTimeout, Command.INFO1_READ, 0, fieldCount, binNames.Length);
				WriteKey(policy, key);

				policy.filterExp?.Write(this, Buffer);

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

		public override void SetReadHeader(Policy policy, Key key)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}
			EstimateOperationSize((string)null);
			SizeBuffer();
			WriteHeaderReadHeader(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA, fieldCount, 0);
			WriteKey(policy, key);

			policy.filterExp?.Write(this, Buffer);
			End();
		}

		//--------------------------------------------------
		// Operate
		//--------------------------------------------------

		public override void SetOperate(WritePolicy policy, Key key, OperateArgs args)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}
			Buffer.Offset += args.size;

			bool compress = SizeBuffer(policy);

			WriteHeaderReadWrite(policy, args, fieldCount);
			WriteKey(policy, key);

			policy.filterExp?.Write(this, Buffer);

			foreach (Operation operation in args.operations)
			{
				WriteOperation(operation);
			}
			End(compress);
		}

		//--------------------------------------------------
		// UDF
		//--------------------------------------------------

		public override void SetUdf(WritePolicy policy, Key key, string packageName, string functionName, Value[] args)
		{
			Begin();
			int fieldCount = EstimateKeySize(policy, key);

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}
			byte[] argBytes = Packer.Pack(args);
			fieldCount += EstimateUdfSize(packageName, functionName, argBytes);

			bool compress = SizeBuffer(policy);

			WriteHeaderWrite(policy, Command.INFO2_WRITE, fieldCount, 0);
			WriteKey(policy, key);

			policy.filterExp?.Write(this, Buffer);
			WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
			WriteField(functionName, FieldType.UDF_FUNCTION);
			WriteField(argBytes, FieldType.UDF_ARGLIST);
			End(compress);
		}

		//--------------------------------------------------
		// Batch Read Only
		//--------------------------------------------------

		public override void SetBatchRead(BatchPolicy policy, List<BatchRead> records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			BatchRead prev = null;

			Begin();
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}

			Buffer.Offset += FIELD_HEADER_SIZE + 5;

			for (int i = 0; i < max; i++)
			{
				BatchRead record = records[offsets[i]];
				Key key = record.key;
				string[] binNames = record.binNames;
				Operation[] ops = record.ops;

				Buffer.Offset += key.digest.Length + 4;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from 
				// fixed variables.  It's fine if equality not determined correctly because it just 
				// results in more space used. The batch will still be correct.
				if (prev != null && prev.key.ns == key.ns && prev.key.setName == key.setName &&
					prev.binNames == binNames && prev.readAllBins == record.readAllBins &&
					prev.ops == ops)
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.Offset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE + 6;
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							EstimateOperationSize(binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							EstimateReadOperationSize(op);
						}
					}
					prev = record;
				}
			}

			bool compress = SizeBuffer(policy);

			int readAttr = Command.INFO1_READ;

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			WriteHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, 0, fieldCount, 0);

			policy.filterExp?.Write(this, Buffer);

			int fieldSizeOffset = Buffer.Offset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = (policy.allowInline) ? (byte)1 : (byte)0;
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;

				BatchRead record = records[index];
				Key key = record.key;
				string[] binNames = record.binNames;
				Operation[] ops = record.ops;
				byte[] digest = key.digest;
				Array.Copy(digest, 0, Buffer.DataBuffer, Buffer.Offset, digest.Length);
				Buffer.Offset += digest.Length;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from 
				// fixed variables.  It's fine if equality not determined correctly because it just 
				// results in more space used. The batch will still be correct.		
				if (prev != null && prev.key.ns == key.ns && prev.key.setName == key.setName &&
					prev.binNames == binNames && prev.readAllBins == record.readAllBins &&
					prev.ops == ops)
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.DataBuffer[Buffer.Offset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full header, namespace and bin names.
					Buffer.DataBuffer[Buffer.Offset++] = BATCH_MSG_READ;

					if (binNames != null && binNames.Length != 0)
					{
						Buffer.DataBuffer[Buffer.Offset++] = (byte)readAttr;
						WriteBatchFields(key, 0, binNames.Length);

						foreach (string binName in binNames)
						{
							WriteOperation(binName, Operation.Type.READ);
						}
					}
					else if (ops != null)
					{
						int offset = Buffer.Offset++;
						WriteBatchFields(key, 0, ops.Length);
						Buffer.DataBuffer[offset] = (byte)WriteReadOnlyOperations(ops, readAttr);
					}
					else
					{
						Buffer.DataBuffer[Buffer.Offset++] = (byte)(readAttr | (record.readAllBins ? Command.INFO1_GET_ALL : Command.INFO1_NOBINDATA));
						WriteBatchFields(key, 0, 0);
					}
					prev = record;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(Buffer.Offset - MSG_TOTAL_HEADER_SIZE - 4), Buffer.DataBuffer, fieldSizeOffset);
			End(compress);
		}

		public override void SetBatchRead
		(
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

			// Estimate Buffer size.
			Begin();
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}
			Buffer.Offset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				Buffer.Offset += key.digest.Length + 4;

				// Try reference equality in hope that namespace for all keys is set from a fixed variable.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.Offset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE + 6;
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (binNames != null)
					{
						foreach (String binName in binNames)
						{
							EstimateOperationSize(binName);
						}
					}
					else if (ops != null)
					{
						foreach (Operation op in ops)
						{
							EstimateReadOperationSize(op);
						}
					}
					prev = key;
				}
			}

			bool compress = SizeBuffer(policy);

			if (policy.readModeAP == ReadModeAP.ALL)
			{
				readAttr |= Command.INFO1_READ_MODE_AP_ALL;
			}

			WriteHeaderRead(policy, totalTimeout, readAttr | Command.INFO1_BATCH, 0, fieldCount, 0);

			policy.filterExp?.Write(this, Buffer);

			int fieldSizeOffset = Buffer.Offset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = (policy.allowInline) ? (byte)1 : (byte)0;
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;

				Key key = keys[index];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, Buffer.DataBuffer, Buffer.Offset, digest.Length);
				Buffer.Offset += digest.Length;

				// Try reference equality in hope that namespace for all keys is set from a fixed variable.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.DataBuffer[Buffer.Offset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full header, namespace and bin names.
					Buffer.DataBuffer[Buffer.Offset++] = BATCH_MSG_READ;

					if (binNames != null && binNames.Length != 0)
					{
						Buffer.DataBuffer[Buffer.Offset++] = (byte)readAttr;
						WriteBatchFields(key, 0, binNames.Length);

						foreach (String binName in binNames)
						{
							WriteOperation(binName, Operation.Type.READ);
						}
					}
					else if (ops != null)
					{
						int offset = Buffer.Offset++;
						WriteBatchFields(key, 0, ops.Length);
						Buffer.DataBuffer[offset] = (byte)WriteReadOnlyOperations(ops, readAttr);
					}
					else
					{
						Buffer.DataBuffer[Buffer.Offset++] = (byte)readAttr;
						WriteBatchFields(key, 0, 0);
					}
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(Buffer.Offset - MSG_TOTAL_HEADER_SIZE - 4), Buffer.DataBuffer, fieldSizeOffset);
			End(compress);
		}

		//--------------------------------------------------
		// Batch Read/Write Operations
		//--------------------------------------------------

		public override void SetBatchOperate(BatchPolicy policy, IList records, BatchNode batch)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;
			BatchRecord prev = null;

			Begin();
			int fieldCount = 1;

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}

			Buffer.Offset += FIELD_HEADER_SIZE + 5;

			for (int i = 0; i < max; i++)
			{
				BatchRecord record = (BatchRecord)records[offsets[i]];
				Key key = record.key;

				Buffer.Offset += key.digest.Length + 4;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from
				// fixed variables.  It's fine if equality not determined correctly because it just
				// results in more space used. The batch will still be correct.
				if (prev != null && prev.key.ns == key.ns && prev.key.setName == key.setName && record.Equals(prev))
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.Offset++;
				}
				else
				{
					// Estimate full header, namespace and bin names.
					Buffer.Offset += 12;
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
					Buffer.Offset += record.Size(policy);
					prev = record;
				}
			}

			bool compress = SizeBuffer(policy);

			WriteBatchHeader(policy, totalTimeout, fieldCount);

			policy.filterExp?.Write(this, Buffer);

			int fieldSizeOffset = Buffer.Offset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = GetBatchFlags(policy);

			BatchAttr attr = new();
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;

				BatchRecord record = (BatchRecord)records[index];
				Key key = record.key;
				byte[] digest = key.digest;
				Array.Copy(digest, 0, Buffer.DataBuffer, Buffer.Offset, digest.Length);
				Buffer.Offset += digest.Length;

				// Avoid relatively expensive full equality checks for performance reasons.
				// Use reference equality only in hope that common namespaces/bin names are set from
				// fixed variables.  It's fine if equality not determined correctly because it just
				// results in more space used. The batch will still be correct.
				if (prev != null && prev.key.ns == key.ns && prev.key.setName == key.setName && record.Equals(prev))
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.DataBuffer[Buffer.Offset++] = BATCH_MSG_REPEAT;
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
									WriteBatchBinNames(key, br.binNames, attr, attr.filterExp);
								}
								else if (br.ops != null)
								{
									attr.AdjustRead(br.ops);
									WriteBatchOperations(key, br.ops, attr, attr.filterExp);
								}
								else
								{
									attr.AdjustRead(br.readAllBins);
									WriteBatchRead(key, attr, attr.filterExp, 0);
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
								WriteBatchOperations(key, bw.ops, attr, attr.filterExp);
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
								WriteBatchWrite(key, attr, attr.filterExp, 3, 0);
								WriteField(bu.packageName, FieldType.UDF_PACKAGE_NAME);
								WriteField(bu.functionName, FieldType.UDF_FUNCTION);
								WriteField(bu.argBytes, FieldType.UDF_ARGLIST);
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
								WriteBatchWrite(key, attr, attr.filterExp, 0, 0);
								break;
							}
					}
					prev = record;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(Buffer.Offset - MSG_TOTAL_HEADER_SIZE - 4), Buffer.DataBuffer, fieldSizeOffset);
			End(compress);
		}

		public void SetBatchOperate
		(
			BatchPolicy policy,
			BatchRecord[] records,
			BatchNode batch,
			string[] binNames,
			Operation[] ops,
			BatchAttr attr
		)
		{
			// Estimate full row size
			int[] offsets = batch.offsets;
			int max = batch.offsetsSize;

			// Estimate Buffer size.
			Begin();
			int fieldCount = 1;
			Expression exp = GetBatchExpression(policy, attr);

			if (exp != null)
			{
				Buffer.Offset += exp.Size();
				fieldCount++;
			}

			Buffer.Offset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = records[offsets[i]].key;

				Buffer.Offset += key.digest.Length + 4;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.Offset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					Buffer.Offset += 12; // header(4) + ttl(4) + fielCount(2) + opCount(2) = 8
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (attr.sendKey)
					{
						Buffer.Offset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
					}

					if (binNames != null)
					{
						foreach (string binName in binNames)
						{
							EstimateOperationSize(binName);
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
									throw new AerospikeException(Client.ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
								}
								Buffer.Offset += 2; // Extra write specific fields.
							}
							EstimateOperationSize(op);
						}
					}
					else if ((attr.writeAttr & Command.INFO2_DELETE) != 0)
					{
						Buffer.Offset += 2; // Extra write specific fields.
					}
					prev = key;
				}
			}

			bool compress = SizeBuffer(policy);

			WriteBatchHeader(policy, totalTimeout, fieldCount);

			exp?.Write(this, Buffer);

			int fieldSizeOffset = Buffer.Offset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = GetBatchFlags(policy);
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;

				Key key = records[index].key;
				byte[] digest = key.digest;
				Array.Copy(digest, 0, Buffer.DataBuffer, Buffer.Offset, digest.Length);
				Buffer.Offset += digest.Length;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.DataBuffer[Buffer.Offset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					if (binNames != null)
					{
						WriteBatchBinNames(key, binNames, attr, null);
					}
					else if (ops != null)
					{
						WriteBatchOperations(key, ops, attr, null);
					}
					else if ((attr.writeAttr & Command.INFO2_DELETE) != 0)
					{
						WriteBatchWrite(key, attr, null, 0, 0);
					}
					else
					{
						WriteBatchRead(key, attr, null, 0);
					}
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(Buffer.Offset - MSG_TOTAL_HEADER_SIZE - 4), Buffer.DataBuffer, fieldSizeOffset);
			End(compress);
		}

		public override void SetBatchUDF
		(
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

			// Estimate Buffer size.
			Begin();
			int fieldCount = 1;
			Expression exp = GetBatchExpression(policy, attr);

			if (exp != null)
			{
				Buffer.Offset += exp.Size();
				fieldCount++;
			}

			Buffer.Offset += FIELD_HEADER_SIZE + 5;

			Key prev = null;

			for (int i = 0; i < max; i++)
			{
				Key key = keys[offsets[i]];

				Buffer.Offset += key.digest.Length + 4;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.Offset++;
				}
				else
				{
					// Write full header and namespace/set/bin names.
					Buffer.Offset += 12; // header(4) + ttl(4) + fielCount(2) + opCount(2) = 8
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
					Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;

					if (attr.sendKey)
					{
						Buffer.Offset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
					}
					Buffer.Offset += 2; // gen(2) = 6
					EstimateUdfSize(packageName, functionName, argBytes);
					prev = key;
				}
			}

			bool compress = SizeBuffer(policy);

			WriteBatchHeader(policy, totalTimeout, fieldCount);

			exp?.Write(this, Buffer);

			int fieldSizeOffset = Buffer.Offset;
			WriteFieldHeader(0, FieldType.BATCH_INDEX); // Need to update size at end

			ByteUtil.IntToBytes((uint)max, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = GetBatchFlags(policy);
			prev = null;

			for (int i = 0; i < max; i++)
			{
				int index = offsets[i];
				ByteUtil.IntToBytes((uint)index, Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;

				Key key = keys[index];
				byte[] digest = key.digest;
				Array.Copy(digest, 0, Buffer.DataBuffer, Buffer.Offset, digest.Length);
				Buffer.Offset += digest.Length;

				// Try reference equality in hope that namespace/set for all keys is set from fixed variables.
				if (prev != null && prev.ns == key.ns && prev.setName == key.setName)
				{
					// Can set repeat previous namespace/bin names to save space.
					Buffer.DataBuffer[Buffer.Offset++] = BATCH_MSG_REPEAT;
				}
				else
				{
					// Write full message.
					WriteBatchWrite(key, attr, null, 3, 0);
					WriteField(packageName, FieldType.UDF_PACKAGE_NAME);
					WriteField(functionName, FieldType.UDF_FUNCTION);
					WriteField(argBytes, FieldType.UDF_ARGLIST);
					prev = key;
				}
			}

			// Write real field size.
			ByteUtil.IntToBytes((uint)(Buffer.Offset - MSG_TOTAL_HEADER_SIZE - 4), Buffer.DataBuffer, fieldSizeOffset);
			End(compress);
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

		private void WriteBatchHeader(Policy policy, int timeout, int fieldCount)
		{
			int readAttr = Command.INFO1_BATCH;

			if (policy.compress)
			{
				readAttr |= Command.INFO1_COMPRESS_RESPONSE;
			}

			// Write all header data except total size which must be written last.
			Buffer.Offset += 8;
			Buffer.DataBuffer[Buffer.Offset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			Buffer.DataBuffer[Buffer.Offset++] = (byte)readAttr;

			Array.Clear(Buffer.DataBuffer, Buffer.Offset, 12);
			Buffer.Offset += 12;

			Buffer.Offset += ByteUtil.IntToBytes((uint)timeout, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)fieldCount, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes(0, Buffer.DataBuffer, Buffer.Offset);
		}

		private void WriteBatchBinNames(Key key, string[] binNames, BatchAttr attr, Expression filter)
		{
			WriteBatchRead(key, attr, filter, binNames.Length);

			foreach (string binName in binNames)
			{
				WriteOperation(binName, Operation.Type.READ);
			}
		}

		private void WriteBatchOperations(Key key, Operation[] ops, BatchAttr attr, Expression filter)
		{
			if (attr.hasWrite)
			{
				WriteBatchWrite(key, attr, filter, 0, ops.Length);
			}
			else
			{
				WriteBatchRead(key, attr, filter, ops.Length);
			}

			foreach (Operation op in ops)
			{
				WriteOperation(op);
			}
		}

		private void WriteBatchRead(Key key, BatchAttr attr, Expression filter, int opCount)
		{
			Buffer.DataBuffer[Buffer.Offset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_TTL);
			Buffer.DataBuffer[Buffer.Offset++] = (byte)attr.readAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)attr.writeAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)attr.infoAttr;
			Buffer.Offset += ByteUtil.IntToBytes((uint)attr.expiration, Buffer.DataBuffer, Buffer.Offset);
			WriteBatchFields(key, filter, 0, opCount);
		}

		private void WriteBatchWrite(Key key, BatchAttr attr, Expression filter, int fieldCount, int opCount)
		{
			Buffer.DataBuffer[Buffer.Offset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_GEN | BATCH_MSG_TTL);
			Buffer.DataBuffer[Buffer.Offset++] = (byte)attr.readAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)attr.writeAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)attr.infoAttr;
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)attr.generation, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.IntToBytes((uint)attr.expiration, Buffer.DataBuffer, Buffer.Offset);

			if (attr.sendKey)
			{
				fieldCount++;
				WriteBatchFields(key, filter, fieldCount, opCount);
				WriteField(key.userKey, FieldType.KEY);
			}
			else
			{
				WriteBatchFields(key, filter, fieldCount, opCount);
			}
		}

		private void WriteBatchFields(Key key, Expression filter, int fieldCount, int opCount)
		{
			if (filter != null)
			{
				fieldCount++;
				WriteBatchFields(key, fieldCount, opCount);
				filter.Write(this, Buffer);
			}
			else
			{
				WriteBatchFields(key, fieldCount, opCount);
			}
		}

		private void WriteBatchFields(Key key, int fieldCount, int opCount)
		{
			fieldCount += 2;
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)fieldCount, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)opCount, Buffer.DataBuffer, Buffer.Offset);
			WriteField(key.ns, FieldType.NAMESPACE);
			WriteField(key.setName, FieldType.TABLE);
		}

		//--------------------------------------------------
		// Scan
		//--------------------------------------------------

		public void SetScan
		(
			ScanPolicy policy,
			string ns,
			string setName,
			string[] binNames,
			ulong taskId
		)
		{
			Begin();
			int fieldCount = 0;

			if (ns != null)
			{
				Buffer.Offset += ByteUtil.EstimateSizeUtf8(ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (setName != null)
			{
				Buffer.Offset += ByteUtil.EstimateSizeUtf8(setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (policy.recordsPerSecond > 0)
			{
				Buffer.Offset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}

			// Estimate scan timeout size.
			Buffer.Offset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate taskId size.
			Buffer.Offset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					EstimateOperationSize(binName);
				}
			}

			SizeBuffer();
			int readAttr = Command.INFO1_READ;

			if (!policy.includeBinData)
			{
				readAttr |= Command.INFO1_NOBINDATA;
			}

			// Clusters that support partition queries also support not sending partition done messages.
			int infoAttr = 0;
			int operationCount = (binNames == null) ? 0 : binNames.Length;
			WriteHeaderRead(policy, totalTimeout, readAttr, infoAttr, fieldCount, operationCount);

			if (ns != null)
			{
				WriteField(ns, FieldType.NAMESPACE);
			}

			if (setName != null)
			{
				WriteField(setName, FieldType.TABLE);
			}

			if (policy.recordsPerSecond > 0)
			{
				WriteField(policy.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			policy.filterExp?.Write(this, Buffer);

			// Write scan timeout
			WriteField(policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

			// Write taskId field
			WriteField(taskId, FieldType.TRAN_ID);

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		//--------------------------------------------------
		// Query
		//--------------------------------------------------

		protected internal void SetQuery
		(
			Policy policy,
			Statement statement,
			ulong taskId,
			bool background
		)
		{
			byte[] functionArgBuffer = null;
			int fieldCount = 0;
			int filterSize = 0;
			int binNameSize = 0;
			bool isNew = false;

			Begin();

			if (statement.ns != null)
			{
				Buffer.Offset += ByteUtil.EstimateSizeUtf8(statement.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (statement.setName != null)
			{
				Buffer.Offset += ByteUtil.EstimateSizeUtf8(statement.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate recordsPerSecond field size. This field is used in new servers and not used
			// (but harmless to add) in old servers.
			if (statement.recordsPerSecond > 0)
			{
				Buffer.Offset += 4 + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			// Estimate socket timeout field size. This field is used in new servers and not used
			// (but harmless to add) in old servers.
			Buffer.Offset += 4 + FIELD_HEADER_SIZE;
			fieldCount++;

			// Estimate taskId field.
			Buffer.Offset += 8 + FIELD_HEADER_SIZE;
			fieldCount++;

			byte[] packedCtx = null;

			if (statement.filter != null)
			{
				IndexCollectionType type = statement.filter.CollectionType;

				// Estimate INDEX_TYPE field.
				if (type != IndexCollectionType.DEFAULT)
				{
					Buffer.Offset += FIELD_HEADER_SIZE + 1;
					fieldCount++;
				}

				// Estimate INDEX_RANGE field.
				Buffer.Offset += FIELD_HEADER_SIZE;
				filterSize++; // num filters
				filterSize += statement.filter.EstimateSize();
				Buffer.Offset += filterSize;
				fieldCount++;

				if (!isNew)
				{
					// Query bin names are specified as a field (Scan bin names are specified later as operations)
					// in old servers. Estimate size for selected bin names.
					if (statement.binNames != null && statement.binNames.Length > 0)
					{
						Buffer.Offset += FIELD_HEADER_SIZE;
						binNameSize++; // num bin names

						foreach (string binName in statement.binNames)
						{
							binNameSize += ByteUtil.EstimateSizeUtf8(binName) + 1;
						}
						Buffer.Offset += binNameSize;
						fieldCount++;
					}
				}

				packedCtx = statement.filter.PackedCtx;

				if (packedCtx != null)
				{
					Buffer.Offset += FIELD_HEADER_SIZE + packedCtx.Length;
					fieldCount++;
				}
			}

			// Estimate aggregation/background function size.
			if (statement.functionName != null)
			{
				Buffer.Offset += FIELD_HEADER_SIZE + 1; // udf type
				Buffer.Offset += ByteUtil.EstimateSizeUtf8(statement.packageName) + FIELD_HEADER_SIZE;
				Buffer.Offset += ByteUtil.EstimateSizeUtf8(statement.functionName) + FIELD_HEADER_SIZE;

				if (statement.functionArgs.Length > 0)
				{
					functionArgBuffer = Packer.Pack(statement.functionArgs);
				}
				else
				{
					functionArgBuffer = Array.Empty<byte>();
				}
				Buffer.Offset += FIELD_HEADER_SIZE + functionArgBuffer.Length;
				fieldCount += 4;
			}

			if (policy.filterExp != null)
			{
				Buffer.Offset += policy.filterExp.Size();
				fieldCount++;
			}

			// Operations (used in query execute) and bin names (used in scan/query) are mutually exclusive.
			int operationCount = 0;

			if (statement.operations != null)
			{
				// Estimate size for background operations.
				if (!background)
				{
					throw new AerospikeException(Client.ResultCode.PARAMETER_ERROR, "Operations not allowed in foreground query");
				}

				foreach (Operation operation in statement.operations)
				{
					if (!Operation.IsWrite(operation.type))
					{
						throw new AerospikeException(Client.ResultCode.PARAMETER_ERROR, "Read operations not allowed in background query");
					}
					EstimateOperationSize(operation);
				}
				operationCount = statement.operations.Length;
			}
			else if (statement.binNames != null && (isNew || statement.filter == null))
			{
				// Estimate size for selected bin names (query bin names already handled for old servers).
				foreach (string binName in statement.binNames)
				{
					EstimateOperationSize(binName);
				}
				operationCount = statement.binNames.Length;
			}

			SizeBuffer();

			if (background)
			{
				WriteHeaderWrite((WritePolicy)policy, Command.INFO2_WRITE, fieldCount, operationCount);
			}
			else
			{
				QueryPolicy qp = (QueryPolicy)policy;
				int readAttr = Command.INFO1_READ;

				if (!qp.includeBinData)
				{
					readAttr |= Command.INFO1_NOBINDATA;
				}

				if (qp.shortQuery)
				{
					readAttr |= Command.INFO1_SHORT_QUERY;
				}

				int infoAttr = isNew ? Command.INFO3_PARTITION_DONE : 0;

				WriteHeaderRead(policy, totalTimeout, readAttr, infoAttr, fieldCount, operationCount);
			}

			if (statement.ns != null)
			{
				WriteField(statement.ns, FieldType.NAMESPACE);
			}

			if (statement.setName != null)
			{
				WriteField(statement.setName, FieldType.TABLE);
			}

			// Write records per second.
			if (statement.recordsPerSecond > 0)
			{
				WriteField(statement.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
			}

			// Write socket idle timeout.
			WriteField(policy.socketTimeout, FieldType.SOCKET_TIMEOUT);

			// Write taskId field
			WriteField(taskId, FieldType.TRAN_ID);

			if (statement.filter != null)
			{
				IndexCollectionType type = statement.filter.CollectionType;

				if (type != IndexCollectionType.DEFAULT)
				{
					WriteFieldHeader(1, FieldType.INDEX_TYPE);
					Buffer.DataBuffer[Buffer.Offset++] = (byte)type;
				}

				WriteFieldHeader(filterSize, FieldType.INDEX_RANGE);
				Buffer.DataBuffer[Buffer.Offset++] = (byte)1;
				Buffer.Offset = statement.filter.Write(Buffer.DataBuffer, Buffer.Offset);

				if (!isNew)
				{
					// Query bin names are specified as a field (Scan bin names are specified later as operations)
					// in old servers.
					if (statement.binNames != null && statement.binNames.Length > 0)
					{
						WriteFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
						Buffer.DataBuffer[Buffer.Offset++] = (byte)statement.binNames.Length;

						foreach (string binName in statement.binNames)
						{
							int len = ByteUtil.StringToUtf8(binName, Buffer.DataBuffer, Buffer.Offset + 1);
							Buffer.DataBuffer[Buffer.Offset] = (byte)len;
							Buffer.Offset += len + 1;
						}
					}
				}

				if (packedCtx != null)
				{
					WriteFieldHeader(packedCtx.Length, FieldType.INDEX_CONTEXT);
					Array.Copy(packedCtx, 0, Buffer.DataBuffer, Buffer.Offset, packedCtx.Length);
					Buffer.Offset += packedCtx.Length;
				}
			}

			if (statement.functionName != null)
			{
				WriteFieldHeader(1, FieldType.UDF_OP);
				Buffer.DataBuffer[Buffer.Offset++] = background ? (byte)2 : (byte)1;
				WriteField(statement.packageName, FieldType.UDF_PACKAGE_NAME);
				WriteField(statement.functionName, FieldType.UDF_FUNCTION);
				WriteField(functionArgBuffer, FieldType.UDF_ARGLIST);
			}

			policy.filterExp?.Write(this, Buffer);

			if (statement.operations != null)
			{
				foreach (Operation operation in statement.operations)
				{
					WriteOperation(operation);
				}
			}
			else if (statement.binNames != null && (isNew || statement.filter == null))
			{
				foreach (string binName in statement.binNames)
				{
					WriteOperation(binName, Operation.Type.READ);
				}
			}
			End();
		}

		//--------------------------------------------------
		// Command Sizing
		//--------------------------------------------------

		private int EstimateKeySize(Policy policy, Key key)
		{
			int fieldCount = 0;

			if (key.ns != null)
			{
				Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.ns) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (key.setName != null)
			{
				Buffer.Offset += ByteUtil.EstimateSizeUtf8(key.setName) + FIELD_HEADER_SIZE;
				fieldCount++;
			}

			Buffer.Offset += key.digest.Length + FIELD_HEADER_SIZE;
			fieldCount++;

			if (policy.sendKey)
			{
				Buffer.Offset += key.userKey.EstimateSize() + FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}
			return fieldCount;
		}

		private int EstimateUdfSize(string packageName, string functionName, byte[] bytes)
		{
			Buffer.Offset += ByteUtil.EstimateSizeUtf8(packageName) + FIELD_HEADER_SIZE;
			Buffer.Offset += ByteUtil.EstimateSizeUtf8(functionName) + FIELD_HEADER_SIZE;
			Buffer.Offset += bytes.Length + FIELD_HEADER_SIZE;
			return 3;
		}

		private void EstimateOperationSize(Bin bin)
		{
			Buffer.Offset += ByteUtil.EstimateSizeUtf8(bin.name) + OPERATION_HEADER_SIZE;
			Buffer.Offset += bin.value.EstimateSize();
		}

		private void EstimateOperationSize(Operation operation)
		{
			Buffer.Offset += ByteUtil.EstimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
			Buffer.Offset += operation.value.EstimateSize();
		}

		private void EstimateReadOperationSize(Operation operation)
		{
			if (Operation.IsWrite(operation.type))
			{
				throw new AerospikeException(Client.ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
			}
			Buffer.Offset += ByteUtil.EstimateSizeUtf8(operation.binName) + OPERATION_HEADER_SIZE;
			Buffer.Offset += operation.value.EstimateSize();
		}

		private void EstimateOperationSize(string binName)
		{
			Buffer.Offset += ByteUtil.EstimateSizeUtf8(binName) + OPERATION_HEADER_SIZE;
		}

		private void EstimateOperationSize()
		{
			Buffer.Offset += OPERATION_HEADER_SIZE;
		}

		//--------------------------------------------------
		// Command Writes
		//--------------------------------------------------

		/// <summary>
		/// Header write for write commands.
		/// </summary>
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

			Buffer.Offset += 8;

			// Write all header data except total size which must be written last. 
			Buffer.DataBuffer[Buffer.Offset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			Buffer.DataBuffer[Buffer.Offset++] = (byte)0;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)writeAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)infoAttr;
			Buffer.DataBuffer[Buffer.Offset++] = 0; // unused
			Buffer.DataBuffer[Buffer.Offset++] = 0; // clear the result code
			Buffer.Offset += ByteUtil.IntToBytes((uint)generation, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.IntToBytes((uint)policy.expiration, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.IntToBytes((uint)serverTimeout, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)fieldCount, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)operationCount, Buffer.DataBuffer, Buffer.Offset);
		}

		/// <summary>
		/// Header write for operate command.
		/// </summary>
		private void WriteHeaderReadWrite
		(
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

			Buffer.Offset += 8;

			// Write all header data except total size which must be written last. 
			Buffer.DataBuffer[Buffer.Offset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			Buffer.DataBuffer[Buffer.Offset++] = (byte)readAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)writeAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)infoAttr;
			Buffer.DataBuffer[Buffer.Offset++] = 0; // unused
			Buffer.DataBuffer[Buffer.Offset++] = 0; // clear the result code
			Buffer.Offset += ByteUtil.IntToBytes((uint)generation, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.IntToBytes((uint)ttl, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.IntToBytes((uint)serverTimeout, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)fieldCount, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)operationCount, Buffer.DataBuffer, Buffer.Offset);
		}

		/// <summary>
		/// Header write for read commands.
		/// </summary>
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

			Buffer.Offset += 8;

			// Write all header data except total size which must be written last. 
			Buffer.DataBuffer[Buffer.Offset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			Buffer.DataBuffer[Buffer.Offset++] = (byte)readAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)0;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)infoAttr;

			for (int i = 0; i < 6; i++)
			{
				Buffer.DataBuffer[Buffer.Offset++] = 0;
			}
			Buffer.Offset += ByteUtil.IntToBytes((uint)policy.readTouchTtlPercent, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.IntToBytes((uint)timeout, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)fieldCount, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)operationCount, Buffer.DataBuffer, Buffer.Offset);
		}

		/// <summary>
		/// Header write for read header commands.
		/// </summary>
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

			Buffer.Offset += 8;

			// Write all header data except total size which must be written last. 
			Buffer.DataBuffer[Buffer.Offset++] = MSG_REMAINING_HEADER_SIZE; // Message header length.
			Buffer.DataBuffer[Buffer.Offset++] = (byte)readAttr;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)0;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)infoAttr;

			for (int i = 0; i < 6; i++)
			{
				Buffer.DataBuffer[Buffer.Offset++] = 0;
			}
			Buffer.Offset += ByteUtil.IntToBytes((uint)policy.readTouchTtlPercent, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.IntToBytes((uint)serverTimeout, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)fieldCount, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += ByteUtil.ShortToBytes((ushort)operationCount, Buffer.DataBuffer, Buffer.Offset);
		}

		private void WriteKey(Policy policy, Key key)
		{
			// Write key into Buffer.
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

		private int WriteReadOnlyOperations(Operation[] ops, int readAttr)
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
							readAttr |= Command.INFO1_GET_ALL;
						}
						readBin = true;
						break;

					case Operation.Type.READ_HEADER:
						readHeader = true;
						break;

					default:
						break;
				}
				WriteOperation(op);
			}

			if (readHeader && !readBin)
			{
				readAttr |= Command.INFO1_NOBINDATA;
			}
			return readAttr;
		}

		private void WriteOperation(Bin bin, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(bin.name, Buffer.DataBuffer, Buffer.Offset + OPERATION_HEADER_SIZE);
			int valueLength = bin.value.Write(Buffer.DataBuffer, Buffer.Offset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = Operation.GetProtocolType(operationType);
			Buffer.DataBuffer[Buffer.Offset++] = (byte)bin.value.Type;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)0;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)nameLength;
			Buffer.Offset += nameLength + valueLength;
		}

		private void WriteOperation(Operation operation)
		{
			int nameLength = ByteUtil.StringToUtf8(operation.binName, Buffer.DataBuffer, Buffer.Offset + OPERATION_HEADER_SIZE);
			int valueLength = operation.value.Write(Buffer.DataBuffer, Buffer.Offset + OPERATION_HEADER_SIZE + nameLength);

			ByteUtil.IntToBytes((uint)(nameLength + valueLength + 4), Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = Operation.GetProtocolType(operation.type);
			Buffer.DataBuffer[Buffer.Offset++] = (byte)operation.value.Type;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)0;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)nameLength;
			Buffer.Offset += nameLength + valueLength;
		}

		private void WriteOperation(string name, Operation.Type operationType)
		{
			int nameLength = ByteUtil.StringToUtf8(name, Buffer.DataBuffer, Buffer.Offset + OPERATION_HEADER_SIZE);

			ByteUtil.IntToBytes((uint)(nameLength + 4), Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = Operation.GetProtocolType(operationType);
			Buffer.DataBuffer[Buffer.Offset++] = (byte)0;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)0;
			Buffer.DataBuffer[Buffer.Offset++] = (byte)nameLength;
			Buffer.Offset += nameLength;
		}

		private void WriteOperation(Operation.Type operationType)
		{
			ByteUtil.IntToBytes(4, Buffer.DataBuffer, Buffer.Offset);
			Buffer.Offset += 4;
			Buffer.DataBuffer[Buffer.Offset++] = Operation.GetProtocolType(operationType);
			Buffer.DataBuffer[Buffer.Offset++] = 0;
			Buffer.DataBuffer[Buffer.Offset++] = 0;
			Buffer.DataBuffer[Buffer.Offset++] = 0;
		}

		private void WriteField(Value value, int type)
		{
			int offset = Buffer.Offset + FIELD_HEADER_SIZE;
			Buffer.DataBuffer[offset++] = (byte)value.Type;
			int len = value.Write(Buffer.DataBuffer, offset) + 1;
			WriteFieldHeader(len, type);
			Buffer.Offset += len;
		}

		private void WriteField(string str, int type)
		{
			int len = ByteUtil.StringToUtf8(str, Buffer.DataBuffer, Buffer.Offset + FIELD_HEADER_SIZE);
			WriteFieldHeader(len, type);
			Buffer.Offset += len;
		}

		private void WriteField(byte[] bytes, int type)
		{
			Array.Copy(bytes, 0, Buffer.DataBuffer, Buffer.Offset + FIELD_HEADER_SIZE, bytes.Length);
			WriteFieldHeader(bytes.Length, type);
			Buffer.Offset += bytes.Length;
		}

		private void WriteField(int val, int type)
		{
			WriteFieldHeader(4, type);
			Buffer.Offset += ByteUtil.IntToBytes((uint)val, Buffer.DataBuffer, Buffer.Offset);
		}

		private void WriteField(ulong val, int type)
		{
			WriteFieldHeader(8, type);
			Buffer.Offset += ByteUtil.LongToBytes(val, Buffer.DataBuffer, Buffer.Offset);
		}

		private void WriteFieldHeader(int size, int type)
		{
			Buffer.Offset += ByteUtil.IntToBytes((uint)size + 1, Buffer.DataBuffer, Buffer.Offset);
			Buffer.DataBuffer[Buffer.Offset++] = (byte)type;
		}

		internal override void WriteExpHeader(int size)
		{
			WriteFieldHeader(size, FieldType.FILTER_EXP);
		}

		private void Begin()
		{
			Buffer.Offset = MSG_TOTAL_HEADER_SIZE;
		}

		private bool SizeBuffer(Policy policy)
		{
			if (policy.compress && Buffer.Offset > COMPRESS_THRESHOLD)
			{
				// Command will be compressed. First, write uncompressed command
				// into separate Buffer. Save normal Buffer for compressed command.
				// Normal Buffer in async mode is from Buffer pool that is used to
				// minimize memory pinning during socket operations.
				//Buffer = new byte[Buffer.Offset];
				Buffer.Resize(Buffer.Offset);
				Buffer.Offset = 0;
				return true;
			}
			else
			{
				// Command will be uncompressed.
				SizeBuffer();
				return false;
			}
		}

		private void End(bool compress)
		{
			if (!compress)
			{
				End();
				return;
			}

			// Write proto header.
			ulong size = ((ulong)Buffer.Offset - 8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, Buffer.DataBuffer, 0);

			byte[] srcBuf = Buffer.DataBuffer;
			int srcSize = Buffer.Offset;

			// Increase requested Buffer size in case compressed Buffer size is
			// greater than the uncompressed Buffer size.
			Buffer.Offset += 16 + 100;

			// This method finds Buffer of requested size, resets Buffer.Offset to segment offset
			// and returns Buffer max size;
			int trgBufSize = SizeBuffer();

			// Compress to target starting at new Buffer.Offset plus new header.
			int trgSize = ByteUtil.Compress(srcBuf, srcSize, Buffer.DataBuffer, Buffer.Offset + 16, trgBufSize - 16) + 16;

			ulong proto = ((ulong)trgSize - 8) | (CL_MSG_VERSION << 56) | (MSG_TYPE_COMPRESSED << 48);
			ByteUtil.LongToBytes(proto, Buffer.DataBuffer, Buffer.Offset);
			ByteUtil.LongToBytes((ulong)srcSize, Buffer.DataBuffer, Buffer.Offset + 8);
			SetLength(trgSize);
		}

		//--------------------------------------------------
		// Response Parsing
		//--------------------------------------------------

		internal override void SkipKey(int fieldCount)
		{
			// There can be fields in the response (setname etc).
			// But for now, ignore them. Expose them to the API if needed in the future.
			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4 + fieldlen;
			}
		}

		internal override Key ParseKey(int fieldCount, out ulong bval)
		{
			byte[] digest = null;
			string ns = null;
			string setName = null;
			Value userKey = null;
			bval = 0;

			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(Buffer.DataBuffer, Buffer.Offset);
				Buffer.Offset += 4;

				int fieldtype = Buffer.DataBuffer[Buffer.Offset++];
				int size = fieldlen - 1;

				switch (fieldtype)
				{
					case FieldType.DIGEST_RIPE:
						digest = new byte[size];
						Array.Copy(Buffer.DataBuffer, Buffer.Offset, digest, 0, size);
						break;

					case FieldType.NAMESPACE:
						ns = ByteUtil.Utf8ToString(Buffer.DataBuffer, Buffer.Offset, size);
						break;

					case FieldType.TABLE:
						setName = ByteUtil.Utf8ToString(Buffer.DataBuffer, Buffer.Offset, size);
						break;

					case FieldType.KEY:
						int type = Buffer.DataBuffer[Buffer.Offset++];
						size--;
						userKey = ByteUtil.BytesToKeyValue((ParticleType)type, Buffer.DataBuffer, Buffer.Offset, size);
						break;

					case FieldType.BVAL_ARRAY:
						bval = (ulong)ByteUtil.LittleBytesToLong(Buffer.DataBuffer, Buffer.Offset);
						break;
				}
				Buffer.Offset += size;
			}
			return new Key(ns, digest, setName, userKey);
		}
	}
}
#pragma warning restore 0618
