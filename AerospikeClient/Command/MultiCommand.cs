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
using System;
using static Aerospike.Client.Connection;

namespace Aerospike.Client
{
	public abstract class MultiCommand : SyncCommand
	{
		private const int MAX_BUFFER_SIZE = 1024 * 1024 * 128;  // 128 MB

		private readonly Node node;
		protected internal readonly String ns;
		private readonly ulong clusterKey;
		protected internal int info3;
		protected internal int resultCode;
		protected internal int generation;
		protected internal int expiration;
		protected internal int batchIndex;
		protected internal int fieldCount;
		protected internal int opCount;
		protected internal readonly bool isOperation;
		private readonly bool first;
		protected internal volatile bool valid = true;

		/// <summary>
		/// Batch and server execute constructor.
		/// </summary>
		protected internal MultiCommand(Cluster cluster, Policy policy, Node node, bool isOperation)
			: base(cluster, policy)
		{
			this.node = node;
			this.isOperation = isOperation;
			this.ns = null;
			this.clusterKey = 0;
			this.first = false;
		}

		/// <summary>
		/// Partition scan/query constructor.
		/// </summary>
		protected internal MultiCommand(Cluster cluster, Policy policy, Node node, String ns, int socketTimeout, int totalTimeout)
			: base(cluster, policy, socketTimeout, totalTimeout)
		{
			this.node = node;
			this.isOperation = false;
			this.ns = ns;
			this.clusterKey = 0;
			this.first = false;
		}

		/// <summary>
		/// Legacy scan/query constructor.
		/// </summary>
		protected internal MultiCommand(Cluster cluster, Policy policy, Node node, String ns, ulong clusterKey, bool first)
			: base(cluster, policy, policy.socketTimeout, policy.totalTimeout)
		{
			this.node = node;
			this.isOperation = false;
			this.ns = ns;
			this.clusterKey = clusterKey;
			this.first = first;
		}

		public void ExecuteAndValidate()
		{
			if (clusterKey != 0)
			{
				if (!first)
				{
					QueryValidate.Validate(node, ns, clusterKey);
				}
				base.Execute();
				QueryValidate.Validate(node, ns, clusterKey);
			}
			else
			{
				base.Execute();
			}
		}

		protected override bool IsSingle()
		{
			return false;
		}

		protected internal override Node GetNode()
		{
			return node;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			return true;
		}

		protected internal sealed override void ParseResult(IConnection conn)
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
				conn.ReadFully(protoBuf, 8, Command.STATE_READ_HEADER);

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

				try
				{
					// Read remaining message bytes in group.
					conn.ReadFully(buf, size, Command.STATE_READ_DETAIL);
					conn.UpdateLastUsed();
				}
				catch (ReadTimeout rt)
				{
					if (rt.offset >= 4)
					{
						throw;
					}

					// First 4 bytes of detail contains whether this is the last
					// group to be sent.  Consider this as part of header.
					// Copy proto back into buffer to complete header.
					byte[] b = new byte[12];
					int count = 0;

					for (int i = 0; i < 8; i++)
					{
						b[count++] = protoBuf[i];
					}

					for (int i = 0; i < rt.offset; i++)
					{
						b[count++] = buf[i];
					}

					throw new ReadTimeout(b, rt.offset + 8, count, Command.STATE_READ_HEADER);
				}

				ulong type = (ulong)((proto >> 48) & 0xff);

				if (type == Command.AS_MSG_TYPE)
				{
					dataBuffer = buf;
					dataOffset = 0;
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
					dataBuffer = ubuf;
					dataOffset = 8;
					receiveSize = usize;
				}
				else
				{
					throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
				}

				if (! ParseGroup(receiveSize))
				{
					break;
				}
			}
		}

		private bool ParseGroup(int receiveSize)
		{
			while (dataOffset < receiveSize)
			{
				dataOffset += 3;
				info3 = dataBuffer[dataOffset];
				dataOffset += 2;
				resultCode = dataBuffer[dataOffset];

				// If this is the end marker of the response, do not proceed further.
				if ((info3 & Command.INFO3_LAST) != 0)
				{
					if (resultCode != 0)
					{
						// The server returned a fatal error.
						throw new AerospikeException(resultCode);
					}
					return false;
				}

				dataOffset++;
				generation = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;
				expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;
				batchIndex = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;
				fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
				dataOffset += 2;
				opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset);
				dataOffset += 2;

				// Note: ParseRow() also handles sync error responses.
				if (! ParseRow())
				{
					return false;
				}
			}
			return true;
		}

		protected internal abstract bool ParseRow();

		protected internal Record ParseRecord()
		{
			if (opCount <= 0)
			{
				return new Record(null, generation, expiration);
			}

			(Record record, dataOffset) = policy.recordParser.ParseRecord(dataBuffer, dataOffset, opCount, generation, expiration, isOperation);
			return record;
		}

		public void Stop()
		{
			valid = false;
		}

		public bool IsValid()
		{
			return valid;
		}
	}
}
