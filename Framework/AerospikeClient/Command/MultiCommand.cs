/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;

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
		private readonly bool stopOnNotFound;
		private readonly bool first;
		protected internal volatile bool valid = true;

		/// <summary>
		/// Batch and server execute constructor.
		/// </summary>
		protected internal MultiCommand(Cluster cluster, Policy policy, Node node, bool stopOnNotFound)
			: base(cluster, policy)
		{
			this.node = node;
			this.stopOnNotFound = stopOnNotFound;
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
			this.stopOnNotFound = true;
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
			this.stopOnNotFound = true;
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

		protected internal override Node GetNode()
		{
			return node;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			return true;
		}

		protected internal sealed override void ParseResult(Connection conn)
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

				// The only valid server return codes are "ok" and "not found".
				// If other return codes are received, then abort the batch.
				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR || resultCode == ResultCode.FILTERED_OUT)
					{
						if (stopOnNotFound)
						{
							return false;
						}
					}
					else
					{
						throw new AerospikeException(resultCode);
					}
				}

				// If this is the end marker of the response, do not proceed further
				if ((info3 & Command.INFO3_LAST) != 0)
				{
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

				Key key = ParseKey(fieldCount);
				ParseRow(key);
			}
			return true;
		}
		
		protected internal Record ParseRecord()
		{
			Dictionary<string, object> bins = null;

			for (int i = 0; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 5;
				byte particleType = dataBuffer[dataOffset];
				dataOffset += 2;
				byte nameSize = dataBuffer[dataOffset++];
				string name = ByteUtil.Utf8ToString(dataBuffer, dataOffset, nameSize);
				dataOffset += nameSize;

				int particleBytesSize = opSize - (4 + nameSize);
				object value = ByteUtil.BytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
				dataOffset += particleBytesSize;

				if (bins == null)
				{
					bins = new Dictionary<string, object>();
				}
				bins[name] = value;
			}
			return new Record(bins, generation, expiration);
		}
		
		public void Stop()
		{
			valid = false;
		}

		public bool IsValid()
		{
			return valid;
		}

		protected internal abstract void ParseRow(Key key);
	}
}
