/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
using System.Net.Sockets;

namespace Aerospike.Client
{
	public abstract class AsyncMultiCommand : AsyncCommand
	{
		protected internal readonly AsyncMultiExecutor parent;
		protected internal int resultCode;
		protected internal int generation;
		protected internal int expiration;
		protected internal int batchIndex;
		protected internal int fieldCount;
		protected internal int opCount;
		private readonly bool stopOnNotFound;
		protected internal volatile bool valid = true;

		public AsyncMultiCommand(AsyncMultiExecutor parent, AsyncCluster cluster, Policy policy, AsyncNode node, bool stopOnNotFound) 
			: base(cluster, policy, null, node, true)
		{
			this.parent = parent;
			this.stopOnNotFound = stopOnNotFound;
		}

		public AsyncMultiCommand(AsyncMultiCommand other) : base(other)
		{
			this.parent = other.parent;
			this.stopOnNotFound = other.stopOnNotFound;
		}

		protected internal sealed override void ParseCommand()
		{
			if (ParseGroup())
			{
				Finish();
				return;
			}
			// Prepare for next group.
			inHeader = true;
			ReceiveBegin();
		}

		private bool ParseGroup()
		{
			// Parse each message response and add it to the result array
			while (dataOffset < dataLength)
			{
				resultCode = dataBuffer[dataOffset + 5];

				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
					{
						if (stopOnNotFound)
						{
							return true;
						}
					}
					else
					{
						throw new AerospikeException(resultCode);
					}
				}

				// If this is the end marker of the response, do not proceed further
				if ((dataBuffer[dataOffset + 3] & Command.INFO3_LAST) != 0)
				{
					return true;
				}
				generation = ByteUtil.BytesToInt(dataBuffer, dataOffset + 6);
				expiration = ByteUtil.BytesToInt(dataBuffer, dataOffset + 10);
				batchIndex = ByteUtil.BytesToInt(dataBuffer, dataOffset + 14);
				fieldCount = ByteUtil.BytesToShort(dataBuffer, dataOffset + 18);
				opCount = ByteUtil.BytesToShort(dataBuffer, dataOffset + 20);

				dataOffset += Command.MSG_REMAINING_HEADER_SIZE;

				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}

				Key key = ParseKey();
				ParseRow(key);
			}
			return false;
		}

		protected internal Key ParseKey()
		{
			byte[] digest = null;
			string ns = null;
			string setName = null;
			Value userKey = null;

			for (int i = 0; i < fieldCount; i++)
			{
				int fieldlen = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;

				int fieldtype = dataBuffer[dataOffset++];
				int size = fieldlen - 1;

				switch (fieldtype)
				{
					case FieldType.DIGEST_RIPE:
						digest = new byte[size];
						Array.Copy(dataBuffer, dataOffset, digest, 0, size);
						dataOffset += size;
						break;

					case FieldType.NAMESPACE:
						ns = ByteUtil.Utf8ToString(dataBuffer, dataOffset, size);
						dataOffset += size;
						break;

					case FieldType.TABLE:
						setName = ByteUtil.Utf8ToString(dataBuffer, dataOffset, size);
						dataOffset += size;
						break;

					case FieldType.KEY:
						int type = dataBuffer[dataOffset++];
						size--;
						userKey = ByteUtil.BytesToKeyValue(type, dataBuffer, dataOffset, size);
						dataOffset += size;
						break;
				} 
			}
			return new Key(ns, digest, setName, userKey);		
		}
		
		protected internal Record ParseRecord()
		{
			Dictionary<string, object> bins = null;

			for (int i = 0 ; i < opCount; i++)
			{
				int opSize = ByteUtil.BytesToInt(dataBuffer, dataOffset);
				byte particleType = dataBuffer[dataOffset + 5];
				byte nameSize = dataBuffer[dataOffset + 7];
				string name = ByteUtil.Utf8ToString(dataBuffer, dataOffset + 8, nameSize);
				dataOffset += 4 + 4 + nameSize;

				int particleBytesSize = (int)(opSize - (4 + nameSize));
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

		protected internal void Stop()
		{
			valid = false;
		}

		protected internal override void OnSuccess()
		{
			parent.ChildSuccess(node);
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			parent.ChildFailure(e);
		}

		protected internal abstract void ParseRow(Key key);
	}
}
