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
using System.Net.Sockets;

namespace Aerospike.Client
{
	public abstract class AsyncMultiCommand : AsyncCommand
	{
		protected internal readonly AsyncExecutor executor;
		protected internal readonly AsyncNode serverNode;
		protected internal int info3;
		protected internal int resultCode;
		protected internal int generation;
		protected internal int expiration;
		protected internal int batchIndex;
		protected internal int fieldCount;
		protected internal int opCount;
		private readonly bool stopOnNotFound;
		protected internal volatile bool valid = true;

		/// <summary>
		/// Batch constructor.
		/// </summary>
		public AsyncMultiCommand(AsyncExecutor executor, AsyncCluster cluster, Policy policy, AsyncNode node)
			: base(cluster, policy)
		{
			this.executor = executor;
			this.serverNode = node;
			this.stopOnNotFound = false;
		}

		/// <summary>
		/// Scan/Query constructor.
		/// </summary>
		public AsyncMultiCommand(AsyncExecutor executor, AsyncCluster cluster, Policy policy, AsyncNode node, int socketTimeout, int totalTimeout)
			: base(cluster, policy, socketTimeout, totalTimeout)
		{
			this.executor = executor;
			this.serverNode = node;
			this.stopOnNotFound = true;
		}

		public AsyncMultiCommand(AsyncMultiCommand other) : base(other)
		{
			this.executor = other.executor;
			this.serverNode = other.serverNode;
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

		protected internal override Node GetNode(Cluster cluster)
		{
			return serverNode;
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			return true;
		}

		private bool ParseGroup()
		{
			// Parse each message response and add it to the result array
			while (dataOffset < dataLength)
			{
				dataOffset += 3;
				info3 = dataBuffer[dataOffset];
				dataOffset += 2;
				resultCode = dataBuffer[dataOffset];

				if (resultCode != 0)
				{
					if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR || resultCode == ResultCode.FILTERED_OUT)
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
				if ((info3 & Command.INFO3_LAST) != 0)
				{
					return true;
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

				if (!valid)
				{
					throw new AerospikeException.QueryTerminated();
				}

				Key key = ParseKey(fieldCount);
				ParseRow(key);
			}
			return false;
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
			executor.ChildSuccess(node);
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			executor.ChildFailure(e);
		}

		protected internal abstract void ParseRow(Key key);
	}
}
