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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public abstract class AsyncMultiCommand : AsyncCommand
	{
		protected internal readonly AsyncNode serverNode;
		protected internal int info3;
		protected internal int batchIndex;
		protected internal readonly bool isOperation;
		protected internal volatile bool valid = true;

		/// <summary>
		/// Batch constructor.
		/// </summary>
		public AsyncMultiCommand(AsyncCluster cluster, Policy policy, AsyncNode node, bool isOperation)
			: base(cluster, policy)
		{
			this.serverNode = node;
			this.isOperation = isOperation;
		}

		/// <summary>
		/// Scan/Query constructor.
		/// </summary>
		public AsyncMultiCommand(AsyncCluster cluster, Policy policy, AsyncNode node, int socketTimeout, int totalTimeout)
			: base(cluster, policy, socketTimeout, totalTimeout)
		{
			this.serverNode = node;
			this.isOperation = false;
		}

		public AsyncMultiCommand(AsyncMultiCommand other) : base(other)
		{
			this.serverNode = other.serverNode;
			this.isOperation = other.isOperation;
		}

		protected internal sealed override void ParseCommand()
		{
			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}

			if (ParseGroup())
			{
				Finish();
				return;
			}

			// Prepare for next group.
			ReceiveNext();
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

				// If this is the end marker of the response, do not proceed further.
				if ((info3 & Command.INFO3_LAST) != 0)
				{
					if (resultCode != 0)
					{
						// The server returned a fatal error.
						throw new AerospikeException(resultCode);
					}
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

				ParseRow();
			}
			return false;
		}

		protected internal abstract void ParseRow();

		protected internal Record ParseRecord()
		{
			if (opCount <= 0)
			{
				return new Record(null, generation, expiration);
			}

			(Record record, dataOffset) = policy.recordParser.ParseRecord(dataBuffer, dataOffset, opCount, generation, expiration, isOperation);
			return record;
		}

		protected internal void Stop()
		{
			valid = false;
		}
	}
}
