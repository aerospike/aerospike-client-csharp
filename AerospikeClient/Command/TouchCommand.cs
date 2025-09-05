/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

namespace Aerospike.Client
{
	public sealed class TouchCommand : SyncWriteCommand
	{
		private readonly bool failOnNotFound;
		internal bool Touched { get; private set; }
		public TouchCommand(Cluster cluster, WritePolicy writePolicy, Key key)
 			: base(cluster, writePolicy, key)
		{
			this.failOnNotFound = true;
		}

		public TouchCommand(Cluster cluster, WritePolicy writePolicy, Key key, bool failOnNotFound)
 			: base(cluster, writePolicy, key)
		{
			this.failOnNotFound = failOnNotFound;
		}

		protected internal override void WriteBuffer()
		{
			SetTouch(writePolicy, key);
		}

		protected internal override void ParseResult(Node node, Connection conn)
		{
			ParseHeader(node, conn);
			ParseFields(policy.Txn, key, true);

			if (resultCode == ResultCode.OK)
			{
				Touched = true;
				return;
			}

			Touched = false;
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				if (failOnNotFound)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (writePolicy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return;
			}

			throw new AerospikeException(resultCode);
		}
	}
}
