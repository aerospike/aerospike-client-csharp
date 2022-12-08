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
namespace Aerospike.Client
{
	public sealed class ServerCommand : MultiCommand
	{
		private readonly Statement statement;

		public ServerCommand(Cluster cluster, Node node, WritePolicy policy, Statement statement) 
			: base(cluster, policy, node, LatencyType.NONE, true)
		{
			this.statement = statement;
		}

		protected internal override bool IsWrite()
		{
			return true;
		}
		
		protected internal override void WriteBuffer()
		{
			SetQuery(policy, statement, true, null);
		}

		protected internal override void ParseRow(Key key)
		{
			// Server commands (Query/Execute UDF) should only send back a return code.
			if (resultCode != 0)
			{
				// Background scans (with null query filter) return KEY_NOT_FOUND_ERROR
				// when the set does not exist on the target node.
				if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					// Non-fatal error.
					return;
				}
				throw new AerospikeException(resultCode);
			}

			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Unexpectedly received bins on background query!");
			}

			if (!valid)
			{
				throw new AerospikeException.QueryTerminated();
			}
		}		
	}
}
