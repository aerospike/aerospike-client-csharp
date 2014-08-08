/* 
 * Copyright 2012-2014 Aerospike, Inc.
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

namespace Aerospike.Client
{
	public sealed class ExistsCommand : SingleCommand
	{
		private readonly Policy policy;
		private bool exists;

		public ExistsCommand(Cluster cluster, Policy policy, Key key)
			: base(cluster, key)
		{
			this.policy = (policy == null) ? new Policy() : policy;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetExists(key);
		}

		protected internal override void ParseResult(Connection conn)
		{
			// Read header.
			conn.ReadFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);

			int resultCode = dataBuffer[13];

			if (resultCode != 0 && resultCode != ResultCode.KEY_NOT_FOUND_ERROR)
			{
				throw new AerospikeException(resultCode);
			}
			exists = resultCode == 0;
			EmptySocket(conn);
		}

		public bool Exists()
		{
			return exists;
		}
	}
}
