/* 
 * Copyright 2012-2018 Aerospike, Inc.
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

namespace Aerospike.Client
{
	public sealed class QueryValidate
	{
		public static ulong ValidateBegin(Node node, string ns)
		{
			if (!node.HasClusterStable)
			{
				return 0;
			}

			// Fail when cluster is in migration.
			string result = Info.Request(node, "cluster-stable:namespace=" + ns);

			try
			{
				return Convert.ToUInt64(result, 16);
			}
			catch (Exception)
			{
				// Yes, even scans return QUERY_ABORTED.
				throw new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + result);
			}
		}

		public static void Validate(Node node, string ns, ulong expectedKey)
		{
			if (expectedKey == 0 || !node.HasClusterStable)
			{
				return;
			}

			// Fail when cluster is in migration.
			ulong clusterKey = ValidateBegin(node, ns);

			if (clusterKey != expectedKey)
			{
				throw new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + expectedKey + ' ' + clusterKey);
			}
		}
	}
}
