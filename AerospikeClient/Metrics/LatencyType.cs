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

namespace Aerospike.Client
{
	public static class Latency
	{
		public enum LatencyType
		{
			CONN,
			WRITE,
			READ,
			BATCH,
			QUERY,
			NONE
		}

		public static int GetMax()
		{
			return (int)LatencyType.NONE;
		}

		public static string LatencyTypeToString(LatencyType type)
		{
			return type switch
			{
				LatencyType.CONN => "conn",
				LatencyType.WRITE => "write",
				LatencyType.READ => "read",
				LatencyType.BATCH => "batch",
				LatencyType.QUERY => "query",
				_ => "none",
			};
		}
	}
}
