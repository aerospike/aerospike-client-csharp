/*
 * Copyright 2012-2026 Aerospike, Inc.
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
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Client state the discovery tests assert on that the client does not expose.
	/// </summary>
	internal static class ClusterInternals
	{
		/// <summary>
		/// Return the seed list the client holds for re-seeding.
		/// </summary>
		internal static Host[] Seeds(AerospikeClient client)
		{
			FieldInfo field = typeof(Cluster).GetField("seeds", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

			if (field == null)
			{
				throw new InvalidOperationException("Cluster.seeds is not readable");
			}
			return (Host[])field.GetValue(client.Cluster);
		}
	}
}
