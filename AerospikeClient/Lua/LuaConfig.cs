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
namespace Aerospike.Client
{
	/// <summary>
	/// Lua static configuration variables. These variables apply to all AerospikeClient instances
	/// in a single process.
	/// </summary>
	public sealed class LuaConfig
	{
		/// <summary>
		/// Directory location which contains user defined Lua source files.
		/// </summary>
		public static string PackagePath = "udf" + Path.DirectorySeparatorChar + "?.lua";

		/// <summary>
		/// Maximum number of Lua runtime instances to cache at any point in time.
		/// Each query with an aggregation function requires a Lua instance.
		/// If the number of concurrent queries exceeds the Lua pool size, a new Lua 
		/// instance will still be created, but it will not be returned to the pool. 
		/// </summary>
		public static int InstancePoolSize = 5;
	}
}
