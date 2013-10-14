/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using LuaInterface;

namespace Aerospike.Client
{
	/// <summary>
	/// Lua static configuration variables.
	/// </summary>
	public sealed class LuaConfig
	{
		/// <summary>
		/// Directory location which contains user defined Lua source files.
		/// </summary>
		public static string PackagePath = "udf/?.lua";

		/// <summary>
		/// Maximum number of Lua runtime instances to cache at any point in time.
		/// Each query with an aggregation function requires a Lua instance.
		/// If the number of concurrent queries exceeds the Lua pool size, a new Lua 
		/// instance will still be created, but it will not be returned to the pool. 
		/// </summary>
		public static int InstancePoolSize = 5;
	}
}