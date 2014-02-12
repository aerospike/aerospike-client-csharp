/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
