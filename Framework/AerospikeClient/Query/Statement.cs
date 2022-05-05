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
using System;
using System.Reflection;

namespace Aerospike.Client
{
	/// <summary>
	/// Query statement parameters.
	/// </summary>
	public sealed class Statement
	{
		internal string ns;
		internal string setName;
		internal string indexName;
		internal string[] binNames;
		internal Filter filter;
		internal Assembly resourceAssembly;
		internal string resourcePath;
		internal string packageName;
		internal string packageContents;
		internal string functionName;
		internal Value[] functionArgs;
		internal Operation[] operations;
		internal ulong taskId;
		internal long maxRecords;
		internal int recordsPerSecond;

		/// <summary>
		/// Query namespace.
		/// </summary>
		public string Namespace
		{
			set { ns = value; }
			get { return ns; }
		}

		/// <summary>
		/// Set query namespace.
		/// </summary>
		public void SetNamespace(string ns)
		{
			this.ns = ns;
		}

		/// <summary>
		/// Optional query set name.
		/// </summary>
		public string SetName
		{
			set { setName = value; }
			get { return setName; }
		}

		/// <summary>
		/// Set optional query set name.
		/// </summary>
		public void SetSetName(string setName)
		{
			this.setName = setName;
		}

		/// <summary>
		/// Optional query index name.  If not set, the server
		/// will determine the index from the filter's bin name.
		/// </summary>
		public string IndexName
		{
			set { indexName = value; }
			get { return indexName; }
		}

		/// <summary>
		/// Set optional query index name.  If not set, the server
		/// will determine the index from the filter's bin name.
		/// </summary>
		public void SetIndexName(string indexName)
		{
			this.indexName = indexName;
		}

		/// <summary>
		/// Query bin names.
		/// </summary>
		public string[] BinNames
		{
			set { SetBinNames(value); }
			get { return binNames; }
		}

		/// <summary>
		/// Set query bin names.
		/// </summary>
		public void SetBinNames(params string[] binNames)
		{
			foreach (string binName in binNames)
			{
				if (binName.Length > 14)
				{
					throw new AerospikeException(ResultCode.BIN_NAME_TOO_LONG, "Bin name length greater than 14 characters");
				}
			}
			this.binNames = binNames;
		}

		/// <summary>
		/// Optional query filter.  This filter is applied to the secondary index on query.
		/// Query index filters must reference a bin which has a secondary index defined.
		/// </summary>
		public Filter Filter
		{
			set { SetFilter(value); }
			get { return filter; }
		}

		/// <summary>
		/// Set optional query index filter.  This filter is applied to the secondary index on query.
		/// Query index filters must reference a bin which has a secondary index defined.
		/// </summary>
		public void SetFilter(Filter filter)
		{
			this.filter = filter;
		}

		/// <summary>
		/// Optional task id.
		/// </summary>
		public ulong TaskId
		{
			set { taskId = value; }
			get { return taskId; }
		}

		/// <summary>
		/// Set optional task id.
		/// </summary>
		public void SetTaskId(long taskId)
		{
			this.taskId = (ulong)taskId;
		}

		/// <summary>
		/// Maximum number of records returned (for foreground query) or processed
		/// (for background execute query). This number is divided by the number of nodes
		/// involved in the query. The actual number of records returned may be less than
		/// maxRecords if node record counts are small and unbalanced across nodes.
		/// <para>
		/// Default: 0 (do not limit record count)
		/// </para>
		/// </summary>
		public long MaxRecords
		{
			set { maxRecords = value; }
			get { return maxRecords; }
		}

		/// <summary>
		/// Limit returned records per second (rps) rate for each server.
		/// Do not apply rps limit if recordsPerSecond is zero (default).
		/// Currently only applicable to a query without a defined filter.
		/// </summary>
		public int RecordsPerSecond
		{
			set { recordsPerSecond = value; }
			get { return recordsPerSecond; }
		}

		/// <summary>
		/// Set returned records per second (rps) rate for each server.
		/// </summary>
		public void SetRecordsPerSecond(int recordsPerSecond)
		{
			this.recordsPerSecond = recordsPerSecond;
		}

		/// <summary>
		/// Set Lua aggregation function parameters for a Lua package located on the filesystem.  
		/// This function will be called on both the server and client for each selected item.
		/// </summary>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		public void SetAggregateFunction(string packageName, string functionName, params Value[] functionArgs)
		{
			this.packageName = packageName;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
		}

		/// <summary>
		/// Set Lua aggregation function parameters for a Lua package located in an assembly resource.  
		/// This function will be called on both the server and client for each selected item.
		/// </summary>
		/// <param name="resourceAssembly">assembly where resource is located.  Current assembly can be obtained by: Assembly.GetExecutingAssembly()"</param>
		/// <param name="resourcePath">namespace path where Lua resource is located.  Example: Aerospike.Client.Resources.mypackage.lua</param>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		public void SetAggregateFunction(Assembly resourceAssembly, string resourcePath, string packageName, string functionName, params Value[] functionArgs)
		{
			this.resourceAssembly = resourceAssembly;
			this.resourcePath = resourcePath;
			this.packageName = packageName;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
		}

		/// <summary>
		/// Set Lua aggregation function parameters for a Lua package located in a string with lua code.  
		/// This function will be called on both the server and client for each selected item.
		/// </summary>
		/// <param name="packageName">package name for package that contains aggregation function</param>
		/// <param name="packageContents">lua code associated with aggregation function.</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		public void SetAggregateFunction(string packageName, string packageContents, string functionName, params Value[] functionArgs)
		{
			this.packageName = packageName;
			this.packageContents = packageContents;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
		}

		/// <summary>
		/// Assembly where resource is located.  Current assembly can be obtained by: Assembly.GetExecutingAssembly().
		/// Used by aggregate queries only.
		/// </summary>
		public Assembly ResourceAssembly
		{
			set { resourceAssembly = value; }
			get { return resourceAssembly; }
		}

		/// <summary>
		/// Namespace path where Lua resource is located.  Example: Aerospike.Client.Resources.mypackage.lua
		/// Used by aggregate queries only.
		/// </summary>
		public string ResourcePath
		{
			set { resourcePath = value; }
			get { return resourcePath; }
		}

		/// <summary>
		/// Server package where user defined function resides.
		/// Used by aggregate queries only.
		/// </summary>
		public string PackageName
		{
			set { packageName = value; }
			get { return packageName; }
		}

		/// <summary>
		/// String containing lua code that comprises a package.
		/// Used by aggregate queries only when aggregation function is defined in a string.
		/// </summary>
		public string PackageContents
		{
			set { packageContents = value; }
			get { return packageContents; }
		}

		/// <summary>
		/// Aggregation function name.
		/// Used by aggregate queries only.
		/// </summary>
		public string FunctionName
		{
			set { functionName = value; }
			get { return functionName; }
		}

		/// <summary>
		/// Arguments to pass to function name, if any.
		/// Used by aggregate queries only.
		/// </summary>
		public Value[] FunctionArgs
		{
			set { functionArgs = value; }
			get { return functionArgs; }
		}

		/// <summary>
		/// Operations to be performed on query/execute.
		/// </summary>
		public Operation[] Operations
		{
			set { operations = value; }
			get { return operations; }
		}

		/// <summary>
		/// Return taskId if set by user. Otherwise return a new taskId.
		/// </summary>
		internal ulong PrepareTaskId()
		{
			return (taskId != 0) ? taskId : RandomShift.ThreadLocalInstance.NextLong();
		}
	}
}
