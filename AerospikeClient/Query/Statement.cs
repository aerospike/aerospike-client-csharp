/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
		internal Filter[] filters;
		internal Assembly resourceAssembly;
		internal string resourcePath;
		internal string packageName;
		internal string functionName;
		internal Value[] functionArgs;
		internal long taskId;
		internal bool returnData;

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
		/// Optional query filters.
		/// Currently, only one filter is allowed by the server on a secondary index lookup.
		/// If multiple filters are necessary, see QueryFilter example for a workaround.
		/// QueryFilter demonstrates how to add additional filters in an user-defined 
		/// aggregation function. 
		/// </summary>
		public Filter[] Filters
		{
			set { filters = value; }
			get { return filters; }
		}

		/// <summary>
		/// Set optional query filters.
		/// Currently, only one filter is allowed by the server on a secondary index lookup.
		/// If multiple filters are necessary, see QueryFilter example for a workaround.
		/// QueryFilter demonstrates how to add additional filters in an user-defined 
		/// aggregation function. 
		/// </summary>
		public void SetFilters(params Filter[] filters)
		{
			this.filters = filters;
		}

		/// <summary>
		/// Optional query task id.
		/// </summary>
		public long TaskId
		{
			set { taskId = value; }
			get { return taskId; }
		}

		/// <summary>
		/// Set optional query task id.
		/// </summary>
		public void SetTaskId(long taskId)
		{
			this.taskId = taskId;
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
		/// Prepare statement just prior to execution.
		/// </summary>
		internal void Prepare(bool returnData)
		{
			this.returnData = returnData;

			if (taskId == 0)
			{
				taskId = Environment.TickCount;
			}
		}
	}
}
