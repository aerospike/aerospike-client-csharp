/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
		internal string packageName;
		internal string functionName;
		internal Value[] functionArgs;
		internal int taskId;
		internal bool returnData;

		/// <summary>
		/// Set query namespace.
		/// </summary>
		public void SetNamespace(string ns)
		{
			this.ns = ns;
		}

		/// <summary>
		/// Set optional query setname.
		/// </summary>
		public void SetSetName(string setName)
		{
			this.setName = setName;
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
		/// Set query bin names.
		/// </summary>
		public void SetBinNames(params string[] binNames)
		{
			this.binNames = binNames;
		}

		/// <summary>
		/// Set optional query filters.
		/// </summary>
		public void SetFilters(params Filter[] filters)
		{
			this.filters = filters;
		}

		/// <summary>
		/// Set optional query task id.
		/// </summary>
		public void TaskId(int taskId)
		{
			this.taskId = taskId;
		}

		/// <summary>
		/// Set Lua aggregation function parameters.  This function will be called on both the server 
		/// and client for each selected item.
		/// </summary>
		/// <param name="packageName">server package where user defined function resides</param>
		/// <param name="functionName">aggregation function name</param>
		/// <param name="functionArgs">arguments to pass to function name, if any</param>
		/// <param name="returnData">whether to return data back to the client or not</param>
		internal void SetAggregateFunction(string packageName, string functionName, Value[] functionArgs, bool returnData)
		{
			this.packageName = packageName;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
			this.returnData = returnData;
		}
	}
}