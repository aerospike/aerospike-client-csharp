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
