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
		internal ReduceSpec<Record, Record>[] reduceSpecs;
		internal ReduceSpec<Record, Record> resolvedReduce;
		internal bool reduceResolved;
		internal string orderByBin;
		internal BinDataType orderByType;
		internal Order orderByOrder;
		internal OrderByFlags orderByFlags;
		internal bool orderBySet;
		internal TopKSpec topK;

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
		/// will determine the index from the filter's bin name
		/// and data type of the operand.
		/// </summary>
		public string IndexName
		{
			set { indexName = value; }
			get { return indexName; }
		}

		/// <summary>
		/// Set optional query index name.  If not set, the server
		/// will determine the index from the filter's bin name
		/// and data type of the operand.
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
		/// Set query bin names for ops projection in queries.
		/// Mutually exclusive with <see cref="Operations"/>.
		/// </summary>
		public void SetBinNames(params string[] binNames)
		{
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
		/// 
		/// RecordsPerSecond is supported in all primary and secondary index
		/// queries in server versions 6.0+. For background queries, RecordsPerSecond
		/// is bounded by the server config background-query-max-rps.
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
		/// <para>
		/// For foreground queries (<see cref="IAerospikeClient.Query(QueryPolicy, Statement)"/>), only read operations
		/// are allowed. Read operations act as ops projections, limiting which bins are returned.
		/// </para>
		/// <para>
		/// Basic read operations (<see cref="Operation.Get(string)"/>, <see cref="Operation.Get()"/>,
		/// <see cref="Operation.GetHeader()"/>) are supported on server versions prior to 8.1.2.
		/// Extended read operations (e.g., <see cref="ExpOperation.Read(string, Expression, ExpReadFlags)"/>,
		/// CDT read operations, bit read operations, HLL read operations) require server version 8.1.2+.
		/// </para>
		/// <para>
		/// For background execute (<see cref="IAerospikeClient.Execute(WritePolicy, Statement, Operation[])"/>), only write operations
		/// are allowed (e.g., <see cref="ExpOperation.Write"/>).
		/// </para>
		/// <para>
		/// Operations and <see cref="SetBinNames"/> are mutually exclusive. If both are set,
		/// the client will log a warning and ignore operations for foreground queries or ignore
		/// bin names for background execute. Setting both will become an error in a future release.
		/// </para>
		/// </summary>
		public Operation[] Operations
		{
			set { operations = value; }
			get { return operations; }
		}

		/// <summary>
		/// Set reduce spec(s) for this query. Accepts:
		/// <list type="bullet">
		/// <item>zero args - clear any reduce (stream all matching records; default behavior)</item>
		/// <item>one Top-K spec - e.g. <c>SetReduce(Reduce.TopK("d", BinDataType.DOUBLE, Order.ASC, OrderByFlags.NONE, 10))</c></item>
		/// <item>exactly one <see cref="Reduce.OrderBy"/> + one <see cref="Reduce.Limit"/> on the same bin -
		/// split Top-K, equivalent to <see cref="Reduce.TopK"/></item>
		/// </list>
		/// Mutually exclusive with <see cref="SetAggregateFunction(string, string, Value[])"/>. Replaces any
		/// previously set reduce (this setter does not accumulate across calls, consistent with
		/// <see cref="Operations"/> and <see cref="SetBinNames"/>).
		/// <para>
		/// Not public for now - <see cref="SetOrderBy(string, BinDataType, Order)"/> + <see cref="SetTopK"/>
		/// are the only supported public entry points into the reduce framework while the API is still settling.
		/// </para>
		/// </summary>
		internal void SetReduce(params ReduceSpec<Record, Record>[] reduceSpecs)
		{
			this.reduceSpecs = reduceSpecs;
			this.resolvedReduce = null;
			this.reduceResolved = false;
			this.topK = null;
		}

		/// <summary>
		/// Return reduce spec(s) set by <see cref="SetReduce"/>.
		/// </summary>
		internal ReduceSpec<Record, Record>[] GetReduce()
		{
			return reduceSpecs;
		}

		/// <summary>
		/// Sort order building block for <see cref="SetTopK(int)"/>. Equivalent to
		/// <c>SetOrderBy(binName, type, order, OrderByFlags.NONE)</c>.
		/// <para>
		/// Sugar: remembers the sort order for a subsequent <see cref="SetTopK(int)"/> call, which
		/// together resolve internally to <c>SetReduce(Reduce.TopK(binName, type, order, flags, k))</c>.
		/// </para>
		/// </summary>
		/// <param name="binName">bin name to order by</param>
		/// <param name="type">scalar type of <paramref name="binName"/></param>
		/// <param name="order">sort direction</param>
		public void SetOrderBy(string binName, BinDataType type, Order order)
		{
			SetOrderBy(binName, type, order, OrderByFlags.NONE);
		}

		/// <summary>
		/// Sort order building block for <see cref="SetTopK(int)"/>.
		/// <para>
		/// Sugar: remembers the sort order for a subsequent <see cref="SetTopK(int)"/> call, which
		/// together resolve internally to <c>SetReduce(Reduce.TopK(binName, type, order, flags, k))</c>.
		/// </para>
		/// </summary>
		/// <param name="binName">bin name to order by</param>
		/// <param name="type">scalar type of <paramref name="binName"/></param>
		/// <param name="order">sort direction</param>
		/// <param name="flags">
		/// comparison options (<see cref="OrderByFlags.CASE_INSENSITIVE"/> for
		/// <see cref="BinDataType.STRING"/> only)
		/// </param>
		public void SetOrderBy(string binName, BinDataType type, Order order, OrderByFlags flags)
		{
			this.orderByBin = binName;
			this.orderByType = type;
			this.orderByOrder = order;
			this.orderByFlags = flags;
			this.orderBySet = true;
			// Order-by sugar changed: clear any previously resolved Top-K until SetTopK() is called again.
			this.topK = null;
			this.reduceSpecs = null;
			this.resolvedReduce = null;
			this.reduceResolved = false;
		}

		/// <summary>
		/// Ordered LIMIT k query; must be preceded by a <see cref="SetOrderBy(string, BinDataType, Order)"/> call
		/// on this statement.
		/// <para>
		/// Sugar for <c>SetReduce(Reduce.TopK(binName, type, order, flags, k))</c> using the bin,
		/// type, order, and flags from the preceding <see cref="SetOrderBy(string, BinDataType, Order)"/> call.
		/// Like <see cref="SetReduce"/>, replaces any previously set reduce.
		/// Supported nodes return bounded candidates; mixed clusters fall back to client-side reduction.
		/// </para>
		/// </summary>
		/// <param name="k">maximum number of records to return, in [1, 1000]</param>
		/// <exception cref="InvalidOperationException">if <see cref="SetOrderBy(string, BinDataType, Order)"/> was not called first</exception>
		public void SetTopK(int k)
		{
			if (!orderBySet)
			{
				throw new InvalidOperationException("SetTopK() requires SetOrderBy() to be called first");
			}
			SetReduce(Reduce.TopK(orderByBin, orderByType, orderByOrder, orderByFlags, k));
			topK = new TopKSpec(orderByBin, orderByType, orderByOrder, orderByFlags, k);
		}

		/// <summary>
		/// Return whether this statement has a Top-K specification.
		/// </summary>
		public bool HasTopK => topK != null;

		/// <summary>
		/// Return the Top-K order-by bin name, or null when Top-K is not set.
		/// </summary>
		public string TopKBin => topK?.bin;

		/// <summary>
		/// Return the Top-K order-by type when Top-K is set.
		/// </summary>
		public BinDataType TopKType => topK != null ? topK.type : default;

		/// <summary>
		/// Return the Top-K order direction when Top-K is set.
		/// </summary>
		public Order TopKOrder => topK != null ? topK.order : default;

		/// <summary>
		/// Return the Top-K order-by flags when Top-K is set.
		/// </summary>
		public OrderByFlags TopKFlags => topK != null ? topK.flags : default;

		/// <summary>
		/// Return the Top-K limit, or zero when Top-K is not set.
		/// </summary>
		public int TopKLimit => topK == null ? 0 : topK.limit;

		/// <summary>
		/// Validate the Top-K specification.
		/// </summary>
		public void ValidateTopK()
		{
			if (topK == null)
			{
				return;
			}

			if (topK.bin == null)
			{
				throw new ArgumentException("Top-K order-by specification is incomplete");
			}

			int length = ByteUtil.EstimateSizeUtf8(topK.bin);

			if (length == 0 || length > 15 || topK.bin.IndexOf('\0') >= 0)
			{
				throw new ArgumentException("Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL");
			}

			if (topK.flags != OrderByFlags.NONE && topK.type != BinDataType.STRING)
			{
				throw new ArgumentException("Top-K order-by flags are only valid for STRING");
			}

			if (topK.limit < 1 || topK.limit > 1000)
			{
				throw new ArgumentException("Top-K limit must be in [1, 1000]");
			}

			if (maxRecords != 0)
			{
				throw new ArgumentException("Top-K is incompatible with maxRecords");
			}

			if (functionName != null)
			{
				throw new ArgumentException("Top-K is only valid for foreground queries");
			}

			if (operations != null)
			{
				foreach (Operation operation in operations)
				{
					if (topK.bin.Equals(operation.binName))
					{
						return;
					}
				}
				throw new ArgumentException("Top-K order-by bin must be included in the operations projection");
			}

			if (binNames != null)
			{
				foreach (string binName in binNames)
				{
					if (topK.bin.Equals(binName))
					{
						return;
					}
				}
				throw new ArgumentException("Top-K order-by bin must be included in the bin projection");
			}
		}

		/// <summary>
		/// Resolve the reduce spec(s) set by <see cref="SetReduce"/> into a single combiner usable by a
		/// query executor. Returns null if no reduce was set. Composes a split
		/// <see cref="Reduce.OrderBy"/> + <see cref="Reduce.Limit"/> pair into a single Top-K combiner.
		/// </summary>
		/// <exception cref="ArgumentException">
		/// if the reduce specs are not a single reducer or a valid orderBy/limit pair on the same bin
		/// </exception>
		internal ReduceSpec<Record, Record> ResolveReduce()
		{
			if (!reduceResolved)
			{
				resolvedReduce = ComputeResolveReduce();
				reduceResolved = true;
			}
			return resolvedReduce;
		}

		private ReduceSpec<Record, Record> ComputeResolveReduce()
		{
			if (reduceSpecs == null || reduceSpecs.Length == 0)
			{
				return null;
			}

			bool isSplit = reduceSpecs[0] is OrderByReduceSpec || reduceSpecs[0] is LimitReduceSpec;

			if (reduceSpecs.Length == 1 && !isSplit)
			{
				return reduceSpecs[0];
			}

			ReduceSpec<Record, Record> orderBy = null;
			ReduceSpec<Record, Record> limit = null;

			foreach (ReduceSpec<Record, Record> spec in reduceSpecs)
			{
				if (spec is OrderByReduceSpec)
				{
					if (orderBy != null)
					{
						throw new ArgumentException("topK requires exactly one orderBy spec");
					}
					orderBy = spec;
				}
				else if (spec is LimitReduceSpec)
				{
					if (limit != null)
					{
						throw new ArgumentException("topK requires exactly one limit spec");
					}
					limit = spec;
				}
				else
				{
					throw new ArgumentException("Cannot mix topK parts (orderBy/limit) with other reducers");
				}
			}

			if (orderBy == null || limit == null)
			{
				throw new ArgumentException("topK requires both an orderBy spec and a limit spec");
			}
			return TopKReduceSpec.Compose(orderBy, limit);
		}

		/// <summary>
		/// Return taskId if set by user. Otherwise return a new taskId.
		/// </summary>
		internal ulong PrepareTaskId()
		{
			return (taskId != 0) ? taskId : RandomShift.ThreadLocalInstance.NextLong();
		}

		internal sealed class TopKSpec
		{
			internal readonly string bin;
			internal readonly BinDataType type;
			internal readonly Order order;
			internal readonly OrderByFlags flags;
			internal readonly int limit;

			internal TopKSpec(string bin, BinDataType type, Order order, OrderByFlags flags, int limit)
			{
				this.bin = bin;
				this.type = type;
				this.order = order;
				this.flags = flags;
				this.limit = limit;
			}
		}
	}
}
