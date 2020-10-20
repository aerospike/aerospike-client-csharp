/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// List bin operations. Create list operations used by client operate command.
	/// <para>
	/// List operations support negative indexing.  If the index is negative, the
	/// resolved index starts backwards from end of list. If an index is out of bounds,
	/// a parameter error will be returned. If a range is partially out of bounds, the
	/// valid part of the range will be returned. Index/Range examples:
	/// <ul>
	/// <li>Index 0: First item in list.</li>
	/// <li>Index 4: Fifth item in list.</li>
	/// <li>Index -1: Last item in list.</li>
	/// <li>Index -3: Third to last item in list.</li>
	/// <li>Index 1 Count 2: Second and third items in list.</li>
	/// <li>Index -3 Count 3: Last three items in list.</li>
	/// <li>Index -5 Count 4: Range between fifth to last item to second to last item inclusive.</li>
	/// </ul>
	/// </para>
	/// <para>
	/// Nested CDT operations are supported by optional CTX context arguments.  Examples:
	/// <ul>
	/// <li>bin = [[7,9,5],[1,2,3],[6,5,4,1]]</li>
	/// <li>Append 11 to last list.</li>
	/// <li>ListOperation.append("bin", Value.Get(11), CTX.listIndex(-1))</li>
	/// <li>bin result = [[7,9,5],[1,2,3],[6,5,4,1,11]]</li>
	/// <li></li>
	/// <li>bin = {key1=[[7,9,5],[13]], key2=[[9],[2,4],[6,1,9]], key3=[[6,5]]}</li>
	/// <li>Append 11 to lowest ranked list in map identified by "key2".</li>
	/// <li>ListOperation.append("bin", Value.Get(11), CTX.mapKey(Value.Get("key2")), CTX.listRank(0))</li>
	/// <li>bin result = {key1=[[7,9,5],[13]], key2=[[9],[2,4,11],[6,1,9]], key3=[[6,5]]}</li>
	/// </ul>
	/// </para>
	/// </summary>
	public class ListOperation
	{
		internal const int SET_TYPE = 0;
		internal const int APPEND = 1;
		internal const int APPEND_ITEMS = 2;
		internal const int INSERT = 3;
		internal const int INSERT_ITEMS = 4;
		internal const int POP = 5;
		internal const int POP_RANGE = 6;
		internal const int REMOVE = 7;
		internal const int REMOVE_RANGE = 8;
		internal const int SET = 9;
		internal const int TRIM = 10;
		internal const int CLEAR = 11;
		internal const int INCREMENT = 12;
		internal const int SORT = 13;
		internal const int SIZE = 16;
		internal const int GET = 17;
		internal const int GET_RANGE = 18;
		internal const int GET_BY_INDEX = 19;
		internal const int GET_BY_RANK = 21;
		internal const int GET_BY_VALUE = 22; // GET_ALL_BY_VALUE on server.
		internal const int GET_BY_VALUE_LIST = 23;
		internal const int GET_BY_INDEX_RANGE = 24;
		internal const int GET_BY_VALUE_INTERVAL = 25;
		internal const int GET_BY_RANK_RANGE = 26;
		internal const int GET_BY_VALUE_REL_RANK_RANGE = 27;
		internal const int REMOVE_BY_INDEX = 32;
		internal const int REMOVE_BY_RANK = 34;
		internal const int REMOVE_BY_VALUE = 35;
		internal const int REMOVE_BY_VALUE_LIST = 36;
		internal const int REMOVE_BY_INDEX_RANGE = 37;
		internal const int REMOVE_BY_VALUE_INTERVAL = 38;
		internal const int REMOVE_BY_RANK_RANGE = 39;
		internal const int REMOVE_BY_VALUE_REL_RANK_RANGE = 40;

		/// <summary>
		/// Create list create operation.
		/// Server creates list at given context level. The context is allowed to be beyond list
		/// boundaries only if pad is set to true.  In that case, nil list entries will be inserted to
		/// satisfy the context position.
		/// </summary>
		public static Operation Create(string binName, ListOrder order, bool pad, params CTX[] ctx)
		{
			// If context not defined, the set order for top-level bin list.
			if (ctx == null || ctx.Length == 0)
			{
				return SetOrder(binName, order);
			}

			Packer packer = new Packer();
			CDT.Init(packer, ctx, SET_TYPE, 1, CTX.GetFlag(order, pad));
			packer.PackNumber((int)order);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create set list order operation.
		/// Server sets list order.  Server returns null.
		/// </summary>
		public static Operation SetOrder(string binName, ListOrder order, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.SET_TYPE, (int)order, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create default list append operation.
		/// Server appends value to end of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Append(string binName, Value value, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.APPEND, value, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list append operation with policy.
		/// Server appends value to list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Append(ListPolicy policy, string binName, Value value, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.APPEND, value, policy.attributes, policy.flags, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create default list append items operation.
		/// Server appends each input list item to end of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation AppendItems(string binName, IList list, params CTX[] ctx)
		{
			// Compiler bug prevents calling of this method.
			// byte[] bytes = PackUtil.Pack(ListOperation.APPEND_ITEMS, list, ctx);
			// Duplicate method instead.
			Packer packer = new Packer();
			PackUtil.Init(packer, ctx);
			packer.PackArrayBegin(2);
			packer.PackNumber(ListOperation.APPEND_ITEMS);
			packer.PackList(list);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list append items operation with policy.
		/// Server appends each input list item to list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation AppendItems(ListPolicy policy, string binName, IList list, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.APPEND_ITEMS, list, policy.attributes, policy.flags, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create default list insert operation.
		/// Server inserts value to specified index of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Insert(string binName, int index, Value value, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INSERT, index, value, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list insert operation with policy.
		/// Server inserts value to specified index of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Insert(ListPolicy policy, string binName, int index, Value value, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INSERT, index, value, policy.flags, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create default list insert items operation.
		/// Server inserts each input list item starting at specified index of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation InsertItems(string binName, int index, IList list, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INSERT_ITEMS, index, list, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list insert items operation with policy.
		/// Server inserts each input list item starting at specified index of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation InsertItems(ListPolicy policy, string binName, int index, IList list, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INSERT_ITEMS, index, list, policy.flags, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create default list increment operation.
		/// Server increments list[index] by 1.
		/// Server returns list[index] after incrementing.
		/// </summary>
		public static Operation Increment(string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INCREMENT, index, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list increment operation with policy.
		/// Server increments list[index] by 1.
		/// Server returns list[index] after incrementing.
		/// </summary>
		public static Operation Increment(ListPolicy policy, string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INCREMENT, index, Value.Get(1), policy.attributes, policy.flags, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create default list increment operation.
		/// Server increments list[index] by value.
		/// Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
		/// Server returns list[index] after incrementing.
		/// </summary>
		public static Operation Increment(string binName, int index, Value value, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INCREMENT, index, value, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list increment operation.
		/// Server increments list[index] by value.
		/// Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
		/// Server returns list[index] after incrementing.
		/// </summary>
		public static Operation Increment(ListPolicy policy, string binName, int index, Value value, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INCREMENT, index, value, policy.attributes, policy.flags, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list pop operation.
		/// Server returns item at specified index and removes item from list bin.
		/// </summary>
		public static Operation Pop(string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.POP, index, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list pop range operation.
		/// Server returns "count" items starting at specified index and removes items from list bin.
		/// </summary>
		public static Operation PopRange(string binName, int index, int count, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.POP_RANGE, index, count, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list pop range operation.
		/// Server returns items starting at specified index to the end of list and removes those items
		/// from list bin.
		/// </summary>
		public static Operation PopRange(string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.POP_RANGE, index, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes item at specified index from list bin.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation Remove(string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE, index, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove range operation.
		/// Server removes "count" items starting at specified index from list bin.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation RemoveRange(string binName, int index, int count, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_RANGE, index, count, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove range operation.
		/// Server removes items starting at specified index to the end of list.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation RemoveRange(string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_RANGE, index, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list set operation.
		/// Server sets item value at specified index in list bin.
		/// Server does not return a result by default.
		/// </summary>
		public static Operation Set(string binName, int index, Value value, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.SET, index, value, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list set operation with policy.
		/// Server sets item value at specified index in list bin.
		/// Server does not return a result by default.
		/// </summary>
		public static Operation Set(ListPolicy policy, string binName, int index, Value value, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.SET, index, value, policy.flags, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list trim operation.
		/// Server removes items in list bin that do not fall into range specified by index
		/// and count range.  If the range is out of bounds, then all items will be removed.
		/// Server returns list size after trim.
		/// </summary>
		public static Operation Trim(string binName, int index, int count, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.TRIM, index, count, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list clear operation.
		/// Server removes all items in list bin.
		/// Server does not return a result by default.
		/// </summary>
		public static Operation Clear(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.CLEAR, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list sort operation.
		/// Server sorts list according to sortFlags.
		/// Server does not return a result by default.
		/// </summary>
		/// <param name="binName">server bin name</param>
		/// <param name="sortFlags">sort flags</param>
		/// <param name="ctx">optional context path for nested CDT</param>		
		public static Operation Sort(string binName, ListSortFlags sortFlags, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.SORT, (int)sortFlags, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items identified by value and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByValue(string binName, Value value, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_VALUE, (int)returnType, value, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items identified by values and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByValueList(string binName, IList values, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_VALUE_LIST, (int)returnType, values, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
		/// If valueBegin is null, the range is less than valueEnd.
		/// If valueEnd is null, the range is greater than equal to valueBegin.
		/// <para>
		/// Server returns removed data specified by returnType.
		/// </para>
		/// </summary>
		public static Operation RemoveByValueRange(string binName, Value valueBegin, Value valueEnd, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = CDT.PackRangeOperation(ListOperation.REMOVE_BY_VALUE_INTERVAL, (int)returnType, valueBegin, valueEnd, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove by value relative to rank range operation.
		/// Server removes list items nearest to value and greater by relative rank.
		/// Server returns removed data specified by returnType.
		/// <para>
		/// Examples for ordered list [0,4,5,9,11,15]:
		/// <ul>
		/// <li>(value,rank) = [removed items]</li>
		/// <li>(5,0) = [5,9,11,15]</li>
		/// <li>(5,1) = [9,11,15]</li>
		/// <li>(5,-1) = [4,5,9,11,15]</li>
		/// <li>(3,0) = [4,5,9,11,15]</li>
		/// <li>(3,3) = [11,15]</li>
		/// <li>(3,-3) = [0,4,5,9,11,15]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Operation RemoveByValueRelativeRankRange(string binName, Value value, int rank, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove by value relative to rank range operation.
		/// Server removes list items nearest to value and greater by relative rank with a count limit.
		/// Server returns removed data specified by returnType.
		/// <para>
		/// Examples for ordered list [0,4,5,9,11,15]:
		/// <ul>
		/// <li>(value,rank,count) = [removed items]</li>
		/// <li>(5,0,2) = [5,9]</li>
		/// <li>(5,1,1) = [9]</li>
		/// <li>(5,-1,2) = [4,5]</li>
		/// <li>(3,0,1) = [4]</li>
		/// <li>(3,3,7) = [11,15]</li>
		/// <li>(3,-3,2) = []</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Operation RemoveByValueRelativeRankRange(string binName, Value value, int rank, int count, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, count, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list item identified by index and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByIndex(string binName, int index, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_INDEX, (int)returnType, index, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items starting at specified index to the end of list and returns removed
		/// data specified by returnType.
		/// </summary>
		public static Operation RemoveByIndexRange(string binName, int index, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_INDEX_RANGE, (int)returnType, index, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes "count" list items starting at specified index and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByIndexRange(string binName, int index, int count, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_INDEX_RANGE, (int)returnType, index, count, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list item identified by rank and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByRank(string binName, int rank, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_RANK, (int)returnType, rank, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items starting at specified rank to the last ranked item and returns removed
		/// data specified by returnType.
		/// </summary>
		public static Operation RemoveByRankRange(string binName, int rank, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_RANK_RANGE, (int)returnType, rank, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes "count" list items starting at specified rank and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByRankRange(string binName, int rank, int count, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_RANK_RANGE, (int)returnType, rank, count, ctx);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list size operation.
		/// Server returns size of list.
		/// </summary>
		public static Operation Size(string binName, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.SIZE, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get operation.
		/// Server returns item at specified index in list bin.
		/// </summary>
		public static Operation Get(string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET, index, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get range operation.
		/// Server returns "count" items starting at specified index in list bin.
		/// </summary>
		public static Operation GetRange(string binName, int index, int count, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_RANGE, index, count, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get range operation.
		/// Server returns items starting at index to the end of list.
		/// </summary>
		public static Operation GetRange(string binName, int index, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_RANGE, index, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by value operation.
		/// Server selects list items identified by value and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByValue(string binName, Value value, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_VALUE, (int)returnType, value, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by value range operation.
		/// Server selects list items identified by value range (valueBegin inclusive, valueEnd exclusive)
		/// If valueBegin is null, the range is less than valueEnd.
		/// If valueEnd is null, the range is greater than equal to valueBegin.
		/// <para>
		/// Server returns selected data specified by returnType.
		/// </para>
		/// </summary>
		public static Operation GetByValueRange(string binName, Value valueBegin, Value valueEnd, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = CDT.PackRangeOperation(ListOperation.GET_BY_VALUE_INTERVAL, (int)returnType, valueBegin, valueEnd, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by value list operation.
		/// Server selects list items identified by values and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByValueList(string binName, IList values, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_VALUE_LIST, (int)returnType, values, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by value relative to rank range operation.
		/// Server selects list items nearest to value and greater by relative rank.
		/// Server returns selected data specified by returnType.
		/// <para>
		/// Examples for ordered list [0,4,5,9,11,15]:
		/// <ul>
		/// <li>(value,rank) = [selected items]</li>
		/// <li>(5,0) = [5,9,11,15]</li>
		/// <li>(5,1) = [9,11,15]</li>
		/// <li>(5,-1) = [4,5,9,11,15]</li>
		/// <li>(3,0) = [4,5,9,11,15]</li>
		/// <li>(3,3) = [11,15]</li>
		/// <li>(3,-3) = [0,4,5,9,11,15]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Operation GetByValueRelativeRankRange(string binName, Value value, int rank, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by value relative to rank range operation.
		/// Server selects list items nearest to value and greater by relative rank with a count limit.
		/// Server returns selected data specified by returnType.
		/// <para>
		/// Examples for ordered list [0,4,5,9,11,15]:
		/// <ul>
		/// <li>(value,rank,count) = [selected items]</li>
		/// <li>(5,0,2) = [5,9]</li>
		/// <li>(5,1,1) = [9]</li>
		/// <li>(5,-1,2) = [4,5]</li>
		/// <li>(3,0,1) = [4]</li>
		/// <li>(3,3,7) = [11,15]</li>
		/// <li>(3,-3,2) = []</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Operation GetByValueRelativeRankRange(string binName, Value value, int rank, int count, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, count, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by index operation.
		/// Server selects list item identified by index and returns selected data specified by returnType
		///.
		/// </summary>
		public static Operation GetByIndex(string binName, int index, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_INDEX, (int)returnType, index, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by index range operation.
		/// Server selects list items starting at specified index to the end of list and returns selected
		/// data specified by returnType.
		/// </summary>
		public static Operation GetByIndexRange(string binName, int index, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_INDEX_RANGE, (int)returnType, index, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by index range operation.
		/// Server selects "count" list items starting at specified index and returns selected data specified
		/// by returnType.
		/// </summary>
		public static Operation GetByIndexRange(string binName, int index, int count, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_INDEX_RANGE, (int)returnType, index, count, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by rank operation.
		/// Server selects list item identified by rank and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByRank(string binName, int rank, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_RANK, (int)returnType, rank, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by rank range operation.
		/// Server selects list items starting at specified rank to the last ranked item and returns selected
		/// data specified by returnType.
		/// </summary>
		public static Operation GetByRankRange(string binName, int rank, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_RANK_RANGE, (int)returnType, rank, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get by rank range operation.
		/// Server selects "count" list items starting at specified rank and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByRankRange(string binName, int rank, int count, ListReturnType returnType, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_RANK_RANGE, (int)returnType, rank, count, ctx);
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}
	}
}
