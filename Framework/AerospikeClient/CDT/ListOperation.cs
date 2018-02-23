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
using System;
using System.Net;
using System.Text;
using System.Collections;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// List bin operations. Create list operations used by client operate command.
	/// List operations support negative indexing.  If the index is negative, the
	/// resolved index starts backwards from end of list.
	/// <para>
	/// Index/Range examples:
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
	/// If an index is out of bounds, a parameter error will be returned. If a range is partially 
	/// out of bounds, the valid part of the range will be returned.
	/// </para>
	/// </summary>
	public class ListOperation
	{
		private const int SET_TYPE = 0;
		private const int APPEND = 1;
		private const int APPEND_ITEMS = 2;
		private const int INSERT = 3;
		private const int INSERT_ITEMS = 4;
		private const int POP = 5;
		private const int POP_RANGE = 6;
		private const int REMOVE = 7;
		private const int REMOVE_RANGE = 8;
		private const int SET = 9;
		private const int TRIM = 10;
		private const int CLEAR = 11;
		private const int INCREMENT = 12;
		private const int SORT = 13;
		private const int SIZE = 16;
		private const int GET = 17;
		private const int GET_RANGE = 18;
		private const int GET_BY_INDEX = 19;
		private const int GET_BY_RANK = 21;
		private const int GET_BY_VALUE = 22; // GET_ALL_BY_VALUE on server.
		private const int GET_BY_VALUE_LIST = 23;
		private const int GET_BY_INDEX_RANGE = 24;
		private const int GET_BY_VALUE_INTERVAL = 25;
		private const int GET_BY_RANK_RANGE = 26;
		private const int REMOVE_BY_INDEX = 32;
		private const int REMOVE_BY_RANK = 34;
		private const int REMOVE_BY_VALUE = 35;
		private const int REMOVE_BY_VALUE_LIST = 36;
		private const int REMOVE_BY_INDEX_RANGE = 37;
		private const int REMOVE_BY_VALUE_INTERVAL = 38;
		private const int REMOVE_BY_RANK_RANGE = 39;

		/// <summary>
		/// Create set list order operation.
		/// Server sets list order.  Server returns null.
		/// </summary>
		public static Operation SetOrder(string binName, ListOrder order)
		{
			return CDT.CreateOperation(SET_TYPE, Operation.Type.CDT_MODIFY, binName, (int)order);
		}

		/// <summary>
		/// Create default list append operation.
		/// Server appends value to end of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Append(string binName, Value value)
		{
			Packer packer = new Packer();
			packer.PackRawShort(APPEND);
			packer.PackArrayBegin(1);
			value.Pack(packer);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create list append operation with policy.
		/// Server appends value to list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Append(ListPolicy policy, string binName, Value value)
		{
			Packer packer = new Packer();
			packer.PackRawShort(APPEND);
			packer.PackArrayBegin(3);
			value.Pack(packer);
			packer.PackNumber(policy.attributes);
			packer.PackNumber(policy.flags);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create default list append items operation.
		/// Server appends each input list item to end of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation AppendItems(string binName, IList list)
		{
			Packer packer = new Packer();
			packer.PackRawShort(APPEND_ITEMS);
			packer.PackArrayBegin(1);
			packer.PackList(list);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create list append items operation with policy.
		/// Server appends each input list item to list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation AppendItems(ListPolicy policy, string binName, IList list)
		{
			Packer packer = new Packer();
			packer.PackRawShort(APPEND_ITEMS);
			packer.PackArrayBegin(3);
			packer.PackList(list);
			packer.PackNumber(policy.attributes);
			packer.PackNumber(policy.flags);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create default list insert operation.
		/// Server inserts value to specified index of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Insert(string binName, int index, Value value)
		{
			return CDT.CreateOperation(INSERT, Operation.Type.CDT_MODIFY, binName, index, value);
		}

		/// <summary>
		/// Create list insert operation with policy.
		/// Server inserts value to specified index of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Insert(ListPolicy policy, string binName, int index, Value value)
		{
			return CDT.CreateOperation(INSERT, Operation.Type.CDT_MODIFY, binName, index, value, policy.flags);
		}

		/// <summary>
		/// Create default list insert items operation.
		/// Server inserts each input list item starting at specified index of list bin. 
		/// Server returns list size.
		/// </summary>
		public static Operation InsertItems(string binName, int index, IList list)
		{
			Packer packer = new Packer();
			packer.PackRawShort(INSERT_ITEMS);
			packer.PackArrayBegin(2);
			packer.PackNumber(index);
			packer.PackList(list);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create list insert items operation with policy.
		/// Server inserts each input list item starting at specified index of list bin. 
		/// Server returns list size.
		/// </summary>
		public static Operation InsertItems(ListPolicy policy, string binName, int index, IList list)
		{
			Packer packer = new Packer();
			packer.PackRawShort(INSERT_ITEMS);
			packer.PackArrayBegin(3);
			packer.PackNumber(index);
			packer.PackList(list);
			packer.PackNumber(policy.flags);
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create default list increment operation.
		/// Server increments list[index] by 1.
		/// Server returns list[index] after incrementing.
		/// </summary>
		public static Operation Increment(string binName, int index)
		{
			return CDT.CreateOperation(INCREMENT, Operation.Type.CDT_MODIFY, binName, index);
		}

		/// <summary>
		/// Create list increment operation with policy.
		/// Server increments list[index] by 1.
		/// Server returns list[index] after incrementing.
		/// </summary>
		public static Operation Increment(ListPolicy policy, string binName, int index)
		{
			return CDT.CreateOperation(INCREMENT, Operation.Type.CDT_MODIFY, binName, index, policy.flags);
		}

		/// <summary>
		/// Create default list increment operation.
		/// Server increments list[index] by value.
		/// Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
		/// Server returns list[index] after incrementing.
		/// </summary>
		public static Operation Increment(string binName, int index, Value value)
		{
			return CDT.CreateOperation(INCREMENT, Operation.Type.CDT_MODIFY, binName, index, value);
		}

		/// <summary>
		/// Create list increment operation.
		/// Server increments list[index] by value.
		/// Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
		/// Server returns list[index] after incrementing.
		/// </summary>
		public static Operation Increment(ListPolicy policy, string binName, int index, Value value)
		{
			return CDT.CreateOperation(INCREMENT, Operation.Type.CDT_MODIFY, binName, index, value, policy.flags);
		}

		/// <summary>
		/// Create list pop operation.
		/// Server returns item at specified index and removes item from list bin.
		/// </summary>
		public static Operation Pop(string binName, int index)
		{
			return CDT.CreateOperation(POP, Operation.Type.CDT_MODIFY, binName, index);
		}

		/// <summary>
		/// Create list pop range operation.
		/// Server returns "count" items starting at specified index and removes items from list bin.
		/// </summary>
		public static Operation PopRange(string binName, int index, int count)
		{
			return CDT.CreateOperation(POP_RANGE, Operation.Type.CDT_MODIFY, binName, index, count);
		}

		/// <summary>
		/// Create list pop range operation.
		/// Server returns items starting at specified index to the end of list and removes those items
		/// from list bin.
		/// </summary>
		public static Operation PopRange(string binName, int index)
		{
			return CDT.CreateOperation(POP_RANGE, Operation.Type.CDT_MODIFY, binName, index);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes item at specified index from list bin.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation Remove(string binName, int index)
		{
			return CDT.CreateOperation(REMOVE, Operation.Type.CDT_MODIFY, binName, index);
		}

		/// <summary>
		/// Create list remove range operation.
		/// Server removes "count" items starting at specified index from list bin.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation RemoveRange(string binName, int index, int count)
		{
			return CDT.CreateOperation(REMOVE_RANGE, Operation.Type.CDT_MODIFY, binName, index, count);
		}

		/// <summary>
		/// Create list remove range operation.
		/// Server removes items starting at specified index to the end of list.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation RemoveRange(string binName, int index)
		{
			return CDT.CreateOperation(REMOVE_RANGE, Operation.Type.CDT_MODIFY, binName, index);
		}

		/// <summary>
		/// Create list set operation.
		/// Server sets item value at specified index in list bin.
		/// Server does not return a result by default.
		/// </summary>
		public static Operation Set(string binName, int index, Value value)
		{
			return CDT.CreateOperation(SET, Operation.Type.CDT_MODIFY, binName, index, value);
		}

		/// <summary>
		/// Create list set operation with policy.
		/// Server sets item value at specified index in list bin.
		/// Server does not return a result by default.
		/// </summary>
		public static Operation Set(ListPolicy policy, string binName, int index, Value value)
		{
			return CDT.CreateOperation(SET, Operation.Type.CDT_MODIFY, binName, index, value, policy.flags);
		}

		/// <summary>
		/// Create list trim operation.
		/// Server removes items in list bin that do not fall into range specified by index 
		/// and count range.  If the range is out of bounds, then all items will be removed.
		/// Server returns list size after trim.
		/// </summary>
		public static Operation Trim(string binName, int index, int count)
		{
			return CDT.CreateOperation(TRIM, Operation.Type.CDT_MODIFY, binName, index, count);
		}

		/// <summary>
		/// Create list clear operation.
		/// Server removes all items in list bin.
		/// Server does not return a result by default.
		/// </summary>
		public static Operation Clear(string binName)
		{
			return CDT.CreateOperation(CLEAR, Operation.Type.CDT_MODIFY, binName);
		}

		/// <summary>
		/// Create list sort operation.
		/// Server sorts list according to sortFlags.
		/// Server does not return a result by default.
		/// </summary>
		/// <param name="binName">server bin name</param>
		/// <param name="sortFlags">sort flags</param>
		public static Operation Sort(string binName, ListSortFlags sortFlags)
		{
			return CDT.CreateOperation(SORT, Operation.Type.CDT_MODIFY, binName, (int)sortFlags);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items identified by value and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByValue(string binName, Value value, ListReturnType returnType)
		{
			return CDT.CreateOperation(REMOVE_BY_VALUE, Operation.Type.CDT_MODIFY, binName, (int)returnType, value);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items identified by values and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByValueList(string binName, IList values, ListReturnType returnType)
		{
			return CDT.CreateOperation(REMOVE_BY_VALUE_LIST, Operation.Type.CDT_MODIFY, binName, (int)returnType, values);
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
		public static Operation RemoveByValueRange(string binName, Value valueBegin, Value valueEnd, ListReturnType returnType)
		{
			return CDT.CreateRangeOperation(REMOVE_BY_VALUE_INTERVAL, Operation.Type.CDT_MODIFY, binName, valueBegin, valueEnd, (int)returnType);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list item identified by index and returns removed data specified by returnType. 
		/// </summary>
		public static Operation RemoveByIndex(string binName, int index, ListReturnType returnType)
		{
			return CDT.CreateOperation(REMOVE_BY_INDEX, Operation.Type.CDT_MODIFY, binName, (int)returnType, index);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items starting at specified index to the end of list and returns removed
		/// data specified by returnType.
		/// </summary>
		public static Operation RemoveByIndexRange(string binName, int index, ListReturnType returnType)
		{
			return CDT.CreateOperation(REMOVE_BY_INDEX_RANGE, Operation.Type.CDT_MODIFY, binName, (int)returnType, index);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes "count" list items starting at specified index and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByIndexRange(string binName, int index, int count, ListReturnType returnType)
		{
			return CDT.CreateOperation(REMOVE_BY_INDEX_RANGE, Operation.Type.CDT_MODIFY, binName, (int)returnType, index, count);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list item identified by rank and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByRank(string binName, int rank, ListReturnType returnType)
		{
			return CDT.CreateOperation(REMOVE_BY_RANK, Operation.Type.CDT_MODIFY, binName, (int)returnType, rank);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes list items starting at specified rank to the last ranked item and returns removed
		/// data specified by returnType.
		/// </summary>
		public static Operation RemoveByRankRange(string binName, int rank, ListReturnType returnType)
		{
			return CDT.CreateOperation(REMOVE_BY_RANK_RANGE, Operation.Type.CDT_MODIFY, binName, (int)returnType, rank);
		}

		/// <summary>
		/// Create list remove operation.
		/// Server removes "count" list items starting at specified rank and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByRankRange(string binName, int rank, int count, ListReturnType returnType)
		{
			return CDT.CreateOperation(REMOVE_BY_RANK_RANGE, Operation.Type.CDT_MODIFY, binName, (int)returnType, rank, count);
		}

		/// <summary>
		/// Create list size operation.
		/// Server returns size of list.
		/// </summary>
		public static Operation Size(string binName)
		{
			return CDT.CreateOperation(SIZE, Operation.Type.CDT_READ, binName);
		}

		/// <summary>
		/// Create list get operation.
		/// Server returns item at specified index in list bin.
		/// </summary>
		public static Operation Get(string binName, int index)
		{
			return CDT.CreateOperation(GET, Operation.Type.CDT_READ, binName, index);
		}

		/// <summary>
		/// Create list get range operation.
		/// Server returns "count" items starting at specified index in list bin.
		/// </summary>
		public static Operation GetRange(string binName, int index, int count)
		{
			return CDT.CreateOperation(GET_RANGE, Operation.Type.CDT_READ, binName, index, count);
		}

		/// <summary>
		/// Create list get range operation.
		/// Server returns items starting at index to the end of list.
		/// </summary>
		public static Operation GetRange(string binName, int index)
		{
			return CDT.CreateOperation(GET_RANGE, Operation.Type.CDT_READ, binName, index);
		}

		/// <summary>
		/// Create list get by value operation.
		/// Server selects list items identified by value and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByValue(string binName, Value value, ListReturnType returnType)
		{
			return CDT.CreateOperation(GET_BY_VALUE, Operation.Type.CDT_READ, binName, (int)returnType, value);
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
		public static Operation GetByValueRange(string binName, Value valueBegin, Value valueEnd, ListReturnType returnType)
		{
			return CDT.CreateRangeOperation(GET_BY_VALUE_INTERVAL, Operation.Type.CDT_READ, binName, valueBegin, valueEnd, (int)returnType);
		}

		/// <summary>
		/// Create list get by value list operation.
		/// Server selects list items identified by values and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByValueList(string binName, IList values, ListReturnType returnType)
		{
			return CDT.CreateOperation(GET_BY_VALUE_LIST, Operation.Type.CDT_READ, binName, (int)returnType, values);
		}

		/// <summary>
		/// Create list get by index operation.
		/// Server selects list item identified by index and returns selected data specified by returnType
		///. 
		/// </summary>
		public static Operation GetByIndex(string binName, int index, ListReturnType returnType)
		{
			return CDT.CreateOperation(GET_BY_INDEX, Operation.Type.CDT_READ, binName, (int)returnType, index);
		}

		/// <summary>
		/// Create list get by index range operation.
		/// Server selects list items starting at specified index to the end of list and returns selected
		/// data specified by returnType.
		/// </summary>
		public static Operation GetByIndexRange(string binName, int index, ListReturnType returnType)
		{
			return CDT.CreateOperation(GET_BY_INDEX_RANGE, Operation.Type.CDT_READ, binName, (int)returnType, index);
		}

		/// <summary>
		/// Create list get by index range operation.
		/// Server selects "count" list items starting at specified index and returns selected data specified
		/// by returnType.
		/// </summary>
		public static Operation GetByIndexRange(string binName, int index, int count, ListReturnType returnType)
		{
			return CDT.CreateOperation(GET_BY_INDEX_RANGE, Operation.Type.CDT_READ, binName, (int)returnType, index, count);
		}

		/// <summary>
		/// Create list get by rank operation.
		/// Server selects list item identified by rank and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByRank(string binName, int rank, ListReturnType returnType)
		{
			return CDT.CreateOperation(GET_BY_RANK, Operation.Type.CDT_READ, binName, (int)returnType, rank);
		}

		/// <summary>
		/// Create list get by rank range operation.
		/// Server selects list items starting at specified rank to the last ranked item and returns selected
		/// data specified by returnType.
		/// </summary>
		public static Operation GetByRankRange(string binName, int rank, ListReturnType returnType)
		{
			return CDT.CreateOperation(GET_BY_RANK_RANGE, Operation.Type.CDT_READ, binName, (int)returnType, rank);
		}

		/// <summary>
		/// Create list get by rank range operation.
		/// Server selects "count" list items starting at specified rank and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByRankRange(string binName, int rank, int count, ListReturnType returnType)
		{
			return CDT.CreateOperation(GET_BY_RANK_RANGE, Operation.Type.CDT_READ, binName, (int)returnType, rank, count);
		}
	}
}
