/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
		private const int SIZE = 16;
		private const int GET = 17;
		private const int GET_RANGE = 18;

		/// <summary>
		/// Create list append operation.
		/// Server appends value to end of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Append(string binName, Value value)
		{
			Packer packer = new Packer();
			packer.PackRawShort(APPEND);
			packer.PackArrayBegin(1);
			value.Pack(packer);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list append items operation.
		/// Server appends each input list item to end of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation AppendItems(string binName, IList list)
		{
			Packer packer = new Packer();
			packer.PackRawShort(APPEND_ITEMS);
			packer.PackArrayBegin(1);
			packer.PackList(list);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list insert operation.
		/// Server inserts value to specified index of list bin.
		/// Server returns list size.
		/// </summary>
		public static Operation Insert(string binName, int index, Value value)
		{
			Packer packer = new Packer();
			packer.PackRawShort(INSERT);
			packer.PackArrayBegin(2);
			packer.PackNumber(index);
			value.Pack(packer);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list insert items operation.
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
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list pop operation.
		/// Server returns item at specified index and removes item from list bin.
		/// </summary>
		public static Operation Pop(string binName, int index)
		{
			Packer packer = new Packer();
			packer.PackRawShort(POP);
			packer.PackArrayBegin(1);
			packer.PackNumber(index);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list pop range operation.
		/// Server returns "count" items starting at specified index and removes items from list bin.
		/// </summary>
		public static Operation PopRange(string binName, int index, int count)
		{
			Packer packer = new Packer();
			packer.PackRawShort(POP_RANGE);
			packer.PackArrayBegin(2);
			packer.PackNumber(index);
			packer.PackNumber(count);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list pop range operation.
		/// Server returns items starting at specified index to the end of list and removes those items
		/// from list bin.
		/// </summary>
		public static Operation PopRange(string binName, int index)
		{
			Packer packer = new Packer();
			packer.PackRawShort(POP_RANGE);
			packer.PackArrayBegin(1);
			packer.PackNumber(index);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}
	
		/// <summary>
		/// Create list remove operation.
		/// Server removes item at specified index from list bin.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation Remove(string binName, int index)
		{
			Packer packer = new Packer();
			packer.PackRawShort(REMOVE);
			packer.PackArrayBegin(1);
			packer.PackNumber(index);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove range operation.
		/// Server removes "count" items starting at specified index from list bin.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation RemoveRange(string binName, int index, int count)
		{
			Packer packer = new Packer();
			packer.PackRawShort(REMOVE_RANGE);
			packer.PackArrayBegin(2);
			packer.PackNumber(index);
			packer.PackNumber(count);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list remove range operation.
		/// Server removes items starting at specified index to the end of list.
		/// Server returns number of items removed.
		/// </summary>
		public static Operation RemoveRange(string binName, int index)
		{
			Packer packer = new Packer();
			packer.PackRawShort(REMOVE_RANGE);
			packer.PackArrayBegin(1);
			packer.PackNumber(index);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}
	
		/// <summary>
		/// Create list set operation.
		/// Server sets item value at specified index in list bin.
		/// Server does not return a result by default.
		/// </summary>
		public static Operation Set(string binName, int index, Value value)
		{
			Packer packer = new Packer();
			packer.PackRawShort(SET);
			packer.PackArrayBegin(2);
			packer.PackNumber(index);
			value.Pack(packer);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list trim operation.
		/// Server removes "count" items in list bin that do not fall into range specified
		/// by index and count range.  If the range is out of bounds, then all items will be removed.
		/// Server returns list size after trim.
		/// </summary>
		public static Operation Trim(string binName, int index, int count)
		{
			Packer packer = new Packer();
			packer.PackRawShort(TRIM);
			packer.PackArrayBegin(2);
			packer.PackNumber(index);
			packer.PackNumber(count);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list clear operation.
		/// Server removes all items in list bin.
		/// Server does not return a result by default.
		/// </summary>
		public static Operation Clear(string binName)
		{
			Packer packer = new Packer();
			packer.PackRawShort(CLEAR);
			//packer.PackArrayBegin(0);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_MODIFY, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list size operation.
		/// Server returns size of list.
		/// </summary>
		public static Operation Size(string binName)
		{
			Packer packer = new Packer();
			packer.PackRawShort(SIZE);
			//packer.PackArrayBegin(0);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get operation.
		/// Server returns item at specified index in list bin.
		/// </summary>
		public static Operation Get(string binName, int index)
		{
			Packer packer = new Packer();
			packer.PackRawShort(GET);
			packer.PackArrayBegin(1);
			packer.PackNumber(index);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get range operation.
		/// Server returns "count" items starting at specified index in list bin.
		/// </summary>
		public static Operation GetRange(string binName, int index, int count)
		{
			Packer packer = new Packer();
			packer.PackRawShort(GET_RANGE);
			packer.PackArrayBegin(2);
			packer.PackNumber(index);
			packer.PackNumber(count);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}

		/// <summary>
		/// Create list get range operation.
		/// Server returns items starting at index to the end of list.
		/// </summary>
		public static Operation GetRange(string binName, int index)
		{
			Packer packer = new Packer();
			packer.PackRawShort(GET_RANGE);
			packer.PackArrayBegin(1);
			packer.PackNumber(index);
			byte[] bytes = packer.ToByteArray();
			return new Operation(Operation.Type.CDT_READ, binName, Value.Get(bytes));
		}
	}
}
