/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
namespace Aerospike.Client
{
	/// <summary>
	/// List expression generator. See <see cref="Aerospike.Client.Exp"/>.
	/// <para>
	/// The bin expression argument in these methods can be a reference to a bin or the
	/// result of another expression. Expressions that modify bin values are only used
	/// for temporary expression evaluation and are not permanently applied to the bin.
	/// </para>
	/// <para>
	/// List modify expressions return the bin's value. This value will be a list except
	/// when the list is nested within a map. In that case, a map is returned for the
	/// list modify expression.
	/// </para>
	/// <para>
	/// List expressions support negative indexing. If the index is negative, the
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
	/// Nested expressions are supported by optional CTX context arguments.  Example:
	/// <ul>
	/// <li>bin = [[7,9,5],[1,2,3],[6,5,4,1]]</li>
	/// <li>Get size of last list.</li>
	/// <li>ListExp.size(Exp.ListBin("bin"), CTX.listIndex(-1))</li>
	/// <li>result = 4</li>
	/// </ul>
	/// </para>
	/// </summary>
	public sealed class ListExp
	{
		private const int MODULE = 0;

		/// <summary>
		/// Create expression that appends list items to end of list.
		/// </summary>
		public static Exp Append(ListPolicy policy, Exp value, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.APPEND, value, policy.attributes, policy.flags, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that appends list items to end of list.
		/// </summary>
		public static Exp AppendItems(ListPolicy policy, Exp list, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.APPEND_ITEMS, list, policy.attributes, policy.flags, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that inserts value to specified index of list.
		/// </summary>
		public static Exp Insert(ListPolicy policy, Exp index, Exp value, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INSERT, index, value, policy.flags, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that inserts each input list item starting at specified index of list.
		/// </summary>
		public static Exp InsertItems(ListPolicy policy, Exp index, Exp list, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INSERT_ITEMS, index, list, policy.flags, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that increments list[index] by value.
		/// Value expression should resolve to a number.
		/// </summary>
		public static Exp Increment(ListPolicy policy, Exp index, Exp value, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.INCREMENT, index, value, policy.attributes, policy.flags, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that sets item value at specified index in list.
		/// </summary>
		public static Exp Set(ListPolicy policy, Exp index, Exp value, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.SET, index, value, policy.flags, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes all items in list.
		/// </summary>
		public static Exp Clear(Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.CLEAR, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that sorts list according to sortFlags.
		/// </summary>
		/// <param name="sortFlags">sort flags. See <see cref="ListSortFlags"/>.</param>
		/// <param name="bin">bin or list value expression</param>
		/// <param name="ctx">optional context path for nested CDT</param>
		public static Exp Sort(ListSortFlags sortFlags, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.SORT, (int)sortFlags, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list items identified by value.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByValue(ListReturnType returnType, Exp value, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_VALUE, (int)returnType, value, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list items identified by values.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByValueList(ListReturnType returnType, Exp values, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_VALUE_LIST, (int)returnType, values, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
		/// If valueBegin is null, the range is less than valueEnd. If valueEnd is null, the range is
		/// greater than equal to valueBegin.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByValueRange(ListReturnType returnType, Exp valueBegin, Exp valueEnd, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = ListExp.PackRangeOperation(ListOperation.REMOVE_BY_VALUE_INTERVAL, (int)returnType, valueBegin, valueEnd, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list items nearest to value and greater by relative rank.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
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
		public static Exp RemoveByValueRelativeRankRange(ListReturnType returnType, Exp value, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list items nearest to value and greater by relative rank with a count limit.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
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
		public static Exp RemoveByValueRelativeRankRange(ListReturnType returnType, Exp value, Exp rank, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, count, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list item identified by index.
		/// </summary>
		public static Exp RemoveByIndex(Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_INDEX, (int)ListReturnType.NONE, index, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list items starting at specified index to the end of list.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByIndexRange(ListReturnType returnType, Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_INDEX_RANGE, (int)returnType, index, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes "count" list items starting at specified index.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByIndexRange(ListReturnType returnType, Exp index, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_INDEX_RANGE, (int)returnType, index, count, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list item identified by rank.
		/// </summary>
		public static Exp RemoveByRank(Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_RANK, (int)ListReturnType.NONE, rank, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes list items starting at specified rank to the last ranked item.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByRankRange(ListReturnType returnType, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_RANK_RANGE, (int)returnType, rank, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes "count" list items starting at specified rank.
		/// Valid returnType values are <see cref="ListReturnType.NONE"/>
		/// or <see cref="ListReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByRankRange(ListReturnType returnType, Exp rank, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.REMOVE_BY_RANK_RANGE, (int)returnType, rank, count, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that returns list size.
		/// </summary>
		/// <example>
		/// <code>
		/// // List bin "a" size > 7
		/// Exp.GT(ListExp.Size(Exp.ListBin("a")), Exp.Val(7))
		/// </code>
		/// </example>
		public static Exp Size(Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.SIZE, ctx);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that selects list items identified by value and returns selected
		/// data specified by returnType.
		/// </summary>
		/// <example>
		/// <code>
		/// // List bin "a" contains at least one item == "abc"
		/// Exp.GT(
		///   ListExp.GetByValue(ListReturnType.COUNT, Exp.Val("abc"), Exp.ListBin("a")),
		///   Exp.Val(0))
		/// </code>
		/// </example>
		/// <param name="returnType">metadata attributes to return. See <see cref="ListReturnType"/></param>
		/// <param name="value">search expression</param>
		/// <param name="bin">list bin or list value expression</param>
		/// <param name="ctx">optional context path for nested CDT</param>
		public static Exp GetByValue(ListReturnType returnType, Exp value, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_VALUE, (int)returnType, value, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects list items identified by value range and returns selected data
		/// specified by returnType.
		/// </summary>
		/// <example>
		/// <code>
		/// // List bin "a" items >= 10 &amp;&amp; items &lt; 20
		/// ListExp.GetByValueRange(ListReturnType.VALUE, Exp.Val(10), Exp.Val(20), Exp.ListBin("a"))
		/// </code>
		/// </example>
		/// <param name="returnType">metadata attributes to return. See <see cref="ListReturnType"/></param>
		/// <param name="valueBegin">begin expression inclusive. If null, range is less than valueEnd.</param>
		/// <param name="valueEnd">end expression exclusive. If null, range is greater than equal to valueBegin.</param>
		/// <param name="bin">bin or list value expression</param>
		/// <param name="ctx">optional context path for nested CDT</param>
		public static Exp GetByValueRange(ListReturnType returnType, Exp valueBegin, Exp valueEnd, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = ListExp.PackRangeOperation(ListOperation.GET_BY_VALUE_INTERVAL, (int)returnType, valueBegin, valueEnd, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects list items identified by values and returns selected data
		/// specified by returnType.
		/// </summary>
		public static Exp GetByValueList(ListReturnType returnType, Exp values, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_VALUE_LIST, (int)returnType, values, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects list items nearest to value and greater by relative rank
		/// and returns selected data specified by returnType.
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
		public static Exp GetByValueRelativeRankRange(ListReturnType returnType, Exp value, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects list items nearest to value and greater by relative rank with a count limit
		/// and returns selected data specified by returnType.
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
		public static Exp GetByValueRelativeRankRange(ListReturnType returnType, Exp value, Exp rank, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, count, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects list item identified by index and returns
		/// selected data specified by returnType.
		/// </summary>
		/// <example>
		/// <code>
		/// // a[3] == 5
		/// Exp.EQ(
		///   ListExp.GetByIndex(ListReturnType.VALUE, Type.INT, Exp.Val(3), Exp.ListBin("a")),
		///   Exp.Val(5));
		/// </code>
		/// </example>
		/// <param name="returnType">metadata attributes to return. See <see cref="ListReturnType"/></param>
		/// <param name="valueType">		expected type of return value </param>
		/// <param name="index">			list index expression </param>
		/// <param name="bin">list bin or list value expression</param>
		/// <param name="ctx">optional context path for nested CDT</param>
		public static Exp GetByIndex(ListReturnType returnType, Exp.Type valueType, Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_INDEX, (int)returnType, index, ctx);
			return AddRead(bin, bytes, valueType);
		}

		/// <summary>
		/// Create expression that selects list items starting at specified index to the end of list
		/// and returns selected data specified by returnType.
		/// </summary>
		public static Exp GetByIndexRange(ListReturnType returnType, Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_INDEX_RANGE, (int)returnType, index, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects "count" list items starting at specified index
		/// and returns selected data specified by returnType.
		/// </summary>
		public static Exp GetByIndexRange(ListReturnType returnType, Exp index, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_INDEX_RANGE, (int)returnType, index, count, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects list item identified by rank and returns selected
		/// data specified by returnType.
		/// </summary>
		/// <example>
		/// <code>
		/// // Player with lowest score.
		/// ListExp.GetByRank(ListReturnType.VALUE, Type.STRING, Exp.Val(0), Exp.ListBin("a"))
		/// </code>
		/// </example>
		/// <param name="returnType">metadata attributes to return. See <see cref="ListReturnType"/></param>
		/// <param name="valueType">expected type of return value</param>
		/// <param name="rank">rank expression</param>
		/// <param name="bin">list bin or list value expression</param>
		/// <param name="ctx">optional context path for nested CDT</param>
		public static Exp GetByRank(ListReturnType returnType, Exp.Type valueType, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_RANK, (int)returnType, rank, ctx);
			return AddRead(bin, bytes, valueType);
		}

		/// <summary>
		/// Create expression that selects list items starting at specified rank to the last ranked item
		/// and returns selected data specified by returnType.
		/// </summary>
		public static Exp GetByRankRange(ListReturnType returnType, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_RANK_RANGE, (int)returnType, rank, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects "count" list items starting at specified rank and returns
		/// selected data specified by returnType.
		/// </summary>
		public static Exp GetByRankRange(ListReturnType returnType, Exp rank, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(ListOperation.GET_BY_RANK_RANGE, (int)returnType, rank, count, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		private static Exp AddWrite(Exp bin, byte[] bytes, CTX[] ctx)
		{
			int retType;

			if (ctx == null || ctx.Length == 0)
			{
				retType = (int)Exp.Type.LIST;
			}
			else
			{
				retType = ((ctx[0].id & 0x10) == 0) ? (int)Exp.Type.MAP : (int)Exp.Type.LIST;
			}
			return new Exp.Module(bin, bytes, retType, MODULE | Exp.MODIFY);
		}

		private static Exp AddRead(Exp bin, byte[] bytes, Exp.Type retType)
		{
			return new Exp.Module(bin, bytes, (int)retType, MODULE);
		}

		private static Exp.Type GetValueType(ListReturnType returnType)
		{
			ListReturnType t = returnType & ~ListReturnType.INVERTED;

			return t switch
			{
				ListReturnType.INDEX or ListReturnType.REVERSE_INDEX or ListReturnType.RANK or ListReturnType.REVERSE_RANK => Exp.Type.LIST,
				ListReturnType.COUNT => Exp.Type.INT,
				ListReturnType.VALUE => Exp.Type.LIST, // This method only called from expressions that can return multiple objects (ie list).
				ListReturnType.EXISTS => Exp.Type.BOOL,
				_ => throw new AerospikeException("Invalid ListReturnType: " + returnType),
			};
		}

		internal static byte[] PackRangeOperation(int command, int returnType, Exp begin, Exp end, CTX[] ctx)
		{
			Packer packer = new Packer();
			PackUtil.Init(packer, ctx);
			packer.PackArrayBegin((end != null) ? 4 : 3);
			packer.PackNumber(command);
			packer.PackNumber(returnType);

			if (begin != null)
			{
				begin.Pack(packer);
			}
			else
			{
				packer.PackNil();
			}

			if (end != null)
			{
				end.Pack(packer);
			}
			return packer.ToByteArray();
		}
	}
}
