/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
	/// Map expression generator. See <see cref="Aerospike.Client.Exp"/>.
	/// <para>
	/// The bin expression argument in these methods can be a reference to a bin or the
	/// result of another expression. Expressions that modify bin values are only used
	/// for temporary expression evaluation and are not permanently applied to the bin.
	/// </para>
	/// <para>
	/// Map modify expressions return the bin's value. This value will be a map except
	/// when the map is nested within a list. In that case, a list is returned for the
	/// map modify expression.
	/// </para>
	/// <para>
	/// Valid map key types are:
	/// <ul>
	/// <li>string</li>
	/// <li>integer</li>
	/// <li>byte[]</li>
	/// </ul>
	/// The server will vaildate map key types in an upcoming release.
	/// </para>
	/// <para>
	/// All maps maintain an index and a rank.  The index is the item offset from the start of the map,
	/// for both unordered and ordered maps.  The rank is the sorted index of the value component.
	/// Map supports negative indexing for index and rank.
	/// </para>
	/// <para>
	/// Index examples:
	/// <ul>
	/// <li>Index 0: First item in map.</li>
	/// <li>Index 4: Fifth item in map.</li>
	/// <li>Index -1: Last item in map.</li>
	/// <li>Index -3: Third to last item in map.</li>
	/// <li>Index 1 Count 2: Second and third items in map.</li>
	/// <li>Index -3 Count 3: Last three items in map.</li>
	/// <li>Index -5 Count 4: Range between fifth to last item to second to last item inclusive.</li>
	/// </ul>
	/// </para>
	/// <para>
	/// Rank examples:
	/// <ul>
	/// <li>Rank 0: Item with lowest value rank in map.</li>
	/// <li>Rank 4: Fifth lowest ranked item in map.</li>
	/// <li>Rank -1: Item with highest ranked value in map.</li>
	/// <li>Rank -3: Item with third highest ranked value in map.</li>
	/// <li>Rank 1 Count 2: Second and third lowest ranked items in map.</li>
	/// <li>Rank -3 Count 3: Top three ranked items in map.</li>
	/// </ul>
	/// </para>
	/// <para>
	/// Nested expressions are supported by optional CTX context arguments.  Example:
	/// <ul>
	/// <li>bin = {key1={key11=9,key12=4}, key2={key21=3,key22=5}}</li>
	/// <li>Set map value to 11 for map key "key21" inside of map key "key2".</li>
	/// <li>Get size of map key2.</li>
	/// <li>MapExp.size(Exp.MapBin("bin"), CTX.mapKey(Value.get("key2"))</li>
	/// <li>result = 2</li>
	/// </ul>
	/// </para>
	/// </summary>
	public sealed class MapExp
	{
		private const int MODULE = 0;

		/// <summary>
		/// Create expression that writes key/value item to map bin.
		/// </summary>
		public static Exp Put(MapPolicy policy, Exp key, Exp value, Exp bin, params CTX[] ctx)
		{
			Packer packer = new Packer();

			if (policy.flags != 0)
			{
				PackUtil.Init(packer, ctx);
				packer.PackArrayBegin(5);
				packer.PackNumber(MapOperation.PUT);
				key.Pack(packer);
				value.Pack(packer);
				packer.PackNumber(policy.attributes);
				packer.PackNumber(policy.flags);
			}
			else
			{
				if (policy.itemCommand == MapOperation.REPLACE)
				{
					// Replace doesn't allow map attributes because it does not create on non-existing key.
					PackUtil.Init(packer, ctx);
					packer.PackArrayBegin(3);
					packer.PackNumber(policy.itemCommand);
					key.Pack(packer);
					value.Pack(packer);
				}
				else
				{
					PackUtil.Init(packer, ctx);
					packer.PackArrayBegin(4);
					packer.PackNumber(policy.itemCommand);
					key.Pack(packer);
					value.Pack(packer);
					packer.PackNumber(policy.attributes);
				}
			}
			byte[] bytes = packer.ToByteArray();
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that writes each map item to map bin.
		/// </summary>
		public static Exp PutItems(MapPolicy policy, Exp map, Exp bin, params CTX[] ctx)
		{
			Packer packer = new Packer();

			if (policy.flags != 0)
			{
				PackUtil.Init(packer, ctx);
				packer.PackArrayBegin(4);
				packer.PackNumber(MapOperation.PUT_ITEMS);
				map.Pack(packer);
				packer.PackNumber(policy.attributes);
				packer.PackNumber(policy.flags);
			}
			else
			{
				if (policy.itemsCommand == MapOperation.REPLACE_ITEMS)
				{
					// Replace doesn't allow map attributes because it does not create on non-existing key.
					PackUtil.Init(packer, ctx);
					packer.PackArrayBegin(2);
					packer.PackNumber(policy.itemsCommand);
					map.Pack(packer);
				}
				else
				{
					PackUtil.Init(packer, ctx);
					packer.PackArrayBegin(3);
					packer.PackNumber(policy.itemsCommand);
					map.Pack(packer);
					packer.PackNumber(policy.attributes);
				}
			}
			byte[] bytes = packer.ToByteArray();
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that increments values by incr for all items identified by key.
		/// Valid only for numbers.
		/// </summary>
		public static Exp Increment(MapPolicy policy, Exp key, Exp incr, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.INCREMENT, key, incr, policy.attributes, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes all items in map.
		/// </summary>
		public static Exp Clear(Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.CLEAR, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map item identified by key.
		/// </summary>
		public static Exp RemoveByKey(Exp key, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_KEY, (int)MapReturnType.NONE, key, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items identified by keys.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByKeyList(MapReturnType returnType, Exp keys, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_KEY_LIST, (int)returnType, keys, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items identified by key range (keyBegin inclusive, keyEnd exclusive).
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// If keyBegin is null, the range is less than keyEnd.
		/// If keyEnd is null, the range is greater than equal to keyBegin.
		/// </summary>
		public static Exp RemoveByKeyRange(MapReturnType returnType, Exp keyBegin, Exp keyEnd, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = ListExp.PackRangeOperation(MapOperation.REMOVE_BY_KEY_INTERVAL, (int)returnType, keyBegin, keyEnd, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items nearest to key and greater by index.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// <para>
		/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
		/// <ul>
		/// <li>(value,index) = [removed items]</li>
		/// <li>(5,0) = [{5=15},{9=10}]</li>
		/// <li>(5,1) = [{9=10}]</li>
		/// <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
		/// <li>(3,2) = [{9=10}]</li>
		/// <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Exp RemoveByKeyRelativeIndexRange(MapReturnType returnType, Exp key, Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_KEY_REL_INDEX_RANGE, (int)returnType, key, index, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items nearest to key and greater by index with a count limit.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// <para>
		/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
		/// <ul>
		/// <li>(value,index,count) = [removed items]</li>
		/// <li>(5,0,1) = [{5=15}]</li>
		/// <li>(5,1,2) = [{9=10}]</li>
		/// <li>(5,-1,1) = [{4=2}]</li>
		/// <li>(3,2,1) = [{9=10}]</li>
		/// <li>(3,-2,2) = [{0=17}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Exp RemoveByKeyRelativeIndexRange(MapReturnType returnType, Exp key, Exp index, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_KEY_REL_INDEX_RANGE, (int)returnType, key, index, count, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items identified by value.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByValue(MapReturnType returnType, Exp value, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_VALUE, (int)returnType, value, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items identified by values.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByValueList(MapReturnType returnType, Exp values, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_VALUE_LIST, (int)returnType, values, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// If valueBegin is null, the range is less than valueEnd.
		/// If valueEnd is null, the range is greater than equal to valueBegin.
		/// </summary>
		public static Exp RemoveByValueRange(MapReturnType returnType, Exp valueBegin, Exp valueEnd, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = ListExp.PackRangeOperation(MapOperation.REMOVE_BY_VALUE_INTERVAL, (int)returnType, valueBegin, valueEnd, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items nearest to value and greater by relative rank.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// <para>
		/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
		/// <ul>
		/// <li>(value,rank) = [removed items]</li>
		/// <li>(11,1) = [{0=17}]</li>
		/// <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Exp RemoveByValueRelativeRankRange(MapReturnType returnType, Exp value, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items nearest to value and greater by relative rank with a count limit.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// <para>
		/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
		/// <ul>
		/// <li>(value,rank,count) = [removed items]</li>
		/// <li>(11,1,1) = [{0=17}]</li>
		/// <li>(11,-1,1) = [{9=10}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Exp RemoveByValueRelativeRankRange(MapReturnType returnType, Exp value, Exp rank, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, count, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map item identified by index.
		/// </summary>
		public static Exp RemoveByIndex(Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_INDEX, (int)MapReturnType.NONE, index, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items starting at specified index to the end of map.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByIndexRange(MapReturnType returnType, Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_INDEX_RANGE, (int)returnType, index, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes "count" map items starting at specified index.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByIndexRange(MapReturnType returnType, Exp index, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_INDEX_RANGE, (int)returnType, index, count, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map item identified by rank.
		/// </summary>
		public static Exp RemoveByRank(Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_RANK, (int)MapReturnType.NONE, rank, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes map items starting at specified rank to the last ranked item.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByRankRange(MapReturnType returnType, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_RANK_RANGE, (int)returnType, rank, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that removes "count" map items starting at specified rank.
		/// Valid returnType values are <see cref="MapReturnType.NONE"/>
		/// or <see cref="MapReturnType.INVERTED"/>
		/// </summary>
		public static Exp RemoveByRankRange(MapReturnType returnType, Exp rank, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.REMOVE_BY_RANK_RANGE, (int)returnType, rank, count, ctx);
			return AddWrite(bin, bytes, ctx);
		}

		/// <summary>
		/// Create expression that returns list size.
		/// </summary>
		/// <example>
		/// <code>
		/// // Map bin "a" size > 7
		/// Exp.GT(MapExp.Size(Exp.MapBin("a")), Exp.Val(7))
		/// </code>
		/// </example>
		public static Exp Size(Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.SIZE, ctx);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that selects map item identified by key and returns selected data
		/// specified by returnType.
		/// </summary>
		/// <example>
		/// <code>
		/// // Map bin "a" contains key "B"
		/// Exp.GT(
		///   MapExp.GetByKey(MapReturnType.COUNT, Type.INT, Exp.Val("B"), Exp.MapBin("a")),
		///   Exp.Val(0));
		/// </code>
		/// </example>
		/// <param name="returnType">metadata attributes to return. See <see cref="MapReturnType"/> </param>
		/// <param name="valueType">		expected type of return value </param>
		/// <param name="key">			map key expression </param>
		/// <param name="bin">			bin or map value expression </param>
		/// <param name="ctx">			optional context path for nested CDT </param>
		public static Exp GetByKey(MapReturnType returnType, Exp.Type valueType, Exp key, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_KEY, (int)returnType, key, ctx);
			return AddRead(bin, bytes, valueType);
		}

		/// <summary>
		/// Create expression that selects map items identified by key range (keyBegin inclusive, keyEnd exclusive).
		/// If keyBegin is null, the range is less than keyEnd.
		/// If keyEnd is null, the range is greater than equal to keyBegin.
		/// <para>
		/// Expression returns selected data specified by returnType.
		/// </para>
		/// </summary>
		public static Exp GetByKeyRange(MapReturnType returnType, Exp keyBegin, Exp keyEnd, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = ListExp.PackRangeOperation(MapOperation.GET_BY_KEY_INTERVAL, (int)returnType, keyBegin, keyEnd, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map items identified by keys and returns selected data specified by
		/// returnType.
		/// </summary>
		public static Exp GetByKeyList(MapReturnType returnType, Exp keys, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_KEY_LIST, (int)returnType, keys, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map items nearest to key and greater by index.
		/// Expression returns selected data specified by returnType.
		/// <para>
		/// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
		/// <ul>
		/// <li>(value,index) = [selected items]</li>
		/// <li>(5,0) = [{5=15},{9=10}]</li>
		/// <li>(5,1) = [{9=10}]</li>
		/// <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
		/// <li>(3,2) = [{9=10}]</li>
		/// <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Exp GetByKeyRelativeIndexRange(MapReturnType returnType, Exp key, Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_KEY_REL_INDEX_RANGE, (int)returnType, key, index, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map items nearest to key and greater by index with a count limit.
		/// Expression returns selected data specified by returnType.
		/// <para>
		/// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
		/// <ul>
		/// <li>(value,index,count) = [selected items]</li>
		/// <li>(5,0,1) = [{5=15}]</li>
		/// <li>(5,1,2) = [{9=10}]</li>
		/// <li>(5,-1,1) = [{4=2}]</li>
		/// <li>(3,2,1) = [{9=10}]</li>
		/// <li>(3,-2,2) = [{0=17}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Exp GetByKeyRelativeIndexRange(MapReturnType returnType, Exp key, Exp index, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_KEY_REL_INDEX_RANGE, (int)returnType, key, index, count, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map items identified by value and returns selected data
		/// specified by returnType.
		/// </summary>
		/// <example>
		/// <code>
		/// // Map bin "a" contains value "BBB"
		/// Exp.GT(
		///   MapExp.GetByValue(MapReturnType.COUNT, Exp.Val("BBB"), Exp.MapBin("a")),
		///   Exp.Val(0))
		/// </code>
		/// </example>
		/// <param name="returnType">metadata attributes to return. See <see cref="MapReturnType"/></param>
		/// <param name="value">value expression</param>
		/// <param name="bin">bin or map value expression</param>
		/// <param name="ctx">optional context path for nested CDT</param>
		public static Exp GetByValue(MapReturnType returnType, Exp value, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_VALUE, (int)returnType, value, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
		/// If valueBegin is null, the range is less than valueEnd.
		/// If valueEnd is null, the range is greater than equal to valueBegin.
		/// <para>
		/// Expression returns selected data specified by returnType.
		/// </para>
		/// </summary>
		public static Exp GetByValueRange(MapReturnType returnType, Exp valueBegin, Exp valueEnd, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = ListExp.PackRangeOperation(MapOperation.GET_BY_VALUE_INTERVAL, (int)returnType, valueBegin, valueEnd, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map items identified by values and returns selected data specified by
		/// returnType.
		/// </summary>
		public static Exp GetByValueList(MapReturnType returnType, Exp values, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_VALUE_LIST, (int)returnType, values, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map items nearest to value and greater by relative rank.
		/// Expression returns selected data specified by returnType.
		/// <para>
		/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
		/// <ul>
		/// <li>(value,rank) = [selected items]</li>
		/// <li>(11,1) = [{0=17}]</li>
		/// <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Exp GetByValueRelativeRankRange(MapReturnType returnType, Exp value, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map items nearest to value and greater by relative rank with a count limit.
		/// Expression returns selected data specified by returnType.
		/// <para>
		/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
		/// <ul>
		/// <li>(value,rank,count) = [selected items]</li>
		/// <li>(11,1,1) = [{0=17}]</li>
		/// <li>(11,-1,1) = [{9=10}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Exp GetByValueRelativeRankRange(MapReturnType returnType, Exp value, Exp rank, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_VALUE_REL_RANK_RANGE, (int)returnType, value, rank, count, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map item identified by index and returns selected data specified by
		/// returnType.
		/// </summary>
		public static Exp GetByIndex(MapReturnType returnType, Exp.Type valueType, Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_INDEX, (int)returnType, index, ctx);
			return AddRead(bin, bytes, valueType);
		}

		/// <summary>
		/// Create expression that selects map items starting at specified index to the end of map and returns selected
		/// data specified by returnType.
		/// </summary>
		public static Exp GetByIndexRange(MapReturnType returnType, Exp index, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_INDEX_RANGE, (int)returnType, index, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects "count" map items starting at specified index and returns selected data
		/// specified by returnType.
		/// </summary>
		public static Exp GetByIndexRange(MapReturnType returnType, Exp index, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_INDEX_RANGE, (int)returnType, index, count, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects map item identified by rank and returns selected data specified by
		/// returnType.
		/// </summary>
		public static Exp GetByRank(MapReturnType returnType, Exp.Type valueType, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_RANK, (int)returnType, rank, ctx);
			return AddRead(bin, bytes, valueType);
		}

		/// <summary>
		/// Create expression that selects map items starting at specified rank to the last ranked item and
		/// returns selected data specified by returnType.
		/// </summary>
		public static Exp GetByRankRange(MapReturnType returnType, Exp rank, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_RANK_RANGE, (int)returnType, rank, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		/// <summary>
		/// Create expression that selects "count" map items starting at specified rank and returns selected
		/// data specified by returnType.
		/// </summary>
		public static Exp GetByRankRange(MapReturnType returnType, Exp rank, Exp count, Exp bin, params CTX[] ctx)
		{
			byte[] bytes = PackUtil.Pack(MapOperation.GET_BY_RANK_RANGE, (int)returnType, rank, count, ctx);
			return AddRead(bin, bytes, GetValueType(returnType));
		}

		private static Exp AddWrite(Exp bin, byte[] bytes, CTX[] ctx)
		{
			int retType;

			if (ctx == null || ctx.Length == 0)
			{
				retType = (int)Exp.Type.MAP;
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

		private static Exp.Type GetValueType(MapReturnType returnType)
		{
			MapReturnType t = returnType & ~MapReturnType.INVERTED;

			return t switch
			{
				// This method only called from expressions that can return multiple integers (ie list).
				MapReturnType.INDEX or MapReturnType.REVERSE_INDEX or MapReturnType.RANK or MapReturnType.REVERSE_RANK => Exp.Type.LIST,
				MapReturnType.COUNT => Exp.Type.INT,
				// This method only called from expressions that can return multiple objects (ie list).
				MapReturnType.KEY or MapReturnType.VALUE => Exp.Type.LIST,
				MapReturnType.KEY_VALUE or MapReturnType.ORDERED_MAP or MapReturnType.UNORDERED_MAP => Exp.Type.MAP,
				MapReturnType.EXISTS => Exp.Type.BOOL,
				_ => throw new AerospikeException("Invalid MapReturnType: " + returnType),
			};
		}
	}
}
