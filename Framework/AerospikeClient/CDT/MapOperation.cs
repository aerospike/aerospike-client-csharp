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

namespace Aerospike.Client
{
	/// <summary>
	/// Unique key map bin operations. Create map operations used by the client operate command.
	/// The default unique key map is unordered.
	/// <para>
	/// All maps maintain an index and a rank.  The index is the item offset from the start of the map,
	/// for both unordered and ordered maps.  The rank is the sorted index of the value component.
	/// Map supports negative indexing for index and rank.
	/// </para>
	/// <para>
	/// Index examples:
	/// </para>
	/// <ul>
	/// <li>Index 0: First item in map.</li>
	/// <li>Index 4: Fifth item in map.</li>
	/// <li>Index -1: Last item in map.</li>
	/// <li>Index -3: Third to last item in map.</li>
	/// <li>Index 1 Count 2: Second and third items in map.</li>
	/// <li>Index -3 Count 3: Last three items in map.</li>
	/// <li>Index -5 Count 4: Range between fifth to last item to second to last item inclusive.</li>
	/// </ul>
	/// <para>
	/// Rank examples:
	/// </para>
	/// <ul>
	/// <li>Rank 0: Item with lowest value rank in map.</li>
	/// <li>Rank 4: Fifth lowest ranked item in map.</li>
	/// <li>Rank -1: Item with highest ranked value in map.</li>
	/// <li>Rank -3: Item with third highest ranked value in map.</li>
	/// <li>Rank 1 Count 2: Second and third lowest ranked items in map.</li>
	/// <li>Rank -3 Count 3: Top three ranked items in map.</li>
	/// </ul>
	/// Nested CDT operations are supported by optional CTX context arguments.  Examples:
	/// <ul>
	/// <li>bin = {key1={key11=9,key12=4}, key2={key21=3,key22=5}}</li>
	/// <li>Set map value to 11 for map key "key21" inside of map key "key2".</li>
	/// <li>MapOperation.put(MapPolicy.Default, "bin", Value.get("key21"), Value.get(11), CTX.mapKey(Value.get("key2")))</li>
	/// <li>bin result = {key1={key11=9,key12=4},key2={key21=11,key22=5}}</li>
	/// <li></li>
	/// <li>bin = {key1={key11={key111=1},key12={key121=5}}, key2={key21={"key211",7}}}</li>
	/// <li>Set map value to 11 in map key "key121" for highest ranked map ("key12") inside of map key "key1".</li>
	/// <li>MapOperation.put(MapPolicy.Default, "bin", Value.get("key121"), Value.get(11), CTX.mapKey(Value.get("key1")), CTX.mapRank(-1))</li>
	/// <li>bin result = {key1={key11={key111=1},key12={key121=11}}, key2={key21={"key211",7}}}</li>
	/// </ul>
	/// </summary>
	public class MapOperation
	{
		private const int SET_TYPE = 64;
		protected internal const int ADD = 65;
		protected internal const int ADD_ITEMS = 66;
		protected internal const int PUT = 67;
		protected internal const int PUT_ITEMS = 68;
		protected internal const int REPLACE = 69;
		protected internal const int REPLACE_ITEMS = 70;
		private const int INCREMENT = 73;
		private const int DECREMENT = 74;
		private const int CLEAR = 75;
		private const int REMOVE_BY_KEY = 76;
		private const int REMOVE_BY_INDEX = 77;
		private const int REMOVE_BY_RANK = 79;
		private const int REMOVE_BY_KEY_LIST = 81;
		private const int REMOVE_BY_VALUE = 82;
		private const int REMOVE_BY_VALUE_LIST = 83;
		private const int REMOVE_BY_KEY_INTERVAL = 84;
		private const int REMOVE_BY_INDEX_RANGE = 85;
		private const int REMOVE_BY_VALUE_INTERVAL = 86;
		private const int REMOVE_BY_RANK_RANGE = 87;
		private const int REMOVE_BY_KEY_REL_INDEX_RANGE = 88;
		private const int REMOVE_BY_VALUE_REL_RANK_RANGE = 89;
		private const int SIZE = 96;
		private const int GET_BY_KEY = 97;
		private const int GET_BY_INDEX = 98;
		private const int GET_BY_RANK = 100;
		private const int GET_BY_VALUE = 102;  // GET_ALL_BY_VALUE on server.
		private const int GET_BY_KEY_INTERVAL = 103;
		private const int GET_BY_INDEX_RANGE = 104;
		private const int GET_BY_VALUE_INTERVAL = 105;
		private const int GET_BY_RANK_RANGE = 106;
		private const int GET_BY_KEY_LIST = 107;
		private const int GET_BY_VALUE_LIST = 108;
		private const int GET_BY_KEY_REL_INDEX_RANGE = 109;
		private const int GET_BY_VALUE_REL_RANK_RANGE = 110;

		/// <summary>
		/// Create map create operation.
		/// Server creates map at given context level.
		/// </summary>
		public static Operation Create(string binName, MapOrder order, params CTX[] ctx)
		{
			// If context not defined, the set order for top-level bin map.
			if (ctx == null || ctx.Length == 0)
			{
				return SetMapPolicy(new MapPolicy(order, MapWriteMode.UPDATE), binName);
			}

			Packer packer = new Packer();
			CDT.Init(packer, ctx, SET_TYPE, 1, CTX.GetFlag(order));
			packer.PackNumber((int)order);
			return new Operation(Operation.Type.MAP_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create set map policy operation.
		/// Server sets map policy attributes.  Server returns null.
		/// <para>
		/// The required map policy attributes can be changed after the map is created.
		/// </para>
		/// </summary>
		public static Operation SetMapPolicy(MapPolicy policy, string binName, params CTX[] ctx)
		{
			return CDT.CreateOperation(SET_TYPE, Operation.Type.MAP_MODIFY, binName, ctx, policy.attributes);
		}

		/// <summary>
		/// Create map put operation.
		/// Server writes key/value item to map bin and returns map size.
		/// <para>
		/// The required map policy dictates the type of map to create when it does not exist.
		/// The map policy also specifies the flags used when writing items to the map.
		/// See policy <seealso cref="Aerospike.Client.MapPolicy"/>.
		/// </para>
		/// </summary>
		public static Operation Put(MapPolicy policy, string binName, Value key, Value value, params CTX[] ctx)
		{
			Packer packer = new Packer();

			if (policy.flags != 0)
			{
				CDT.Init(packer, ctx, PUT, 4);
				key.Pack(packer);
				value.Pack(packer);
				packer.PackNumber(policy.attributes);
				packer.PackNumber(policy.flags);
			}
			else
			{
				if (policy.itemCommand == REPLACE)
				{
					// Replace doesn't allow map attributes because it does not create on non-existing key.
					CDT.Init(packer, ctx, policy.itemCommand, 2);
					key.Pack(packer);
					value.Pack(packer);
				}
				else
				{
					CDT.Init(packer, ctx, policy.itemCommand, 3);
					key.Pack(packer);
					value.Pack(packer);
					packer.PackNumber(policy.attributes);
				}
			}
			return new Operation(Operation.Type.MAP_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create map put items operation
		/// Server writes each map item to map bin and returns map size.
		/// <para>
		/// The required map policy dictates the type of map to create when it does not exist.
		/// The map policy also specifies the flags used when writing items to the map.
		/// See policy <seealso cref="Aerospike.Client.MapPolicy"/>.
		/// </para>
		/// </summary>
		public static Operation PutItems(MapPolicy policy, string binName, IDictionary map, params CTX[] ctx)
		{
			Packer packer = new Packer();

			if (policy.flags != 0)
			{
				CDT.Init(packer, ctx, PUT_ITEMS, 3);
				packer.PackMap(map);
				packer.PackNumber(policy.attributes);
				packer.PackNumber(policy.flags);
			}
			else
			{
				if (policy.itemsCommand == REPLACE_ITEMS)
				{
					// Replace doesn't allow map attributes because it does not create on non-existing key.
					CDT.Init(packer, ctx, policy.itemsCommand, 1);
					packer.PackMap(map);
				}
				else
				{
					CDT.Init(packer, ctx, policy.itemsCommand, 2);
					packer.PackMap(map);
					packer.PackNumber(policy.attributes);
				}
			}
			return new Operation(Operation.Type.MAP_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create map increment operation.
		/// Server increments values by incr for all items identified by key and returns final result.
		/// Valid only for numbers. 
		/// <para>
		/// The required map policy dictates the type of map to create when it does not exist.
		/// The map policy also specifies the mode used when writing items to the map.
		/// See policy <seealso cref="Aerospike.Client.MapPolicy"/> and write mode 
		/// <seealso cref="Aerospike.Client.MapWriteMode"/>.
		/// </para>
		/// </summary>
		public static Operation Increment(MapPolicy policy, string binName, Value key, Value incr, params CTX[] ctx)
		{
			return CDT.CreateOperation(INCREMENT, Operation.Type.MAP_MODIFY, binName, ctx, key, incr, policy.attributes);
		}

		/// <summary>
		/// Create map decrement operation.
		/// Server decrements values by decr for all items identified by key and returns final result.
		/// Valid only for numbers. 
		/// <para>
		/// The required map policy dictates the type of map to create when it does not exist.
		/// The map policy also specifies the mode used when writing items to the map.
		/// See policy <seealso cref="Aerospike.Client.MapPolicy"/> and write mode 
		/// <seealso cref="Aerospike.Client.MapWriteMode"/>.
		/// </para>
		/// </summary>
		public static Operation Decrement(MapPolicy policy, string binName, Value key, Value decr, params CTX[] ctx)
		{
			return CDT.CreateOperation(DECREMENT, Operation.Type.MAP_MODIFY, binName, ctx, key, decr, policy.attributes);
		}

		/// <summary>
		/// Create map clear operation.
		/// Server removes all items in map.  Server returns null.
		/// </summary>
		public static Operation Clear(string binName, params CTX[] ctx)
		{
			return CDT.CreateOperation(CLEAR, Operation.Type.MAP_MODIFY, binName, ctx);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map item identified by key and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByKey(string binName, Value key, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_KEY, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, key);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map items identified by keys and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByKeyList(string binName, IList keys, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_KEY_LIST, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, keys);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map items identified by key range (keyBegin inclusive, keyEnd exclusive).
		/// If keyBegin is null, the range is less than keyEnd.
		/// If keyEnd is null, the range is greater than equal to keyBegin. 
		/// <para>
		/// Server returns removed data specified by returnType. 
		/// </para>
		/// </summary>
		public static Operation RemoveByKeyRange(string binName, Value keyBegin, Value keyEnd, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateRangeOperation(REMOVE_BY_KEY_INTERVAL, Operation.Type.MAP_MODIFY, binName, ctx, keyBegin, keyEnd, (int)returnType);
		}

		/// <summary>
		/// Create map remove by key relative to index range operation.
		/// Server removes map items nearest to key and greater by index.
		/// Server returns removed data specified by returnType.
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
		public static Operation RemoveByKeyRelativeIndexRange(string binName, Value key, int index, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_KEY_REL_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, key, index);
		}

		/// <summary>
		/// Create map remove by key relative to index range operation.
		/// Server removes map items nearest to key and greater by index with a count limit.
		/// Server returns removed data specified by returnType.
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
		public static Operation RemoveByKeyRelativeIndexRange(string binName, Value key, int index, int count, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_KEY_REL_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, key, index, count);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map items identified by value and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByValue(string binName, Value value, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_VALUE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, value);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map items identified by values and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByValueList(string binName, IList values, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_VALUE_LIST, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, values);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
		/// If valueBegin is null, the range is less than valueEnd.
		/// If valueEnd is null, the range is greater than equal to valueBegin. 
		/// <para>
		/// Server returns removed data specified by returnType. 
		/// </para>
		/// </summary>
		public static Operation RemoveByValueRange(string binName, Value valueBegin, Value valueEnd, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateRangeOperation(REMOVE_BY_VALUE_INTERVAL, Operation.Type.MAP_MODIFY, binName, ctx, valueBegin, valueEnd, (int)returnType);
		}

		/// <summary>
		/// Create map remove by value relative to rank range operation.
		/// Server removes map items nearest to value and greater by relative rank.
		/// Server returns removed data specified by returnType.
		/// <para>
		/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
		/// <ul>
		/// <li>(value,rank) = [removed items]</li>
		/// <li>(11,1) = [{0=17}]</li>
		/// <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Operation RemoveByValueRelativeRankRange(string binName, Value value, int rank, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_VALUE_REL_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, value, rank);
		}

		/// <summary>
		/// Create map remove by value relative to rank range operation.
		/// Server removes map items nearest to value and greater by relative rank with a count limit.
		/// Server returns removed data specified by returnType.
		/// <para>
		/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
		/// <ul>
		/// <li>(value,rank,count) = [removed items]</li>
		/// <li>(11,1,1) = [{0=17}]</li>
		/// <li>(11,-1,1) = [{9=10}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Operation RemoveByValueRelativeRankRange(string binName, Value value, int rank, int count, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_VALUE_REL_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, value, rank, count);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map item identified by index and returns removed data specified by returnType. 
		/// </summary>
		public static Operation RemoveByIndex(string binName, int index, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_INDEX, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, index);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map items starting at specified index to the end of map and returns removed
		/// data specified by returnType.
		/// </summary>
		public static Operation RemoveByIndexRange(string binName, int index, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, index);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes "count" map items starting at specified index and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByIndexRange(string binName, int index, int count, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, index, count);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map item identified by rank and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByRank(string binName, int rank, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_RANK, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, rank);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes map items starting at specified rank to the last ranked item and returns removed
		/// data specified by returnType.
		/// </summary>
		public static Operation RemoveByRankRange(string binName, int rank, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, rank);
		}

		/// <summary>
		/// Create map remove operation.
		/// Server removes "count" map items starting at specified rank and returns removed data specified by returnType.
		/// </summary>
		public static Operation RemoveByRankRange(string binName, int rank, int count, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(REMOVE_BY_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, (int)returnType, rank, count);
		}

		/// <summary>
		/// Create map size operation.
		/// Server returns size of map.
		/// </summary>
		public static Operation Size(string binName, params CTX[] ctx)
		{
			return CDT.CreateOperation(SIZE, Operation.Type.MAP_READ, binName, ctx);
		}

		/// <summary>
		/// Create map get by key operation.
		/// Server selects map item identified by key and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByKey(string binName, Value key, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_KEY, Operation.Type.MAP_READ, binName, ctx, (int)returnType, key);
		}

		/// <summary>
		/// Create map get by key range operation.
		/// Server selects map items identified by key range (keyBegin inclusive, keyEnd exclusive). 
		/// If keyBegin is null, the range is less than keyEnd.
		/// If keyEnd is null, the range is greater than equal to keyBegin. 
		/// <para>
		/// Server returns selected data specified by returnType. 
		/// </para>
		/// </summary>
		public static Operation GetByKeyRange(string binName, Value keyBegin, Value keyEnd, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateRangeOperation(GET_BY_KEY_INTERVAL, Operation.Type.MAP_READ, binName, ctx, keyBegin, keyEnd, (int)returnType);
		}

		/// <summary>
		/// Create map get by key list operation.
		/// Server selects map items identified by keys and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByKeyList(string binName, IList keys, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_KEY_LIST, Operation.Type.MAP_READ, binName, ctx, (int)returnType, keys);
		}

		/// <summary>
		/// Create map get by key relative to index range operation.
		/// Server selects map items nearest to key and greater by index.
		/// Server returns selected data specified by returnType.
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
		public static Operation GetByKeyRelativeIndexRange(string binName, Value key, int index, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_KEY_REL_INDEX_RANGE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, key, index);
		}

		/// <summary>
		/// Create map get by key relative to index range operation.
		/// Server selects map items nearest to key and greater by index with a count limit.
		/// Server returns selected data specified by returnType.
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
		public static Operation GetByKeyRelativeIndexRange(string binName, Value key, int index, int count, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_KEY_REL_INDEX_RANGE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, key, index, count);
		}

		/// <summary>
		/// Create map get by value operation.
		/// Server selects map items identified by value and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByValue(string binName, Value value, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_VALUE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, value);
		}

		/// <summary>
		/// Create map get by value range operation.
		/// Server selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
		/// If valueBegin is null, the range is less than valueEnd.
		/// If valueEnd is null, the range is greater than equal to valueBegin. 
		/// <para>
		/// Server returns selected data specified by returnType. 
		/// </para>
		/// </summary>
		public static Operation GetByValueRange(string binName, Value valueBegin, Value valueEnd, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateRangeOperation(GET_BY_VALUE_INTERVAL, Operation.Type.MAP_READ, binName, ctx, valueBegin, valueEnd, (int)returnType);
		}

		/// <summary>
		/// Create map get by value list operation.
		/// Server selects map items identified by values and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByValueList(string binName, IList values, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_VALUE_LIST, Operation.Type.MAP_READ, binName, ctx, (int)returnType, values);
		}

		/// <summary>
		/// Create map get by value relative to rank range operation.
		/// Server selects map items nearest to value and greater by relative rank.
		/// Server returns selected data specified by returnType.
		/// <para>
		/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
		/// <ul>
		/// <li>(value,rank) = [selected items]</li>
		/// <li>(11,1) = [{0=17}]</li>
		/// <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Operation GetByValueRelativeRankRange(string binName, Value value, int rank, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_VALUE_REL_RANK_RANGE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, value, rank);
		}

		/// <summary>
		/// Create map get by value relative to rank range operation.
		/// Server selects map items nearest to value and greater by relative rank with a count limit.
		/// Server returns selected data specified by returnType.
		/// <para>
		/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
		/// <ul>
		/// <li>(value,rank,count) = [selected items]</li>
		/// <li>(11,1,1) = [{0=17}]</li>
		/// <li>(11,-1,1) = [{9=10}]</li>
		/// </ul>
		/// </para>
		/// </summary>
		public static Operation GetByValueRelativeRankRange(string binName, Value value, int rank, int count, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_VALUE_REL_RANK_RANGE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, value, rank, count);
		}

		/// <summary>
		/// Create map get by index operation.
		/// Server selects map item identified by index and returns selected data specified by returnType. 
		/// </summary>
		public static Operation GetByIndex(string binName, int index, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_INDEX, Operation.Type.MAP_READ, binName, ctx, (int)returnType, index);
		}

		/// <summary>
		/// Create map get by index range operation.
		/// Server selects map items starting at specified index to the end of map and returns selected
		/// data specified by returnType.
		/// </summary>
		public static Operation GetByIndexRange(string binName, int index, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_INDEX_RANGE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, index);
		}

		/// <summary>
		/// Create map get by index range operation.
		/// Server selects "count" map items starting at specified index and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByIndexRange(string binName, int index, int count, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_INDEX_RANGE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, index, count);
		}

		/// <summary>
		/// Create map get by rank operation.
		/// Server selects map item identified by rank and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByRank(string binName, int rank, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_RANK, Operation.Type.MAP_READ, binName, ctx, (int)returnType, rank);
		}

		/// <summary>
		/// Create map get by rank range operation.
		/// Server selects map items starting at specified rank to the last ranked item and returns selected
		/// data specified by returnType.
		/// </summary>
		public static Operation GetByRankRange(string binName, int rank, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_RANK_RANGE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, rank);
		}

		/// <summary>
		/// Create map get by rank range operation.
		/// Server selects "count" map items starting at specified rank and returns selected data specified by returnType.
		/// </summary>
		public static Operation GetByRankRange(string binName, int rank, int count, MapReturnType returnType, params CTX[] ctx)
		{
			return CDT.CreateOperation(GET_BY_RANK_RANGE, Operation.Type.MAP_READ, binName, ctx, (int)returnType, rank, count);
		}
	}
}
