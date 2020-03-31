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
using System.Collections;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// HyperLogLog (HLL) operations.
	/// Requires server versions >= 4.9.
	/// <para>
	/// HyperLogLog operations on HLL items nested in lists/maps are not currently
	/// supported by the server.
	/// </para>
	/// </summary>
	public sealed class HLLOperation
	{
		private const int INIT = 0;
		private const int ADD = 1;
		private const int SET_UNION = 2;
		private const int SET_COUNT = 3;
		private const int FOLD = 4;
		private const int COUNT = 50;
		private const int UNION = 51;
		private const int UNION_COUNT = 52;
		private const int INTERSECT_COUNT = 53;
		private const int SIMILARITY = 54;
		private const int DESCRIBE = 55;

		/// <summary>
		/// Create HLL init operation.
		/// Server creates a new HLL or resets an existing HLL.
		/// Server does not return a value.
		/// </summary>
		/// <param name="policy">write policy, use <seealso cref="HLLPolicy.Default"/> for default</param>
		/// <param name="binName">name of bin</param>
		/// <param name="indexBitCount">number of index bits. Must be between 4 and 16 inclusive.</param>
		public static Operation Init(HLLPolicy policy, string binName, int indexBitCount)
		{
			return Init(policy, binName, indexBitCount, -1);
		}

		/// <summary>
		/// Create HLL init operation with minhash bits.
		/// Server creates a new HLL or resets an existing HLL.
		/// Server does not return a value.
		/// </summary>
		/// <param name="policy">write policy, use <seealso cref="HLLPolicy.Default"/> for default</param>
		/// <param name="binName">name of bin</param>
		/// <param name="indexBitCount">number of index bits. Must be between 4 and 16 inclusive.</param>
		/// <param name="minHashBitCount">number of min hash bits. Must be between 4 and 58 inclusive.</param>
		public static Operation Init(HLLPolicy policy, string binName, int indexBitCount, int minHashBitCount)
		{
			Packer packer = new Packer();
			Init(packer, INIT, 3);
			packer.PackNumber(indexBitCount);
			packer.PackNumber(minHashBitCount);
			packer.PackNumber(policy.flags);
			return new Operation(Operation.Type.HLL_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL add operation. This operation assumes HLL bin already exists.
		/// Server adds values to the HLL set.
		/// Server returns number of entries that caused HLL to update a register.
		/// </summary>
		/// <param name="policy">write policy, use <seealso cref="HLLPolicy.Default"/> for default</param>
		/// <param name="binName">name of bin</param>
		/// <param name="list">list of values to be added</param>
		public static Operation Add(HLLPolicy policy, string binName, IList list)
		{
			return Add(policy, binName, list, -1, -1);
		}

		/// <summary>
		/// Create HLL add operation.
		/// Server adds values to HLL set. If HLL bin does not exist, use indexBitCount to create HLL bin.
		/// Server returns number of entries that caused HLL to update a register.
		/// </summary>
		/// <param name="policy">write policy, use <seealso cref="HLLPolicy.Default"/> for default</param>
		/// <param name="binName">name of bin</param>
		/// <param name="list">list of values to be added</param>
		/// <param name="indexBitCount">number of index bits. Must be between 4 and 16 inclusive.</param>
		public static Operation Add(HLLPolicy policy, string binName, IList list, int indexBitCount)
		{
			return Add(policy, binName, list, indexBitCount, -1);
		}

		/// <summary>
		/// Create HLL add operation with minhash bits.
		/// Server adds values to HLL set. If HLL bin does not exist, use indexBitCount and minHashBitCount
		/// to create HLL bin. Server returns number of entries that caused HLL to update a register.
		/// </summary>
		/// <param name="policy">write policy, use <seealso cref="HLLPolicy.Default"/> for default</param>
		/// <param name="binName">name of bin</param>
		/// <param name="list">list of values to be added</param>
		/// <param name="indexBitCount">number of index bits. Must be between 4 and 16 inclusive.</param>
		/// <param name="minHashBitCount">number of min hash bits. Must be between 4 and 58 inclusive.</param>
		public static Operation Add(HLLPolicy policy, string binName, IList list, int indexBitCount, int minHashBitCount)
		{
			Packer packer = new Packer();
			Init(packer, ADD, 4);
			packer.PackList(list);
			packer.PackNumber(indexBitCount);
			packer.PackNumber(minHashBitCount);
			packer.PackNumber(policy.flags);
			return new Operation(Operation.Type.HLL_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL set union operation.
		/// Server sets union of specified HLL objects with HLL bin.
		/// Server does not return a value.
		/// </summary>
		/// <param name="policy">write policy, use <seealso cref="HLLPolicy.Default"/> for default</param>
		/// <param name="binName">name of bin</param>
		/// <param name="list">list of HLL objects</param>
		public static Operation SetUnion(HLLPolicy policy, string binName, IList<Value.HLLValue> list)
		{
			Packer packer = new Packer();
			Init(packer, SET_UNION, 2);
			packer.PackList((IList)list);
			packer.PackNumber(policy.flags);
			return new Operation(Operation.Type.HLL_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL refresh operation.
		/// Server updates the cached count (if stale) and returns the count.
		/// </summary>
		/// <param name="binName">name of bin</param>
		public static Operation RefreshCount(string binName)
		{
			Packer packer = new Packer();
			Init(packer, SET_COUNT, 0);
			return new Operation(Operation.Type.HLL_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL fold operation.
		/// Servers folds indexBitCount to the specified value.
		/// This can only be applied when minHashBitCount on the HLL bin is 0.
		/// Server does not return a value.
		/// </summary>
		/// <param name="binName">name of bin</param>
		/// <param name="indexBitCount">number of index bits. Must be between 4 and 16 inclusive.</param>
		public static Operation Fold(string binName, int indexBitCount)
		{
			Packer packer = new Packer();
			Init(packer, FOLD, 1);
			packer.PackNumber(indexBitCount);
			return new Operation(Operation.Type.HLL_MODIFY, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL getCount operation.
		/// Server returns estimated number of elements in the HLL bin.
		/// </summary>
		/// <param name="binName">name of bin</param>
		public static Operation GetCount(string binName)
		{
			Packer packer = new Packer();
			Init(packer, COUNT, 0);
			return new Operation(Operation.Type.HLL_READ, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL getUnion operation.
		/// Server returns an HLL object that is the union of all specified HLL objects in the list
		/// with the HLL bin.
		/// </summary>
		/// <param name="binName">name of bin</param>
		/// <param name="list">list of HLL objects</param>
		public static Operation GetUnion(string binName, IList<Value.HLLValue> list)
		{
			Packer packer = new Packer();
			Init(packer, UNION, 1);
			packer.PackList((IList)list);
			return new Operation(Operation.Type.HLL_READ, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL getUnionCount operation.
		/// Server returns estimated number of elements that would be contained by the union of these
		/// HLL objects.
		/// </summary>
		/// <param name="binName">name of bin</param>
		/// <param name="list">list of HLL objects</param>
		public static Operation GetUnionCount(string binName, IList<Value.HLLValue> list)
		{
			Packer packer = new Packer();
			Init(packer, UNION_COUNT, 1);
			packer.PackList((IList)list);
			return new Operation(Operation.Type.HLL_READ, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL getIntersectCount operation.
		/// Server returns estimated number of elements that would be contained by the intersection of
		/// these HLL objects.
		/// </summary>
		/// <param name="binName">name of bin</param>
		/// <param name="list">list of HLL objects</param>
		public static Operation GetIntersectCount(string binName, IList<Value.HLLValue> list)
		{
			Packer packer = new Packer();
			Init(packer, INTERSECT_COUNT, 1);
			packer.PackList((IList)list);
			return new Operation(Operation.Type.HLL_READ, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL getSimilarity operation.
		/// Server returns estimated similarity of these HLL objects. Return type is a double.
		/// </summary>
		/// <param name="binName">name of bin</param>
		/// <param name="list">list of HLL objects</param>
		public static Operation GetSimilarity(string binName, IList<Value.HLLValue> list)
		{
			Packer packer = new Packer();
			Init(packer, SIMILARITY, 1);
			packer.PackList((IList)list);
			return new Operation(Operation.Type.HLL_READ, binName, Value.Get(packer.ToByteArray()));
		}

		/// <summary>
		/// Create HLL describe operation.
		/// Server returns indexBitCount and minHashBitCount used to create HLL bin in a list of longs.
		/// The list size is 2.
		/// </summary>
		/// <param name="binName">name of bin</param>
		public static Operation Describe(string binName)
		{
			Packer packer = new Packer();
			Init(packer, DESCRIBE, 0);
			return new Operation(Operation.Type.HLL_READ, binName, Value.Get(packer.ToByteArray()));
		}

		private static void Init(Packer packer, int command, int count)
		{
			packer.PackArrayBegin(count + 1);
			packer.PackNumber(command);
		}
	}
}
