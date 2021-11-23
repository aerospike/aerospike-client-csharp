/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
	/// HyperLogLog (HLL) expression generator. See <see cref="Aerospike.Client.Exp"/>.
	/// <para>
	/// The bin expression argument in these methods can be a reference to a bin or the
	/// result of another expression. Expressions that modify bin values are only used
	/// for temporary expression evaluation and are not permanently applied to the bin.
	/// HLL modify expressions return the HLL bin's value.
	/// </para>
	/// </summary>
	public sealed class HLLExp
	{
		private const int MODULE = 2;

		/// <summary>
		/// Create expression that creates a new HLL or resets an existing HLL.
		/// </summary>
		/// <param name="policy">write policy, use <see cref="Aerospike.Client.HLLPolicy.Default"/> for default</param>
		/// <param name="indexBitCount">number of index bits expression. Must be between 4 and 16 inclusive.</param>
		/// <param name="bin">HLL bin or value expression</param>
		public static Exp Init(HLLPolicy policy, Exp indexBitCount, Exp bin)
		{
			return Init(policy, indexBitCount, Exp.Val(-1), bin);
		}

		/// <summary>
		/// Create expression that creates a new HLL or resets an existing HLL with minhash bits.
		/// </summary>
		/// <param name="policy">write policy, use <see cref="Aerospike.Client.HLLPolicy.Default"/> for default</param>
		/// <param name="indexBitCount">number of index bits expression. Must be between 4 and 16 inclusive.</param>
		/// <param name="minHashBitCount">number of min hash bits expression. Must be between 4 and 51 inclusive.</param>
		/// <param name="bin">HLL bin or value expression</param>
		public static Exp Init(HLLPolicy policy, Exp indexBitCount, Exp minHashBitCount, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.INIT, indexBitCount, minHashBitCount, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that adds list values to a HLL set and returns HLL set.
		/// The function assumes HLL bin already exists.
		/// </summary>
		/// <example>
		/// <code>
		/// // Add values to HLL bin "a" and check count > 7
		/// Exp.GT(
		///   HLLExp.GetCount(
		///     HLLExp.Add(HLLPolicy.Default, Exp.Val(list), Exp.HLLBin("a"))),
		///   Exp.Val(7))
		/// </code>
		/// </example>
		/// <param name="policy">write policy, use <see cref="Aerospike.Client.HLLPolicy.Default"/> for default</param>
		/// <param name="list">list bin or value expression of values to be added</param>
		/// <param name="bin">HLL bin or value expression</param>
		public static Exp Add(HLLPolicy policy, Exp list, Exp bin)
		{
			return Add(policy, list, Exp.Val(-1), Exp.Val(-1), bin);
		}

		/// <summary>
		/// Create expression that adds values to a HLL set and returns HLL set.
		/// If HLL bin does not exist, use indexBitCount to create HLL bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // Add values to HLL bin "a" and check count > 7
		/// Exp.GT(
		///   HLLExp.GetCount(
		///     HLLExp.Add(HLLPolicy.Default, Exp.Val(list), Exp.Val(10), Exp.HLLBin("a"))),
		///   Exp.Val(7))
		/// </code>
		/// </example>
		/// <param name="policy">write policy, use <see cref="Aerospike.Client.HLLPolicy.Default"/> for default</param>
		/// <param name="list">list bin or value expression of values to be added</param>
		/// <param name="indexBitCount">number of index bits expression. Must be between 4 and 16 inclusive.</param>
		/// <param name="bin">HLL bin or value expression</param>
		public static Exp Add(HLLPolicy policy, Exp list, Exp indexBitCount, Exp bin)
		{
			return Add(policy, list, indexBitCount, Exp.Val(-1), bin);
		}

		/// <summary>
		/// Create expression that adds values to a HLL set and returns HLL set. If HLL bin does not
		/// exist, use indexBitCount and minHashBitCount to create HLL set.
		/// </summary>
		/// <example>
		/// <code>
		/// // Add values to HLL bin "a" and check count > 7
		/// Exp.GT(
		///   HLLExp.GetCount(
		///     HLLExp.Add(HLLPolicy.Default, Exp.Val(list), Exp.Val(10), Exp.Val(20), Exp.HLLBin("a"))),
		///   Exp.Val(7))
		/// </code>
		/// </example>
		/// <param name="policy">write policy, use <see cref="Aerospike.Client.HLLPolicy.Default"/> for default</param>
		/// <param name="list">list bin or value expression of values to be added</param>
		/// <param name="indexBitCount">number of index bits expression. Must be between 4 and 16 inclusive.</param>
		/// <param name="minHashBitCount">number of min hash bits expression. Must be between 4 and 51 inclusive.</param>
		/// <param name="bin">HLL bin or value expression</param>
		public static Exp Add(HLLPolicy policy, Exp list, Exp indexBitCount, Exp minHashBitCount, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.ADD, list, indexBitCount, minHashBitCount, policy.flags);
			return AddWrite(bin, bytes);
		}

		/// <summary>
		/// Create expression that returns estimated number of elements in the HLL bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // HLL bin "a" count > 7
		/// Exp.GT(HLLExp.GetCount(Exp.HLLBin("a")), Exp.Val(7))
		/// </code>
		/// </example>
		public static Exp GetCount(Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.COUNT);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that returns a HLL object that is the union of all specified HLL objects
		/// in the list with the HLL bin.
		/// </summary>
		/// <example>
		/// <code>
		/// // Union of HLL bins "a" and "b"
		/// HLLExp.GetUnion(Exp.HLLBin("a"), Exp.HLLBin("b"))
		/// 
		/// // Union of local HLL list with bin "b"
		/// HLLExp.GetUnion(Exp.Val(list), Exp.HLLBin("b"))
		/// </code>
		/// </example>
		public static Exp GetUnion(Exp list, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.UNION, list);
			return AddRead(bin, bytes, Exp.Type.HLL);
		}

		/// <summary>
		/// Create expression that returns estimated number of elements that would be contained by
		/// the union of these HLL objects.
		/// </summary>
		/// <example>
		/// <code>
		/// // Union count of HLL bins "a" and "b"
		/// HLLExp.GetUnionCount(Exp.HLLBin("a"), Exp.HLLBin("b"))
		/// 
		/// // Union count of local HLL list with bin "b"
		/// HLLExp.GetUnionCount(Exp.Val(list), Exp.HLLBin("b"))
		/// </code>
		/// </example>
		public static Exp GetUnionCount(Exp list, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.UNION_COUNT, list);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that returns estimated number of elements that would be contained by
		/// the intersection of these HLL objects.
		/// </summary>
		/// <example>
		/// <code>
		/// // Intersect count of HLL bins "a" and "b"
		/// HLLExp.GetIntersectCount(Exp.HLLBin("a"), Exp.HLLBin("b"))
		/// 
		/// // Intersect count of local HLL list with bin "b"
		/// HLLExp.GetIntersectCount(Exp.Val(list), Exp.HLLBin("b"))
		/// </code>
		/// </example>
		public static Exp GetIntersectCount(Exp list, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.INTERSECT_COUNT, list);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		/// <summary>
		/// Create expression that returns estimated similarity of these HLL objects as a
		/// 64 bit float.
		/// </summary>
		/// <example>
		/// <code>
		/// // Similarity of HLL bins "a" and "b" >= 0.75
		/// Exp.GE(HLLExp.GetSimilarity(Exp.HLLBin("a"), Exp.HLLBin("b")), Exp.Val(0.75))
		/// </code>
		/// </example>
		public static Exp GetSimilarity(Exp list, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.SIMILARITY, list);
			return AddRead(bin, bytes, Exp.Type.FLOAT);
		}

		/// <summary>
		/// Create expression that returns indexBitCount and minHashBitCount used to create HLL bin
		/// in a list of longs. list[0] is indexBitCount and list[1] is minHashBitCount.
		/// </summary>
		/// <example>
		/// <code>
		/// // Bin "a" indexBitCount &lt; 10
		/// Exp.LT(
		///   ListExp.GetByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.Val(0),
		///     HLLExp.describe(Exp.HLLBin("a"))),
		///   Exp.Val(10))
		/// </code>
		/// </example>
		public static Exp Describe(Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.DESCRIBE);
			return AddRead(bin, bytes, Exp.Type.LIST);
		}

		/// <summary>
		/// Create expression that returns one if HLL bin may contain all items in the list.
		/// </summary>
		/// <example>
		/// <code>
		/// // Bin "a" may contain value "x"
		/// List list = new List();
		/// list.Add(Value.Get("x"));
		/// Exp.EQ(HLLExp.MayContain(Exp.Val(list), Exp.HLLBin("a")), Exp.Val(1));
		/// </code>
		/// </example>
		public static Exp MayContain(Exp list, Exp bin)
		{
			byte[] bytes = PackUtil.Pack(HLLOperation.MAY_CONTAIN, list);
			return AddRead(bin, bytes, Exp.Type.INT);
		}

		private static Exp AddWrite(Exp bin, byte[] bytes)
		{
			return new Exp.Module(bin, bytes, (int)Exp.Type.HLL, MODULE | Exp.MODIFY);
		}

		private static Exp AddRead(Exp bin, byte[] bytes, Exp.Type retType)
		{
			return new Exp.Module(bin, bytes, (int)retType, MODULE);
		}
	}
}
