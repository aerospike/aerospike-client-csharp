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
namespace Aerospike.Client
{
	/// <summary>
	/// Factory for <see cref="ReduceSpec{TInput, TResult}"/> instances used with
	/// <see cref="Statement.SetReduce(ReduceSpec{Record, Record}[])"/>.
	/// <para>
	/// Supports ordered LIMIT k reduces (<see cref="TopK"/>, or <see cref="OrderBy"/> +
	/// <see cref="Limit"/> passed together) - returns up to k full records, globally ordered.
	/// </para>
	/// </summary>
	public static class Reduce
	{
		/// <summary>
		/// Ordered LIMIT k reduce; returns up to <paramref name="k"/> full records in global sort order
		/// (e.g. vector search Top-K, <c>ORDER BY bin LIMIT k</c>).
		/// </summary>
		/// <param name="bin">
		/// bin name to order by; must be a physical bin or a projected bin from
		/// <see cref="Statement.Operations"/>
		/// </param>
		/// <param name="type">scalar type of <paramref name="bin"/></param>
		/// <param name="order">sort direction</param>
		/// <param name="flags">
		/// comparison options (<see cref="OrderByFlags.CASE_INSENSITIVE"/> for
		/// <see cref="BinDataType.STRING"/> only)
		/// </param>
		/// <param name="k">maximum number of records to return, in [1, 1000]</param>
		public static ReduceSpec<Record, Record> TopK(string bin, BinDataType type, Order order, OrderByFlags flags, int k)
		{
			return TopKReduceSpec.Compose(OrderBy(bin, type, order, flags), Limit(bin, k));
		}

		/// <summary>
		/// Sort-order building block for <see cref="TopK"/>. Must be paired with
		/// <see cref="Limit(string, int)"/> on the same bin via
		/// <see cref="Statement.SetReduce(ReduceSpec{Record, Record}[])"/>; using it alone throws
		/// <see cref="NotSupportedException"/>.
		/// </summary>
		public static ReduceSpec<Record, Record> OrderBy(string bin, BinDataType type, Order order, OrderByFlags flags)
		{
			return new OrderByReduceSpec(bin, type, order, flags);
		}

		/// <summary>
		/// Cap building block for <see cref="TopK"/>. Must be paired with
		/// <see cref="OrderBy(string, BinDataType, Order, OrderByFlags)"/> on the same bin via
		/// <see cref="Statement.SetReduce(ReduceSpec{Record, Record}[])"/>; using it alone throws
		/// <see cref="NotSupportedException"/>.
		/// </summary>
		/// <param name="bin">bin name; must match the paired <c>OrderBy</c> bin</param>
		/// <param name="limit">maximum number of records to return, in [1, 1000]</param>
		public static ReduceSpec<Record, Record> Limit(string bin, int limit)
		{
			return new LimitReduceSpec(bin, limit);
		}
	}
}
