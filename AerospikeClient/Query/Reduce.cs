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
	/// Internal until a public aggregation API exists; use
	/// <see cref="Statement.SetOrderBy(string, BinDataType, Order)"/> /
	/// <see cref="Statement.ThenOrderBy(string, BinDataType, Order)"/> /
	/// <see cref="Statement.SetTopK(int)"/>.
	/// </summary>
	internal static class Reduce
	{
		/// <summary>
		/// Ordered LIMIT k reduce; returns up to <paramref name="k"/> full records in global sort order.
		/// </summary>
		internal static ReduceSpec<Record, Record> TopK(string bin, BinDataType type, Order order, OrderByFlags flags, int k)
		{
			return TopKReduceSpec.Compose(OrderBy(bin, type, order, flags), Limit(bin, k));
		}

		/// <summary>
		/// Ordered LIMIT k reduce with a primary and secondary (tie-breaker) order key.
		/// </summary>
		internal static ReduceSpec<Record, Record> TopK(
			string bin,
			BinDataType type,
			Order order,
			OrderByFlags flags,
			string thenBin,
			BinDataType thenType,
			Order thenOrder,
			OrderByFlags thenFlags,
			int k)
		{
			return TopKReduceSpec.Create(bin, type, order, flags, thenBin, thenType, thenOrder, thenFlags, k);
		}

		/// <summary>
		/// Sort-order building block for single-key Top-K. Must be paired with
		/// <see cref="Limit(string, int)"/> on the same bin via
		/// <see cref="Statement.SetReduce(ReduceSpec{Record, Record}[])"/>.
		/// </summary>
		internal static ReduceSpec<Record, Record> OrderBy(string bin, BinDataType type, Order order, OrderByFlags flags)
		{
			return new OrderByReduceSpec(bin, type, order, flags);
		}

		/// <summary>
		/// Cap building block for single-key Top-K. Must be paired with
		/// <see cref="OrderBy(string, BinDataType, Order, OrderByFlags)"/> on the same bin via
		/// <see cref="Statement.SetReduce(ReduceSpec{Record, Record}[])"/>.
		/// </summary>
		internal static ReduceSpec<Record, Record> Limit(string bin, int limit)
		{
			return new LimitReduceSpec(bin, limit);
		}
	}
}
