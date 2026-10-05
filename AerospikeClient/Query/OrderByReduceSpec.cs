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
	/// Sort-metadata-only reduce spec produced by
	/// <see cref="Reduce.OrderBy(string, BinDataType, Order, OrderByFlags)"/>.
	/// <para>
	/// Carries no combiner logic by itself. <see cref="Statement.SetReduce"/> pairs this
	/// with a <see cref="LimitReduceSpec"/> on the same bin and composes them into a
	/// <see cref="TopKReduceSpec"/> via <see cref="TopKReduceSpec.Compose"/>. Not part of the
	/// public API surface beyond the <see cref="Reduce.OrderBy"/> factory method.
	/// </para>
	/// </summary>
	internal sealed class OrderByReduceSpec : ReduceSpec<Record, Record>
	{
		internal readonly string bin;
		internal readonly BinDataType type;
		internal readonly Order order;
		internal readonly OrderByFlags flags;

		internal OrderByReduceSpec(string bin, BinDataType type, Order order, OrderByFlags flags)
		{
			this.bin = bin;
			this.type = type;
			this.order = order;
			this.flags = flags;
		}

		public void AcceptPartial(Record record, Key key)
		{
			throw new NotSupportedException("orderBy must be combined with limit (see Reduce.TopK)");
		}

		public Record GetScalarResult()
		{
			throw new NotSupportedException("orderBy must be combined with limit (see Reduce.TopK)");
		}

		public Record[] GetResult()
		{
			throw new NotSupportedException("orderBy must be combined with limit (see Reduce.TopK)");
		}
	}
}
