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
	/// Cap-metadata-only reduce spec produced by <see cref="Reduce.Limit(string, int)"/>.
	/// <para>
	/// Carries no combiner logic by itself. <see cref="Statement.SetReduce"/> pairs this
	/// with an <see cref="OrderByReduceSpec"/> on the same bin and composes them into a
	/// <see cref="TopKReduceSpec"/> via <see cref="TopKReduceSpec.Compose"/>. Not part
	/// of the public API surface beyond the <see cref="Reduce.Limit"/> factory method.
	/// </para>
	/// </summary>
	internal sealed class LimitReduceSpec : ReduceSpec<Record, Record>
	{
		internal readonly string bin;
		internal readonly int limit;

		internal LimitReduceSpec(string bin, int limit)
		{
			if (limit < 1 || limit > 1000)
			{
				throw new ArgumentException("limit must be in [1, 1000], got: " + limit);
			}
			this.bin = bin;
			this.limit = limit;
		}

		public void AcceptPartial(Record record, Key key)
		{
			throw new NotSupportedException("limit must be combined with orderBy (see Reduce.TopK)");
		}

		public Record GetScalarResult()
		{
			throw new NotSupportedException("limit must be combined with orderBy (see Reduce.TopK)");
		}

		public Record[] GetResult()
		{
			throw new NotSupportedException("limit must be combined with orderBy (see Reduce.TopK)");
		}
	}
}
