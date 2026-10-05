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
	/// Client-side global reduce combiner for query results.
	/// <para>
	/// Each cluster node produces a partial result for a query (e.g. a locally bounded Top-K
	/// heap). The query executor feeds every node's partial results into
	/// <see cref="AcceptPartial(TInput, Key)"/> in any order (nodes complete concurrently), then reads
	/// the merged result via <see cref="GetScalarResult"/> or <see cref="GetResult"/>.
	/// </para>
	/// <para>
	/// Instances are created via <see cref="Reduce"/> factory methods and passed to
	/// <see cref="Statement.SetReduce(ReduceSpec{Record, Record}[])"/>.
	/// </para>
	/// </summary>
	/// <typeparam name="TInput">
	/// Type of each partial accepted by <see cref="AcceptPartial(TInput, Key)"/>
	/// (usually <see cref="Record"/>).
	/// </typeparam>
	/// <typeparam name="TResult">Element type returned by <see cref="GetResult"/>.</typeparam>
	public interface ReduceSpec<TInput, TResult>
	{
		/// <summary>
		/// Merge one partial result from a node into this combiner.
		/// </summary>
		/// <param name="record">partial result (usually a <see cref="Record"/>)</param>
		/// <param name="key">record key; used for digest-based deduplication and tie-breaking</param>
		void AcceptPartial(TInput record, Key key);

		/// <summary>
		/// Return a single scalar view of the merged result (e.g. the best-ranked record for a
		/// Top-K reduce).
		/// </summary>
		TResult GetScalarResult();

		/// <summary>
		/// Return the full merged result set (e.g. all Top-K records in order).
		/// </summary>
		TResult[] GetResult();
	}
}
