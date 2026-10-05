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
	/// Vector expression generator. See <see cref="Aerospike.Client.Exp"/>.
	/// </summary>
	public sealed class VectorExp
	{
		/// <summary>
		/// Create expression that returns the distance between a stored vector bin and a query
		/// vector as a 64 bit float, using the given distance metric.
		/// <para>
		/// The query vector's element type and dimension count must match the stored vector.
		/// </para>
		/// </summary>
		/// <example>
		/// <code>
		/// // Records whose "embedding" vector is within cosine distance threshold of a query
		/// Vector query = Vector.OfFloat32(queryEmbedding);
		/// Exp.GT(
		///   VectorExp.Distance(VectorDistanceMetric.COSINE, query, Exp.VectorBin("embedding")),
		///   Exp.Val(0.8))
		/// </code>
		/// </example>
		/// <param name="metric">distance metric used to compare the vectors</param>
		/// <param name="query">query vector compared against the stored vector bin</param>
		/// <param name="bin">vector bin read, typically <see cref="Exp.VectorBin(string)"/></param>
		public static Exp Distance(VectorDistanceMetric metric, Vector query, Exp bin)
		{
			if (query is null)
			{
				throw new ArgumentNullException(nameof(query));
			}
			return new Exp.VectorDist(Exp.VectorDistOpcode(metric), query, bin);
		}

		private VectorExp()
		{
		}
	}
}
