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
namespace Aerospike.Client
{
	/// <summary>
	/// Partition filter used in scan/query.
	/// </summary>
	public sealed class PartitionFilter
	{
		/// <summary>
		/// Filter by partition id.
		/// </summary>
		/// <param name="id">partition id (0 - 4095)</param>
		public static PartitionFilter Id(int id)
		{
			return new PartitionFilter(id, 1);
		}

		/// <summary>
		/// Return records after key's digest in partition containing the digest.
		/// Note that digest order is not the same as userKey order.
		/// </summary>
		/// <param name="key">return records after this key's digest </param>
		public static PartitionFilter After(Key key)
		{
			return new PartitionFilter(key.digest);
		}

		/// <summary>
		/// Filter by partition range.
		/// </summary>
		/// <param name="begin">start partition id (0 - 4095)</param>
		/// <param name="count">number of partitions</param>
		public static PartitionFilter Range(int begin, int count)
		{
			return new PartitionFilter(begin, count);
		}

		internal readonly int begin;
		internal readonly int count;
		internal readonly byte[] digest;

		private PartitionFilter(int begin, int count)
		{
			this.begin = begin;
			this.count = count;
			this.digest = null;
		}

		private PartitionFilter(byte[] digest)
		{
			this.begin = (int)Partition.GetPartitionId(digest);
			this.count = 1;
			this.digest = digest;
		}
	}
}
