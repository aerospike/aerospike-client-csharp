/* 
 * Copyright 2012-2022 Aerospike, Inc.
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

namespace Aerospike.Client
{
	/// <summary>
	/// Partition filter used in scan/query. This filter is also used as a cursor.
	/// <para>
	/// If a previous scan/query returned all records specified by a PartitionFilter instance, a
	/// future scan/query using the same PartitionFilter instance will only return new records added
	/// after the last record read (in digest order) in each partition in the previous scan/query.
	/// To reset the cursor of an existing PartitionFilter instance, set
	/// <see cref="PartitionFilter.Partitions"/> to null.
	/// </para>
	/// </summary>
	[Serializable]
	public sealed class PartitionFilter
	{
		/// <summary>
		/// Read all partitions.
		/// </summary>
		public static PartitionFilter All()
		{
			return new PartitionFilter(0, 4096);
		}

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
		/// Note that digest order is not the same as userKey order. This method
		/// only works for scan or query with null filter.
		/// </summary>
		/// <param name="key">return records after this key's digest</param>
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
		internal PartitionStatus[] partitions; // Initialized in PartitionTracker.
		internal bool done;
		internal bool retry;

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

		/// <summary>
		/// First partition id.
		/// </summary>
		public int Begin
		{
			get { return begin; }
		}

		/// <summary>
		/// Count of partitions.
		/// </summary>
		public int Count
		{
			get { return count; }
		}

		/// <summary>
		/// Resume after digest.
		/// </summary>
		public byte[] Digest
		{
			get { return digest; }
		}

		/// <summary>
		/// Cursor status of each partition after scan termination. The cursor contains the last record
		/// read for each partition and is usually obtained from Partitions after a previous scan/query.
		/// This is useful for a retry of a partially completed scan/query.
		/// <para>
		/// If a previous scan/query returned all records specified by a PartitionFilter instance, a
		/// future scan/query using the same PartitionFilter instance will only return new records added
		/// after the last record read (in digest order) in each partition in the previous scan/query.
		/// To reset the cursor of an existing PartitionFilter instance, set Partitions to null.
		/// </para>
		/// <para>
		/// The partition status is accurate for sync/async ScanPartitions and async QueryPartitions.
		/// </para>
		/// <para>
		/// The partition status may not be accurate for
		/// <see cref="Aerospike.Client.AerospikeClient.QueryPartitions(QueryPolicy, Statement, PartitionFilter)"/>
		/// because the last digest received is set during query parsing, but the user may not have
		/// retrieved that digest from the RecordSet yet.
		/// </para>
		/// </summary>
		public PartitionStatus[] Partitions
		{
			get { return partitions; }
			set { this.partitions = value; }
		}

		/// <summary>
		/// If using <see cref="Aerospike.Client.ScanPolicy.maxRecords"/> or
		/// <see cref="Aerospike.Client.QueryPolicy.maxRecords"/>,
		/// did previous paginated scans with this partition filter instance return all records?
		/// </summary>
		public bool Done
		{
			get { return done; }
		}
	}
}
