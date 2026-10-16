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
	/// Ordered LIMIT k reduce combiner backing
	/// <see cref="Reduce.TopK(string, BinDataType, Order, OrderByFlags, int)"/>
	/// and the composed split form <c>SetReduce(Reduce.OrderBy(...), Reduce.Limit(...))</c>.
	/// </summary>
	internal sealed class TopKReduceSpec : ReduceSpec<Record, Record>
	{
		private static readonly IComparer<OrderKey> WorstFirst = Comparer<OrderKey>.Create((a, b) => b.CompareTo(a));
		private static readonly DigestEqualityComparer DigestComparer = new();

		private readonly string bin;
		private readonly BinDataType type;
		private readonly Order order;
		private readonly OrderByFlags flags;
		private readonly string thenBin;
		private readonly BinDataType thenType;
		private readonly Order thenOrder;
		private readonly OrderByFlags thenFlags;
		private readonly int limit;

		/// <summary>digest -> (key, record), for dedup and result reconstruction.</summary>
		private readonly Dictionary<byte[], Entry> byDigest = new(DigestComparer);

		/// <summary>Worst-first set: Min is evicted first when the set exceeds <see cref="limit"/>.</summary>
		private readonly SortedSet<OrderKey> heap = new(WorstFirst);

		private readonly object sync = new();

		private sealed class DigestEqualityComparer : IEqualityComparer<byte[]>
		{
			public bool Equals(byte[] x, byte[] y) => Util.ByteArrayEquals(x, y);

			public int GetHashCode(byte[] obj)
			{
				if (obj == null)
				{
					return 0;
				}

				int h = 1;
				foreach (byte b in obj)
				{
					h = 31 * h + b;
				}
				return h;
			}
		}

		private sealed class Entry
		{
			internal readonly Key key;
			internal readonly Record record;
			internal readonly OrderKey orderKey;

			internal Entry(Key key, Record record, OrderKey orderKey)
			{
				this.key = key;
				this.record = record;
				this.orderKey = orderKey;
			}
		}

		private TopKReduceSpec(
			string bin,
			BinDataType type,
			Order order,
			OrderByFlags flags,
			string thenBin,
			BinDataType thenType,
			Order thenOrder,
			OrderByFlags thenFlags,
			int limit)
		{
			this.bin = bin;
			this.type = type;
			this.order = order;
			this.flags = flags;
			this.thenBin = thenBin;
			this.thenType = thenType;
			this.thenOrder = thenOrder;
			this.thenFlags = thenFlags;
			this.limit = limit;
		}

		/// <summary>
		/// Compose an <see cref="OrderByReduceSpec"/> and a <see cref="LimitReduceSpec"/> on the same bin into a
		/// single <see cref="TopKReduceSpec"/> combiner.
		/// </summary>
		internal static TopKReduceSpec Compose(ReduceSpec<Record, Record> orderBy, ReduceSpec<Record, Record> limit)
		{
			if (orderBy is not OrderByReduceSpec ob || limit is not LimitReduceSpec lim)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"topK requires one orderBy spec and one limit spec");
			}

			if (!ob.bin.Equals(lim.bin))
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"orderBy bin (" + ob.bin + ") must match limit bin (" + lim.bin + ")");
			}
			return Create(ob.bin, ob.type, ob.order, ob.flags, null, default, default, default, lim.limit);
		}

		internal static TopKReduceSpec Create(
			string bin,
			BinDataType type,
			Order order,
			OrderByFlags flags,
			string thenBin,
			BinDataType thenType,
			Order thenOrder,
			OrderByFlags thenFlags,
			int limit)
		{
			if (limit < 1 || limit > 1000)
			{
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"limit must be in [1, 1000], got: " + limit);
			}

			return new TopKReduceSpec(bin, type, order, flags, thenBin, thenType, thenOrder, thenFlags, limit);
		}

		public void AcceptPartial(Record record, Key key)
		{
			lock (sync)
			{
				byte[] digest = key.digest;
				OrderKey candidate = MakeOrderKey(record, digest);

				// Dedup: at most one row per digest (partition migration may re-scan a record).
				// PRD R29: keep the most recent version, not the better-ranked copy.
				// Generation is used until the server includes last-update time on query records
				// (drift #12); then switch to LUT comparison.
				if (byDigest.TryGetValue(digest, out Entry existingEntry))
				{
					if (record.generation < existingEntry.record.generation)
					{
						return; // stale rescan
					}

					if (record.generation == existingEntry.record.generation)
					{
						return; // same version already held
					}

					heap.Remove(existingEntry.orderKey);
					byDigest.Remove(digest);
				}

				heap.Add(candidate);
				byDigest[digest] = new Entry(key, record, candidate);

				if (heap.Count > limit)
				{
					OrderKey evicted = heap.Min;
					heap.Remove(evicted);
					byDigest.Remove(evicted.digest);
				}
			}
		}

		private OrderKey MakeOrderKey(Record record, byte[] digest)
		{
			if (thenBin == null)
			{
				return new OrderKey(record, bin, type, order, flags, digest);
			}

			OrderKey secondary = new(record, thenBin, thenType, thenOrder, thenFlags, digest);
			return new OrderKey(record, bin, type, order, flags, digest, secondary);
		}

		public Record GetScalarResult()
		{
			lock (sync)
			{
				Record[] all = GetResultUnlocked();

				if (all.Length == 0)
				{
					throw new InvalidOperationException(
						"GetScalarResult() called before any records were accepted via AcceptPartial()");
				}
				return all[0];
			}
		}

		public Record[] GetResult()
		{
			lock (sync)
			{
				return GetResultUnlocked();
			}
		}

		/// <summary>
		/// Return the record keys corresponding to <see cref="GetResult"/>, in the same order.
		/// </summary>
		internal Key[] GetResultKeys()
		{
			lock (sync)
			{
				OrderKey[] list = SortedBestFirst();
				Key[] output = new Key[list.Length];

				for (int i = 0; i < list.Length; i++)
				{
					output[i] = byDigest[list[i].digest].key;
				}
				return output;
			}
		}

		private Record[] GetResultUnlocked()
		{
			OrderKey[] list = SortedBestFirst();
			Record[] output = new Record[list.Length];

			for (int i = 0; i < list.Length; i++)
			{
				output[i] = byDigest[list[i].digest].record;
			}
			return output;
		}

		private OrderKey[] SortedBestFirst()
		{
			OrderKey[] list = new OrderKey[heap.Count];
			heap.CopyTo(list);
			Array.Sort(list); // best first (natural OrderKey order).
			return list;
		}
	}
}
