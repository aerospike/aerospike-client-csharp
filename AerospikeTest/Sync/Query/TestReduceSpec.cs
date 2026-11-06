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
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Pure client-side unit tests for the ReduceSpec / Reduce / Statement map-reduce framework.
	/// These tests do not require a running Aerospike cluster.
	/// </summary>
	[TestClass]
	public class TestReduceSpec
	{
		private const string Ns = "test";
		private const string Set = "reduceset";
		private const string Bin = "val";

		private static Key Key(string userKey)
		{
			return new Key(Ns, Set, userKey);
		}

		private static Record Record(string binName, object value)
		{
			return Record(binName, value, 1);
		}

		private static Record Record(string binName, object value, int generation)
		{
			return new Record(new Dictionary<string, object> { { binName, value } }, generation, 0);
		}

		//-------------------------------------------------------
		// Top-K reducer
		//-------------------------------------------------------

		[TestMethod]
		public void TopKReturnsKLargestDescending()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 3);

			for (int i = 1; i <= 10; i++)
			{
				reducer.AcceptPartial(Record(Bin, (long)i), Key("k" + i));
			}

			Record[] result = reducer.GetResult();
			Assert.AreEqual(3, result.Length);
			Assert.AreEqual(10L, result[0].GetLong(Bin));
			Assert.AreEqual(9L, result[1].GetLong(Bin));
			Assert.AreEqual(8L, result[2].GetLong(Bin));
		}

		[TestMethod]
		public void TopKReturnsKSmallestAscending()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.ASC, 3);

			for (int i = 1; i <= 10; i++)
			{
				reducer.AcceptPartial(Record(Bin, (long)i), Key("k" + i));
			}

			Record[] result = reducer.GetResult();
			Assert.AreEqual(3, result.Length);
			Assert.AreEqual(1L, result[0].GetLong(Bin));
			Assert.AreEqual(2L, result[1].GetLong(Bin));
			Assert.AreEqual(3L, result[2].GetLong(Bin));
		}

		[TestMethod]
		public void TopKScalarResultIsBestRanked()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 3);

			reducer.AcceptPartial(Record(Bin, 1L), Key("k1"));
			reducer.AcceptPartial(Record(Bin, 5L), Key("k2"));
			reducer.AcceptPartial(Record(Bin, 3L), Key("k3"));

			Assert.AreEqual(5L, reducer.GetScalarResult().GetLong(Bin));
		}

		[TestMethod]
		public void TopKFewerThanKMatchesReturnsAllOfThem()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 5);

			reducer.AcceptPartial(Record(Bin, 1L), Key("k1"));
			reducer.AcceptPartial(Record(Bin, 2L), Key("k2"));

			Assert.AreEqual(2, reducer.GetResult().Length);
		}

		[TestMethod]
		public void TopKDedupesSameDigestKeepingLaterLastUpdateTime()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 5);

			Key k1 = Key("k1");

			reducer.AcceptPartial(Record(Bin, 1L, 1), k1, 100);
			reducer.AcceptPartial(Record(Bin, 1L, 1), Key("k2"), 100);
			// Later last-update time replaces even when the order key is worse for DESC.
			reducer.AcceptPartial(Record(Bin, 0L, 2), k1, 200);

			Record[] result = reducer.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(1L, result[0].GetLong(Bin));
			Assert.AreEqual(0L, result[1].GetLong(Bin));
			Assert.AreEqual(2, result[1].generation);
		}

		[TestMethod]
		public void TopKDedupeIgnoresEarlierLastUpdateTimeEvenIfBetterRanked()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 5);

			Key k1 = Key("k1");

			reducer.AcceptPartial(Record(Bin, 1L, 3), k1, 200);
			// Earlier last-update time with a "better" DESC value must not win (R29).
			reducer.AcceptPartial(Record(Bin, 9L, 1), k1, 100);

			Record[] result = reducer.GetResult();
			Assert.AreEqual(1, result.Length);
			Assert.AreEqual(1L, result[0].GetLong(Bin));
			Assert.AreEqual(3, result[0].generation);
		}

		[TestMethod]
		public void TopKDedupeEqualLastUpdateTimeKeepsBetterRanked()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 5);

			Key k1 = Key("k1");

			reducer.AcceptPartial(Record(Bin, 1L), k1, 100);
			// Equal LUT: better-ranked DESC value wins (also covers client-side reduce with LUT 0).
			reducer.AcceptPartial(Record(Bin, 9L), k1, 100);

			Record[] result = reducer.GetResult();
			Assert.AreEqual(1, result.Length);
			Assert.AreEqual(9L, result[0].GetLong(Bin));
		}

		[TestMethod]
		public void TopKDedupeRemembersLutAfterEviction()
		{
			// k=1: admit a newer, worse-ranked copy of digest D so it occupies the heap,
			// then admit a better-ranked unrelated record that evicts D. An older copy of D
			// must not re-enter (R29), even though byDigest no longer tracks D.
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 1);

			Key d = Key("dup");
			reducer.AcceptPartial(Record(Bin, 1L, 1), d, 200);
			reducer.AcceptPartial(Record(Bin, 100L, 1), Key("winner"), 100);
			reducer.AcceptPartial(Record(Bin, 50L, 1), d, 100);

			Record[] result = reducer.GetResult();
			Assert.AreEqual(1, result.Length);
			Assert.AreEqual(100L, result[0].GetLong(Bin));
		}

		[TestMethod]
		public void TopKNegativeZeroEqualsPositiveZero()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.DOUBLE, Order.ASC, 2);

			Key neg = Key("neg");
			Key pos = Key("pos");
			Key one = Key("one");

			reducer.AcceptPartial(Record(Bin, -0.0), neg);
			reducer.AcceptPartial(Record(Bin, 0.0), pos);
			reducer.AcceptPartial(Record(Bin, 1.0), one);

			Record[] result = reducer.GetResult();
			Assert.AreEqual(2, result.Length);
			// −0.0 and 0.0 tie; digest ascending decides which comes first.
			Assert.AreEqual(0.0, result[0].GetDouble(Bin));
			Assert.AreEqual(0.0, result[1].GetDouble(Bin));

			TopKReduceSpec typed = (TopKReduceSpec)reducer;
			Key[] keys = typed.GetResultKeys();
			Assert.AreEqual(2, keys.Length);
			int digestCmp = CompareDigests(keys[0].digest, keys[1].digest);
			Assert.IsTrue(digestCmp < 0, "tied zeros must sort by digest ascending");
		}

		private static int CompareDigests(byte[] a, byte[] b)
		{
			int len = Math.Min(a.Length, b.Length);

			for (int i = 0; i < len; i++)
			{
				int cmp = (a[i] & 0xff) - (b[i] & 0xff);

				if (cmp != 0)
				{
					return cmp;
				}
			}
			return a.Length - b.Length;
		}

		[TestMethod]
		public void TopKIntegerAcceptsBoxedInt()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 2);

			reducer.AcceptPartial(Record(Bin, 1), Key("k1"));
			reducer.AcceptPartial(Record(Bin, 9), Key("k2"));
			reducer.AcceptPartial(Record(Bin, 5), Key("k3"));

			Record[] result = reducer.GetResult();
			Assert.AreEqual(2, result.Length);
			// Hand-built records store boxed int; assert via GetValue (GetLong expects long).
			Assert.AreEqual(9, result[0].GetValue(Bin));
			Assert.AreEqual(5, result[1].GetValue(Bin));
		}

		[TestMethod]
		public void TopKThenOrderByBreaksPrimaryTies()
		{
			const string thenBin = "ts";
			ReduceSpec<Record, Record> reducer = Reduce.TopK(
				Bin, BinDataType.INTEGER, Order.DESC,
				thenBin, BinDataType.INTEGER, Order.ASC,
				2);

			reducer.AcceptPartial(RecordTwo(Bin, 10L, thenBin, 30L), Key("late"));
			reducer.AcceptPartial(RecordTwo(Bin, 10L, thenBin, 10L), Key("early"));
			reducer.AcceptPartial(RecordTwo(Bin, 10L, thenBin, 20L), Key("mid"));
			reducer.AcceptPartial(RecordTwo(Bin, 5L, thenBin, 1L), Key("low"));

			Record[] result = reducer.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(10L, result[0].GetLong(Bin));
			Assert.AreEqual(10L, result[0].GetLong(thenBin));
			Assert.AreEqual(10L, result[1].GetLong(Bin));
			Assert.AreEqual(20L, result[1].GetLong(thenBin));
		}

		[TestMethod]
		public void TopKTwoKeyRejectsOutOfRangeLimit()
		{
			AerospikeException ex = Assert.Throws<AerospikeException>(() => Reduce.TopK(
				Bin, BinDataType.INTEGER, Order.DESC,
				"ts", BinDataType.INTEGER, Order.ASC,
				0));
			Assert.AreEqual(ResultCode.PARSE_ERROR, ex.Result);
		}

		private static Record RecordTwo(string binA, object valueA, string binB, object valueB)
		{
			return new Record(new Dictionary<string, object>
			{
				{ binA, valueA },
				{ binB, valueB }
			}, 1, 0);
		}

		[TestMethod]
		public void TopKTiesBrokenDeterministicallyByDigest()
		{
			ReduceSpec<Record, Record> a = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 2);
			a.AcceptPartial(Record(Bin, 5L), Key("alpha"));
			a.AcceptPartial(Record(Bin, 5L), Key("beta"));
			a.AcceptPartial(Record(Bin, 5L), Key("gamma"));

			ReduceSpec<Record, Record> b = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 2);
			b.AcceptPartial(Record(Bin, 5L), Key("gamma"));
			b.AcceptPartial(Record(Bin, 5L), Key("alpha"));
			b.AcceptPartial(Record(Bin, 5L), Key("beta"));

			Record[] ra = a.GetResult();
			Record[] rb = b.GetResult();
			Assert.AreEqual(ra.Length, rb.Length);

			for (int i = 0; i < ra.Length; i++)
			{
				Assert.AreEqual(ra[i].GetLong(Bin), rb[i].GetLong(Bin));
			}
		}

		[TestMethod]
		public void LimitRejectsOutOfRangeValues()
		{
			Assert.Throws<AerospikeException>(() => Reduce.Limit(Bin, 0));
			Assert.Throws<AerospikeException>(() => Reduce.Limit(Bin, 1001));
		}

		[TestMethod]
		public void OrderByAloneThrowsUnsupported()
		{
			ReduceSpec<Record, Record> orderBy = Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.ASC);

			Assert.Throws<AerospikeException>(() => orderBy.AcceptPartial(Record(Bin, 1L), Key("k1")));
		}

		[TestMethod]
		public void LimitAloneThrowsUnsupported()
		{
			ReduceSpec<Record, Record> limit = Reduce.Limit(Bin, 5);

			Assert.Throws<AerospikeException>(() => limit.AcceptPartial(Record(Bin, 1L), Key("k1")));
		}

		[TestMethod]
		public void SetTopKWithoutSetOrderByArmsWithoutResolving()
		{
			Statement stmt = new();
			stmt.SetTopK(5);

			Assert.IsTrue(stmt.HasTopK);
			Assert.IsFalse(stmt.HasOrderBy);
			Assert.AreEqual(5, stmt.TopKLimit);
			Assert.IsNull(Internals.ResolveReduce(stmt));
		}

		[TestMethod]
		public void TopKWithKOne()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 1);

			for (int i = 1; i <= 5; i++)
			{
				reducer.AcceptPartial(Record(Bin, (long)i), Key("k" + i));
			}

			Record[] result = reducer.GetResult();
			Assert.AreEqual(1, result.Length);
			Assert.AreEqual(5L, result[0].GetLong(Bin));
			Assert.AreSame(result[0], reducer.GetScalarResult());
		}

		[TestMethod]
		public void TopKUpperBoundKThousandAccepted()
		{
			Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 1000);
		}

		[TestMethod]
		public void TopKScalarResultBeforeAnyAcceptThrows()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 3);

			Assert.Throws<InvalidOperationException>(() => reducer.GetScalarResult());
		}

		[TestMethod]
		public void TopKDescTiesOrderedByDigestAscending()
		{
			ReduceSpec<Record, Record> asc = Reduce.TopK(Bin, BinDataType.INTEGER, Order.ASC, 3);
			ReduceSpec<Record, Record> desc = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 3);

			string[] userKeys = { "alpha", "beta", "gamma", "delta", "epsilon" };

			foreach (string uk in userKeys)
			{
				Record shared = Record(Bin, 5L);
				asc.AcceptPartial(shared, Key(uk));
				desc.AcceptPartial(shared, Key(uk));
			}

			Record[] ra = asc.GetResult();
			Record[] rd = desc.GetResult();
			Assert.AreEqual(3, ra.Length);
			Assert.AreEqual(3, rd.Length);

			for (int i = 0; i < ra.Length; i++)
			{
				Assert.AreSame(ra[i], rd[i]);
			}
		}

		[TestMethod]
		public void TopKBytesOrdering()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.BYTES, Order.ASC, 2);

			reducer.AcceptPartial(Record(Bin, new byte[] { 0x01, 0x02 }), Key("k1"));
			reducer.AcceptPartial(Record(Bin, new byte[] { 0x01 }), Key("k2"));
			reducer.AcceptPartial(Record(Bin, new byte[] { 0xff }), Key("k3"));

			Record[] result = reducer.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(1, ((byte[])result[0].GetValue(Bin)).Length);
			Assert.AreEqual(2, ((byte[])result[1].GetValue(Bin)).Length);
		}

		[TestMethod]
		public void TopKIsThreadSafeUnderConcurrentAccept()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, 5);
			int threads = 8;
			int perThread = 20000;
			long total = (long)threads * perThread;
			Exception failure = null;
			object failureLock = new();

			Barrier barrier = new(threads);
			Thread[] workers = new Thread[threads];

			for (int t = 0; t < threads; t++)
			{
				int idx = t;
				workers[t] = new Thread(() =>
				{
					try
					{
						barrier.SignalAndWait();
						for (int i = 0; i < perThread; i++)
						{
							long val = (long)idx * perThread + i;
							reducer.AcceptPartial(Record(Bin, val), Key("t" + idx + "-k" + i));
						}
					}
					catch (Exception e)
					{
						lock (failureLock)
						{
							failure ??= e;
						}
					}
				});
				workers[t].Start();
			}

			foreach (Thread thread in workers)
			{
				thread.Join();
			}

			if (failure != null)
			{
				Assert.Fail("Concurrent AcceptPartial threw: " + failure);
			}

			Record[] result = reducer.GetResult();
			Assert.AreEqual(5, result.Length);
			for (int i = 0; i < 5; i++)
			{
				Assert.AreEqual(total - 1 - i, result[i].GetLong(Bin));
			}
		}
	}
}
