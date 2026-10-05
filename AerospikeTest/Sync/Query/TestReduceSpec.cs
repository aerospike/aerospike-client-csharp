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
			return new Record(new Dictionary<string, object> { { binName, value } }, 1, 0);
		}

		//-------------------------------------------------------
		// Top-K reducer
		//-------------------------------------------------------

		[TestMethod]
		public void TopKReturnsKLargestDescending()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 3);

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
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE, 3);

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
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 3);

			reducer.AcceptPartial(Record(Bin, 1L), Key("k1"));
			reducer.AcceptPartial(Record(Bin, 5L), Key("k2"));
			reducer.AcceptPartial(Record(Bin, 3L), Key("k3"));

			Assert.AreEqual(5L, reducer.GetScalarResult().GetLong(Bin));
		}

		[TestMethod]
		public void TopKFewerThanKMatchesReturnsAllOfThem()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 5);

			reducer.AcceptPartial(Record(Bin, 1L), Key("k1"));
			reducer.AcceptPartial(Record(Bin, 2L), Key("k2"));

			Assert.AreEqual(2, reducer.GetResult().Length);
		}

		[TestMethod]
		public void TopKDedupesSameDigestKeepingBetterValue()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 5);

			Key k1 = Key("k1");

			reducer.AcceptPartial(Record(Bin, 1L), k1);
			reducer.AcceptPartial(Record(Bin, 1L), Key("k2"));
			reducer.AcceptPartial(Record(Bin, 9L), k1);

			Record[] result = reducer.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(9L, result[0].GetLong(Bin));
			Assert.AreEqual(1L, result[1].GetLong(Bin));
		}

		[TestMethod]
		public void TopKDedupeIgnoresWorseRescan()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 5);

			Key k1 = Key("k1");

			reducer.AcceptPartial(Record(Bin, 9L), k1);
			reducer.AcceptPartial(Record(Bin, 1L), k1);

			Record[] result = reducer.GetResult();
			Assert.AreEqual(1, result.Length);
			Assert.AreEqual(9L, result[0].GetLong(Bin));
		}

		[TestMethod]
		public void TopKTiesBrokenDeterministicallyByDigest()
		{
			ReduceSpec<Record, Record> a = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2);
			a.AcceptPartial(Record(Bin, 5L), Key("alpha"));
			a.AcceptPartial(Record(Bin, 5L), Key("beta"));
			a.AcceptPartial(Record(Bin, 5L), Key("gamma"));

			ReduceSpec<Record, Record> b = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2);
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
		public void TopKCaseInsensitiveStringOrdering()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE, 3);

			reducer.AcceptPartial(Record(Bin, "banana"), Key("k1"));
			reducer.AcceptPartial(Record(Bin, "Apple"), Key("k2"));
			reducer.AcceptPartial(Record(Bin, "cherry"), Key("k3"));

			Record[] result = reducer.GetResult();
			Assert.AreEqual(3, result.Length);
			Assert.AreEqual("Apple", result[0].GetString(Bin));
			Assert.AreEqual("banana", result[1].GetString(Bin));
			Assert.AreEqual("cherry", result[2].GetString(Bin));
		}

		[TestMethod]
		public void LimitRejectsOutOfRangeValues()
		{
			Assert.Throws<ArgumentException>(() => Reduce.Limit(Bin, 0));
			Assert.Throws<ArgumentException>(() => Reduce.Limit(Bin, 1001));
		}

		[TestMethod]
		public void OrderByAloneThrowsUnsupported()
		{
			ReduceSpec<Record, Record> orderBy = Reduce.OrderBy(Bin, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE);

			Assert.Throws<NotSupportedException>(() => orderBy.AcceptPartial(Record(Bin, 1L), Key("k1")));
		}

		[TestMethod]
		public void LimitAloneThrowsUnsupported()
		{
			ReduceSpec<Record, Record> limit = Reduce.Limit(Bin, 5);

			Assert.Throws<NotSupportedException>(() => limit.AcceptPartial(Record(Bin, 1L), Key("k1")));
		}

		[TestMethod]
		public void SetTopKWithoutSetOrderByThrows()
		{
			Statement stmt = new();

			Assert.Throws<InvalidOperationException>(() => stmt.SetTopK(5));
		}

		[TestMethod]
		public void TopKWithKOne()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 1);

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
			Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 1000);
		}

		[TestMethod]
		public void TopKScalarResultBeforeAnyAcceptThrows()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 3);

			Assert.Throws<InvalidOperationException>(() => reducer.GetScalarResult());
		}

		[TestMethod]
		public void TopKDescTiesOrderedByDigestAscending()
		{
			ReduceSpec<Record, Record> asc = Reduce.TopK(Bin, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE, 3);
			ReduceSpec<Record, Record> desc = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 3);

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
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.BYTES, Order.ASC, OrderByFlags.NONE, 2);

			reducer.AcceptPartial(Record(Bin, new byte[] { 0x01, 0x02 }), Key("k1"));
			reducer.AcceptPartial(Record(Bin, new byte[] { 0x01 }), Key("k2"));
			reducer.AcceptPartial(Record(Bin, new byte[] { 0xff }), Key("k3"));

			Record[] result = reducer.GetResult();
			Assert.AreEqual(2, result.Length);
			Assert.AreEqual(1, ((byte[])result[0].GetValue(Bin)).Length);
			Assert.AreEqual(2, ((byte[])result[1].GetValue(Bin)).Length);
		}

		[TestMethod]
		public void TopKCaseSensitiveStringOrdering()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.STRING, Order.ASC, OrderByFlags.NONE, 3);

			reducer.AcceptPartial(Record(Bin, "banana"), Key("k1"));
			reducer.AcceptPartial(Record(Bin, "Apple"), Key("k2"));
			reducer.AcceptPartial(Record(Bin, "Cherry"), Key("k3"));

			Record[] result = reducer.GetResult();
			Assert.AreEqual("Apple", result[0].GetString(Bin));
			Assert.AreEqual("Cherry", result[1].GetString(Bin));
			Assert.AreEqual("banana", result[2].GetString(Bin));
		}

		[TestMethod]
		public void TopKIsThreadSafeUnderConcurrentAccept()
		{
			ReduceSpec<Record, Record> reducer = Reduce.TopK(Bin, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 5);
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
