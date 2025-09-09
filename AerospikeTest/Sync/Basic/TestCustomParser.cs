/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
	[TestClass]
	public class TestCustomParser : TestSync
	{
		private delegate void ParseBin(byte[] buffer);

		private class BinParser(TestCustomParser.ParseBin parseBin) : IRecordParser
		{
			private readonly ParseBin parseBin = parseBin;

			public Record ParseRecord(byte[] dataBuffer, ref int dataOffset, int opCount, int generation, int expiration, bool isOperation)
			{
				dataOffset = RecordParser.ExtractBinValue(
					dataBuffer,
					dataOffset,
					out string binName,
					out byte valueType,
					out int valueOffset,
					out int valueSize);

				this.parseBin(dataBuffer.Skip(valueOffset).Take(valueSize).ToArray());
				return new Record(null, generation, expiration);
			}
		}

		private static Record Get(Key key, ParseBin parseBin)
		{
			var policy = new Policy() { recordParser = new BinParser(parseBin) };
			return client.Get(policy, key);
		}

		[TestMethod]
		public void UnpackMap()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "customparser");
			var map = new Dictionary<object, object>
			{
				{ "k0", null },
				{ "k1", 50L },
				{ "k2", 5000L },
				{ "k3", 5000000L },
				{ "k4", "test" },
				{ "k5", "a loooooooooonger string" },
				{ "k6", 123.45 },
				{ "k7", -1234L }
			};

			Bin bin = new(Suite.GetBinName("listmapbin"), map);
			client.Put(null, key, bin);

			var received = new Dictionary<object, object>();
			Record record = Get(key, buffer =>
			{
				var unpacker = new Unpacker(buffer, 0, buffer.Length, false);
				int count = unpacker.UnpackMapItemCount(out MapOrder mapOrder);
				while (count-- > 0)
				{
					string k = unpacker.UnpackString();
					Assert.IsNotNull(k);
					switch (k)
					{
						case "k0":
						case "k1":
						case "k2":
						case "k3":
						case "k7":
							{
								long? value = unpacker.UnpackInteger();
								received.Add(k, value);
								break;
							}

						case "k4":
						case "k5":
							{
								string value = unpacker.UnpackString();
								received.Add(k, value);
								break;
							}

						case "k6":
							{
								double? value = unpacker.UnpackDouble();
								received.Add(k, value);
								break;
							}

						default:
							Assert.Fail();
							break;
					}
				}
			});

			Assert.IsTrue(record.generation > 0);
			Assert.AreEqual(map.Count, received.Count);
			foreach (var kv in map)
			{
				Assert.AreEqual(kv.Value, received[kv.Key], "mismatch for key: " + kv.Key);
			}
		}

		[TestMethod]
		public void UnpackList()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "customparser");
			var list = new List<object>
			{
				null,
				50L,
				5000L,
				5000000L,
				"test",
				"a loooooooooonger string",
				123.45,
				-1234L,
				234.56F,
				true,
				false
			};

			Bin bin = new(Suite.GetBinName("listmapbin"), list);
			client.Put(null, key, bin);

			var received = new List<object>();
			Record record = Get(key, buffer =>
			{
				var unpacker = new Unpacker(buffer, 0, buffer.Length, false);
				int count = unpacker.UnpackListItemCount();
				Assert.AreEqual(list.Count, count);
				received.Add(unpacker.UnpackInteger());
				received.Add(unpacker.UnpackInteger());
				received.Add(unpacker.UnpackInteger());
				received.Add(unpacker.UnpackInteger());
				received.Add(unpacker.UnpackString());
				received.Add(unpacker.UnpackString());
				received.Add(unpacker.UnpackDouble());
				received.Add(unpacker.UnpackInteger());
				received.Add(unpacker.UnpackFloat());
				received.Add(unpacker.UnpackBool());
				received.Add(unpacker.UnpackBool());
			});

			Assert.IsTrue(record.generation > 0);
			Assert.AreEqual(list.Count, received.Count);
			for (int i = 0; i < list.Count; ++i)
				Assert.AreEqual(list[i], received[i], "mismatch for index: " + i);
		}

		[TestMethod]
		public void UnpackComplex()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "customparser");
			var list = new List<object>();
			var m1 = new Dictionary<object, object>();
			var m11 = new Dictionary<object, object>();
			var l12 = new List<object>();
			var m13 = new Dictionary<object, object>();
			var l2 = new List<object>();

			list.Add(50L);
			list.Add(m1);
			list.Add(l2);
			m1.Add("k0", "text");
			m1.Add("k1", m11);
			m1.Add("k2", l12);
			m1.Add("k3", m13);
			l12.Add(100L);
			l12.Add(200L);
			m13.Add("k131", 300L);
			l2.Add("some more text");

			Bin bin = new(Suite.GetBinName("listmapbin"), list);
			client.Put(null, key, bin);

			Record record = Get(key, buffer =>
			{
				var unpacker = new Unpacker(buffer, 0, buffer.Length, false);
				int listCount = unpacker.UnpackListItemCount();
				Assert.AreEqual(list.Count, listCount);
				Assert.AreEqual(list[0], unpacker.UnpackInteger());

				int m1Count = unpacker.UnpackMapItemCount(out MapOrder m1Order);
				Assert.AreEqual(m1.Count, m1Count);
				var m1Keys = new HashSet<string>();
				for (int i = 0; i < m1Count; ++i)
				{
					string k = unpacker.UnpackString();
					Assert.IsFalse(m1Keys.Contains(k));
					m1Keys.Add(k);

					switch (k)
					{
						case "k0":
							Assert.AreEqual(m1[k], unpacker.UnpackString());
							break;
						case "k1":
							{
								Assert.AreEqual(m11.Count, unpacker.UnpackMapItemCount(out MapOrder k1Order));
								break;
							}
						case "k2":
							Assert.AreEqual(l12.Count, unpacker.UnpackListItemCount());
							Assert.AreEqual(l12[0], unpacker.UnpackInteger());
							Assert.AreEqual(l12[1], unpacker.UnpackInteger());
							break;
						case "k3":
							{
								Assert.AreEqual(m13.Count, unpacker.UnpackMapItemCount(out MapOrder k3Order));
								Assert.AreEqual("k131", unpacker.UnpackString());
								Assert.AreEqual(m13["k131"], unpacker.UnpackInteger());
								break;
							}
						default:
							Assert.Fail("unexpected m1 key: " + k);
							break;
					}
				}

				Assert.AreEqual(l2.Count, unpacker.UnpackListItemCount());
				Assert.AreEqual(l2[0], unpacker.UnpackString());
			});

			Assert.IsTrue(record.generation > 0);
		}

		[TestMethod]
		public void SkippingComplex()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "customparser");
			var list = new List<object>();
			var m1 = new Dictionary<object, object>();
			var m11 = new Dictionary<object, object>();
			var l12 = new List<object>();
			var m13 = new Dictionary<object, object>();
			var l2 = new List<object>();

			list.Add(50L);
			list.Add(m1);
			list.Add(null);
			list.Add(l2);
			list.Add(-12345L);
			list.Add(12345L);
			list.Add(123456789012345L);
			list.Add(42424242L);
			m1.Add("k0", "text");
			m1.Add("k1", m11);
			m1.Add("k2", l12);
			m1.Add("k3", m13);
			l12.Add(100L);
			l12.Add(200L);
			m13.Add("k131", 300L);
			l2.Add("some more text");

			Bin bin = new(Suite.GetBinName("listmapbin"), list);
			client.Put(null, key, bin);

			Record record = Get(key, buffer =>
			{
				var unpacker = new Unpacker(buffer, 0, buffer.Length, false);
				int listCount = unpacker.UnpackListItemCount();
				Assert.AreEqual(list.Count, listCount);
				unpacker.SkipObject(); // skip long
				unpacker.SkipObject(); // skip m1
				unpacker.SkipObject(); // skip null
				unpacker.SkipObject(); // skip l2
				unpacker.SkipObjects(3); // skip longs
				Assert.AreEqual(list.Last(), unpacker.UnpackInteger());
			});

			Assert.IsTrue(record.generation > 0);
		}
	}
}
