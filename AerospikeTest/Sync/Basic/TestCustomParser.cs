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

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestCustomParser : TestSync
	{
		private delegate void ParseBin(byte[] buffer);

		private class BinParser : IRecordParser
		{
			private ParseBin parseBin;

			public BinParser(ParseBin parseBin)
			{
				this.parseBin = parseBin;
			}

			public (Record record, int dataOffset) ParseRecord(byte[] dataBuffer, int dataOffset, int opCount, int generation, int expiration, bool isOperation)
			{
				string binName;
				byte valueType;
				int valueOffset;
				int valueSize;

				int dataOffsetLocal = RecordParser.ExtractBinValue(
					dataBuffer,
					dataOffset,
					out binName,
					out valueType,
					out valueOffset,
					out valueSize);

				this.parseBin(dataBuffer.Skip(valueOffset).Take(valueSize).ToArray());
				return (new Record(null, generation, expiration), dataOffsetLocal);
			}
		}

		private Record Get(Key key, ParseBin parseBin)
		{
			var policy = new Policy() { recordParser = new BinParser(parseBin) };
			return client.Get(policy, key);
		}

		[TestMethod]
		public void UnpackMap()
		{
			Key key = new Key(args.ns, args.set, "customparser");
			var map = new Dictionary<object, object>();

			map.Add("k0", null);
			map.Add("k1", 50L);
			map.Add("k2", 5000L);
			map.Add("k3", 5000000L);
			map.Add("k4", "test");
			map.Add("k5", "a loooooooooonger string");
			map.Add("k6", 123.45);
			map.Add("k7", -1234L);

			Bin bin = new Bin(args.GetBinName("listmapbin"), map);
			client.Put(null, key, bin);

			var received = new Dictionary<object, object>();
			Record record = Get(key, buffer =>
			{
				var unpacker = new Unpacker(buffer, 0, buffer.Length, false);
				MapOrder mapOrder;
				int count = unpacker.UnpackMapItemCount(out mapOrder);
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
			Key key = new Key(args.ns, args.set, "customparser");
			var list = new List<object>();

			list.Add(null);
			list.Add(50L);
			list.Add(5000L);
			list.Add(5000000L);
			list.Add("test");
			list.Add("a loooooooooonger string");
			list.Add(123.45);
			list.Add(-1234L);
			list.Add(234.56F);
			list.Add(true);
			list.Add(false);

			Bin bin = new Bin(args.GetBinName("listmapbin"), list);
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
			Key key = new Key(args.ns, args.set, "customparser");
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

			Bin bin = new Bin(args.GetBinName("listmapbin"), list);
			client.Put(null, key, bin);

			Record record = Get(key, buffer =>
			{
				var unpacker = new Unpacker(buffer, 0, buffer.Length, false);
				int listCount = unpacker.UnpackListItemCount();
				Assert.AreEqual(list.Count, listCount);
				Assert.AreEqual(list[0], unpacker.UnpackInteger());

				MapOrder m1Order;
				int m1Count = unpacker.UnpackMapItemCount(out m1Order);
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
							MapOrder k1Order;
							Assert.AreEqual(m11.Count, unpacker.UnpackMapItemCount(out k1Order));
							break;
						}
						case "k2":
							Assert.AreEqual(l12.Count, unpacker.UnpackListItemCount());
							Assert.AreEqual(l12[0], unpacker.UnpackInteger());
							Assert.AreEqual(l12[1], unpacker.UnpackInteger());
							break;
						case "k3":
						{
							MapOrder k3Order;
							Assert.AreEqual(m13.Count, unpacker.UnpackMapItemCount(out k3Order));
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
			Key key = new Key(args.ns, args.set, "customparser");
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

			Bin bin = new Bin(args.GetBinName("listmapbin"), list);
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
