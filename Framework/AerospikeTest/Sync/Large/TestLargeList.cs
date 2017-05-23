/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestLargeList : TestSync
	{
		private static readonly string binName = args.GetBinName("ListBin");

		[ClassInitialize()]
		public static void Register(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.Resources.largelist_example.lua", "largelist_example.lua", Language.LUA);
			task.Wait();
		}

		[TestMethod]
		public void SimpleLargeList()
		{
			if (!args.ValidateLDT())
			{
				return;
			}
			Key key = new Key(args.ns, args.set, "setkey");

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize large set operator.
			LargeList llist = client.GetLargeList(null, key, binName);
			string orig1 = "llistValue1";
			string orig2 = "llistValue2";
			string orig3 = "llistValue3";

			// Write values.
			llist.Add(Value.Get(orig1));
			llist.Add(Value.Get(orig2));
			llist.Add(Value.Get(orig3));

			// Perform exists.
			bool b = llist.Exists(Value.Get(orig2));
			Assert.IsTrue(b);

			b = llist.Exists(Value.Get("notfound"));
			Assert.IsFalse(b);

			// Test record not found.
			LargeList nflist = client.GetLargeList(null, new Key(args.ns, args.set, "sfdfdqw"), binName);
			try
			{
				b = nflist.Exists(Value.Get(orig2));
				Assert.IsFalse(b);
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.KEY_NOT_FOUND_ERROR, ae.Result);
			}

			IList klist = new List<Value>();
			klist.Add(Value.Get(orig2));
			klist.Add(Value.Get(orig1));
			klist.Add(Value.Get("notfound"));
			IList<bool> blist = llist.Exists(klist);
			Assert.IsTrue(blist[0]);
			Assert.IsTrue(blist[1]);
			Assert.IsFalse(blist[2]);

			// Test record not found.
			try
			{
				IList<bool> blist2 = nflist.Exists(klist);
				Assert.IsFalse(blist2[0]);
				Assert.IsFalse(blist2[1]);
				Assert.IsFalse(blist2[2]);
			}
			catch (AerospikeException ae)
			{
				Assert.AreEqual(ResultCode.KEY_NOT_FOUND_ERROR, ae.Result);
			}

			// Perform a Range Query -- look for "llistValue2" to "llistValue3"
			IList rangeList = llist.Range(Value.Get(orig2), Value.Get(orig3));
			Assert.IsNotNull(rangeList);
			Assert.AreEqual(2, rangeList.Count);

			string v2 = (string) rangeList[0];
			string v3 = (string) rangeList[1];
			Assert.AreEqual(orig2, v2);
			Assert.AreEqual(orig3, v3);

			// Remove last value.
			llist.Remove(Value.Get(orig3));

			int size = llist.Size();
			Assert.AreEqual(2, size);

			IList listReceived = llist.Find(Value.Get(orig2));
			string expected = orig2;

			Assert.IsNotNull(listReceived);

			string stringReceived = (string) listReceived[0];
			Assert.IsNotNull(stringReceived);
			Assert.AreEqual(expected, stringReceived);
		}

		[TestMethod]
		public void FilterLargeList()
		{
			if (!args.ValidateLDT())
			{
				return;
			}
			Key key = new Key(args.ns, args.set, "setkey");

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize large set operator.
			LargeList llist = client.GetLargeList(null, key, binName);
			int orig1 = 1;
			int orig2 = 2;
			int orig3 = 3;
			int orig4 = 4;

			// Write values.
			llist.Add(Value.Get(orig1), Value.Get(orig2), Value.Get(orig3), Value.Get(orig4));

			// Filter on values
			IList filterList = llist.Filter("largelist_example", "my_filter_func", Value.Get(orig3));
			Assert.IsNotNull(filterList);
			Assert.AreEqual(1, filterList.Count);

			long v = (long)filterList[0];
			Assert.AreEqual(orig3, (int)v);
		}

		[TestMethod]
		public void DistinctBinsLargeList()
		{
			if (!args.ValidateLDT())
			{
				return;
			}
			Key key = new Key(args.ns, args.set, "accountId");

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize large list operator.
			LargeList list = client.GetLargeList(null, key, "trades");

			// Write trades
			Dictionary<string, Value> map = new Dictionary<string, Value>();

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			map["key"] = Value.Get(timestamp1.Ticks);
			map["ticker"] = Value.Get("IBM");
			map["qty"] = Value.Get(100);
			map["price"] = Value.Get(BitConverter.GetBytes(181.82));
			list.Add(Value.Get(map));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			map["key"] = Value.Get(timestamp2.Ticks);
			map["ticker"] = Value.Get("GE");
			map["qty"] = Value.Get(500);
			map["price"] = Value.Get(BitConverter.GetBytes(26.36));
			list.Add(Value.Get(map));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			map["key"] = Value.Get(timestamp3.Ticks);
			map["ticker"] = Value.Get("AAPL");
			map["qty"] = Value.Get(75);
			map["price"] = Value.Get(BitConverter.GetBytes(91.85));
			list.Add(Value.Get(map));

			// Verify list size
			int size = list.Size();
			Assert.AreEqual(3, size);

			// Filter on range of timestamps
			DateTime begin = new DateTime(2014, 6, 26);
			DateTime end = new DateTime(2014, 6, 28);
			IList results = list.Range(Value.Get(begin.Ticks), Value.Get(end.Ticks));
			Assert.IsNotNull(results);
			Assert.AreEqual(2, results.Count);

			// Verify data.
			ValidateWithDistinctBins(results, 0, timestamp2, "GE", 500, 26.36);
			ValidateWithDistinctBins(results, 1, timestamp3, "AAPL", 75, 91.85);

			IList rows = list.Scan();
			foreach (IDictionary row in rows)
			{
				foreach (DictionaryEntry entry in row)
				{
					//console.Info(entry.Key.ToString());
					//console.Info(entry.Value.ToString());
				}
			}
		}

		private void ValidateWithDistinctBins(IList list, int index, DateTime expectedTime, string expectedTicker, int expectedQty, double expectedPrice)
		{
			IDictionary map = (IDictionary)list[index];
			DateTime receivedTime = new DateTime((long)map["key"]);

			Assert.AreEqual(expectedTime, receivedTime);
			Assert.AreEqual(expectedTicker, (string)map["ticker"]);
			Assert.AreEqual(expectedQty, (int)((long)map["qty"]));
			Assert.AreEqual(expectedPrice, BitConverter.ToDouble((byte[])map["price"], 0), 0.000001);
		}

		[TestMethod]
		public void RunWithSerializedBin()
		{
			if (!args.ValidateLDT())
			{
				return;
			}
			Key key = new Key(args.ns, args.set, "accountId");

			// Delete record if it already exists.
			client.Delete(null, key);

			// Initialize large list operator.
			LargeList list = client.GetLargeList(null, key, "trades");

			// Write trades
			Dictionary<string, Value> map = new Dictionary<string, Value>();
			MemoryStream ms = new MemoryStream(500);

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			map["key"] = Value.Get(timestamp1.Ticks);
			BinaryWriter writer = new BinaryWriter(ms);
			writer.Write("IBM");  // ticker
			writer.Write(100);    // qty
			writer.Write(181.82); // price
			map["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(map));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			map["key"] = Value.Get(timestamp2.Ticks);
			ms.SetLength(0);
			writer = new BinaryWriter(ms);
			writer.Write("GE");  // ticker
			writer.Write(500);   // qty
			writer.Write(26.36); // price
			map["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(map));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			map["key"] = Value.Get(timestamp3.Ticks);
			ms.SetLength(0);
			writer = new BinaryWriter(ms);
			writer.Write("AAPL");  // ticker
			writer.Write(75);      // qty
			writer.Write(91.85);   // price
			map["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(map));

			// Verify list size
			int size = list.Size();
			Assert.AreEqual(3, size);

			// Filter on range of timestamps
			DateTime begin = new DateTime(2014, 6, 26);
			DateTime end = new DateTime(2014, 6, 28);
			IList results = list.Range(Value.Get(begin.Ticks), Value.Get(end.Ticks));
			Assert.IsNotNull(results);
			Assert.AreEqual(2, results.Count);

			// Verify data.
			ValidateWithSerializedBin(results, 0, timestamp2, "GE", 500, 26.36);
			ValidateWithSerializedBin(results, 1, timestamp3, "AAPL", 75, 91.85);
		}

		private void ValidateWithSerializedBin(IList list, int index, DateTime expectedTime, string expectedTicker, int expectedQty, double expectedPrice)
		{
			IDictionary map = (IDictionary)list[index];
			DateTime receivedTime = new DateTime((long)map["key"]);

			Assert.AreEqual(expectedTime, receivedTime);

			byte[] value = (byte[])map["value"];
			MemoryStream ms = new MemoryStream(value);
			BinaryReader reader = new BinaryReader(ms);
			string receivedTicker = reader.ReadString();
			int receivedQty = reader.ReadInt32();
			double receivedPrice = reader.ReadDouble();

			Assert.AreEqual(expectedTicker, receivedTicker);
			Assert.AreEqual(expectedQty, receivedQty);
			Assert.AreEqual(expectedPrice, receivedPrice, 0.000001);
		}

		[TestMethod]
		public void RunVolumeInsert()
		{
			if (!args.ValidateLDT())
			{
				return;
			}
			// This key has already been created in runSimpleExample().
			Key key = new Key(args.ns, args.set, "setkey");

			int itemCount = 2000;
			LargeList llist2 = client.GetLargeList(null, key, "NumberBin");
			for (int i = itemCount; i > 0; i--)
			{
				llist2.Add(Value.Get(i));
			}
			Assert.AreEqual(2000, llist2.Size());
		}
	}
}
