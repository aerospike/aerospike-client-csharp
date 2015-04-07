/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class LargeList : SyncExample
	{
		public LargeList(Console console) : base(console)
		{
		}

		/// <summary>
		/// Perform operations on a list within a single bin.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasLargeDataTypes)
			{
				console.Info("Large list functions are not supported by the connected Aerospike server.");
				return;
			}

			RunSimpleExample(client, args);
			RunWithDistinctBins(client, args);
			RunWithSerializedBin(client, args);
			RunWithDefaultSerializedBin(client, args);
		}

		/// <summary>
		/// Simple examples of large list functionality.
		/// </summary>
		private void RunSimpleExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "setkey");
			string binName = args.GetBinName("ListBin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Initialize large set operator.
			Aerospike.Client.LargeList llist = client.GetLargeList(args.writePolicy, key, binName);
			string orig1 = "llistValue1";
			string orig2 = "llistValue2";
			string orig3 = "llistValue3";

			// Write values.
			llist.Add(Value.Get(orig1));
			llist.Add(Value.Get(orig2));
			llist.Add(Value.Get(orig3));

			IDictionary map = llist.GetConfig();

			foreach (DictionaryEntry entry in map)
			{
				console.Info(entry.Key.ToString() + ',' + entry.Value);
			}

			IList rangeList = llist.Range(Value.Get(orig2), Value.Get(orig3));

			if (rangeList == null)
			{
				throw new Exception("Range returned null.");
			}

			if (rangeList.Count != 2)
			{
				throw new Exception("Range Size mismatch. Expected 2 Received " + rangeList.Count);
			}
			string v2 = (string) rangeList[0];
			string v3 = (string) rangeList[1];

			if (v2.Equals(orig2) && v3.Equals(orig3))
			{
				console.Info("Range Query matched: v2=" + orig2 + " v3=" + orig3);
			}
			else
			{
				throw new Exception("Range Content mismatch. Expected (" + orig2 + ":" + orig3 +
					") Received (" + v2 + ":" + v3 + ")");
			}

			// Remove last value.
			llist.Remove(Value.Get(orig3));

			int size = llist.Size();

			if (size != 2)
			{
				throw new Exception("Size mismatch. Expected 2 Received " + size);
			}

			IList listReceived = llist.Find(Value.Get(orig2));
			string expected = orig2;

			if (listReceived == null)
			{
				console.Error("Data mismatch: Expected " + expected + " Received null");
				return;
			}

			string stringReceived = (string) listReceived[0];

			if (stringReceived != null && stringReceived.Equals(expected))
			{
				console.Info("Data matched: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey +
					" value=" + stringReceived);
			}
			else
			{
				console.Error("Data mismatch: Expected " + expected + " Received " + stringReceived);
			}
		}

		/// <summary>
		/// Use distinct sub-bins for row in largelist bin. 
		/// </summary>
		private void RunWithDistinctBins(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "accountId");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);	

			// Initialize large list operator.
			Aerospike.Client.LargeList list = client.GetLargeList(args.writePolicy, key, "trades");

			// Write trades
			Dictionary<string,Value> dict = new Dictionary<string,Value>();

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			dict["key"] = Value.Get(timestamp1.Ticks);
			dict["ticker"] = Value.Get("IBM");
			dict["qty"] = Value.Get(100);
			dict["price"] = Value.Get(BitConverter.GetBytes(181.82));
			list.Add(Value.Get(dict));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			dict["key"] = Value.Get(timestamp2.Ticks);
			dict["ticker"] = Value.Get("GE");
			dict["qty"] = Value.Get(500);
			dict["price"] = Value.Get(BitConverter.GetBytes(26.36));
			list.Add(Value.Get(dict));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			dict["key"] = Value.Get(timestamp3.Ticks);
			dict["ticker"] = Value.Get("AAPL");
			dict["qty"] = Value.Get(75);
			dict["price"] = Value.Get(BitConverter.GetBytes(91.85));
			list.Add(Value.Get(dict));

			// Verify list size
			int size = list.Size();

			if (size != 3)
			{
				throw new Exception("List size mismatch. Expected 3 Received " + size);
			}

			// Filter on range of timestamps
			DateTime begin = new DateTime(2014, 6, 26);
			DateTime end = new DateTime(2014, 6, 28);
			IList results = list.Range(Value.Get(begin.Ticks), Value.Get(end.Ticks));

			if (results.Count != 2)
			{
				throw new Exception("Query results size mismatch. Expected 2 Received " + results.Count);
			}

			// Verify data.
			ValidateWithDistinctBins(results, 0, timestamp2, "GE", 500, 26.36);
			ValidateWithDistinctBins(results, 1, timestamp3, "AAPL", 75, 91.85);
			
			console.Info("Data matched.");

			console.Info("Run large list scan.");
			IList rows = list.Scan();
			foreach (IDictionary row in rows)
			{
				foreach (DictionaryEntry entry in row)
				{
					//console.Info(entry.Key.ToString());
					//console.Info(entry.Value.ToString());
				}
			}
			console.Info("Large list scan complete.");
		}

		private void ValidateWithDistinctBins(IList list, int index, DateTime expectedTime, string expectedTicker, int expectedQty, double expectedPrice)
		{
			IDictionary dict = (IDictionary)list[index];
			DateTime receivedTime = new DateTime((long)dict["key"]);

			if (expectedTime != receivedTime)
			{
				throw new Exception("Time mismatch: Expected " + expectedTime + ". Received " + receivedTime);
			}

			string receivedTicker = (string)dict["ticker"];

			if (expectedTicker != receivedTicker)
			{
				throw new Exception("Ticker mismatch: Expected " + expectedTicker + ". Received " + receivedTicker);
			}

			long receivedQty = (long)dict["qty"];

			if (expectedQty != receivedQty)
			{
				throw new Exception("Quantity mismatch: Expected " + expectedQty + ". Received " + receivedQty);
			}

			double receivedPrice = BitConverter.ToDouble((byte[])dict["price"], 0);

			if (expectedPrice != receivedPrice)
			{
				throw new Exception("Price mismatch: Expected " + expectedPrice + ". Received " + receivedPrice);
			}
		}

		/// <summary>
		/// Use serialized bin for row in largelist bin. 
		/// </summary>
		private void RunWithSerializedBin(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "accountId");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Initialize large list operator.
			Aerospike.Client.LargeList list = client.GetLargeList(args.writePolicy, key, "trades");

			// Write trades
			Dictionary<string, Value> dict = new Dictionary<string, Value>();
			MemoryStream ms = new MemoryStream(500);

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			dict["key"] = Value.Get(timestamp1.Ticks);
			BinaryWriter writer = new BinaryWriter(ms);
			writer.Write("IBM");  // ticker
			writer.Write(100);    // qty
			writer.Write(181.82); // price
			dict["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(dict));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			dict["key"] = Value.Get(timestamp2.Ticks);
			ms.SetLength(0);
			writer = new BinaryWriter(ms);
			writer.Write("GE");  // ticker
			writer.Write(500);   // qty
			writer.Write(26.36); // price
			dict["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(dict));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			dict["key"] = Value.Get(timestamp3.Ticks);
			ms.SetLength(0);
			writer = new BinaryWriter(ms);
			writer.Write("AAPL");  // ticker
			writer.Write(75);      // qty
			writer.Write(91.85);   // price
			dict["value"] = Value.Get(ms.ToArray());
			list.Add(Value.Get(dict));

			// Verify list size
			int size = list.Size();

			if (size != 3)
			{
				throw new Exception("List size mismatch. Expected 3 Received " + size);
			}

			// Filter on range of timestamps
			DateTime begin = new DateTime(2014, 6, 26);
			DateTime end = new DateTime(2014, 6, 28);
			IList results = list.Range(Value.Get(begin.Ticks), Value.Get(end.Ticks));

			if (results.Count != 2)
			{
				throw new Exception("Query results size mismatch. Expected 2 Received " + results.Count);
			}

			// Verify data.
			ValidateWithSerializedBin(results, 0, timestamp2, "GE", 500, 26.36);
			ValidateWithSerializedBin(results, 1, timestamp3, "AAPL", 75, 91.85);

			console.Info("Data matched.");
		}

		private void ValidateWithSerializedBin(IList list, int index, DateTime expectedTime, string expectedTicker, int expectedQty, double expectedPrice)
		{
			IDictionary dict = (IDictionary)list[index];
			DateTime receivedTime = new DateTime((long)dict["key"]);

			if (expectedTime != receivedTime)
			{
				throw new Exception("Time mismatch: Expected " + expectedTime + ". Received " + receivedTime);
			}

			byte[] value = (byte[])dict["value"];
			MemoryStream ms = new MemoryStream(value);
			BinaryReader reader = new BinaryReader(ms);
			string receivedTicker = reader.ReadString();

			if (expectedTicker != receivedTicker)
			{
				throw new Exception("Ticker mismatch: Expected " + expectedTicker + ". Received " + receivedTicker);
			}

			int receivedQty = reader.ReadInt32();

			if (expectedQty != receivedQty)
			{
				throw new Exception("Quantity mismatch: Expected " + expectedQty + ". Received " + receivedQty);
			}

			double receivedPrice = reader.ReadDouble();

			if (expectedPrice != receivedPrice)
			{
				throw new Exception("Price mismatch: Expected " + expectedPrice + ". Received " + receivedPrice);
			}
		}

		/// <summary>
		/// Use default serialized bin for row in largelist bin. 
		/// </summary>
		private void RunWithDefaultSerializedBin(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "accountId");
			object value1 = new CompoundObject("IBM", 100);
			object value2 = new CompoundObject("GE", 500);
			object value3 = new CompoundObject("AAPL", 75);

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Initialize large list operator.
			Aerospike.Client.LargeList list = client.GetLargeList(args.writePolicy, key, "trades");

			// Write trades
			Dictionary<string, Value> dict = new Dictionary<string, Value>();

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			dict["key"] = Value.Get(timestamp1.Ticks);
			dict["value"] = Value.Get(value1);
			list.Add(Value.Get(dict));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			dict["key"] = Value.Get(timestamp2.Ticks);
			dict["value"] = Value.Get(value2);
			list.Add(Value.Get(dict));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			dict["key"] = Value.Get(timestamp3.Ticks);
			dict["value"] = Value.Get(value3);
			list.Add(Value.Get(dict));

			// Verify list size
			int size = list.Size();

			if (size != 3)
			{
				throw new Exception("List size mismatch. Expected 3 Received " + size);
			}

			// Filter on range of timestamps
			DateTime begin = new DateTime(2014, 6, 26);
			DateTime end = new DateTime(2014, 6, 28);
			IList results = list.Range(Value.Get(begin.Ticks), Value.Get(end.Ticks));

			if (results.Count != 2)
			{
				throw new Exception("Query results size mismatch. Expected 2 Received " + results.Count);
			}

			// Verify data.
			ValidateDefault(results, 0, timestamp2, value2);
			ValidateDefault(results, 1, timestamp3, value3);

			console.Info("Data matched.");
		}

		private static void ValidateDefault(IList list, int index, DateTime expectedTime, object expectedValue)
		{
			IDictionary dict = (IDictionary)list[index];
			DateTime receivedTime = new DateTime((long)dict["key"]);

			if (expectedTime != receivedTime)
			{
				throw new Exception("Time mismatch: Expected " + expectedTime + ". Received " + receivedTime);
			}

			object receivedValue = dict["value"];

			if (!receivedValue.Equals(expectedValue))
			{
				throw new Exception(string.Format("Mismatch: expected={0} received={1}", expectedValue, receivedValue));
			}
		}
	}
}
