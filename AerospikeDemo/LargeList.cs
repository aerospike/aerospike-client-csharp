/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
			if (!args.hasUdf)
			{
				console.Info("Large list functions are not supported by the connected Aerospike server.");
				return;
			}

			RunWithDistinctBins(client, args);
			RunWithSerializedBin(client, args);
		}

		/// <summary>
		/// Use distinct sub-bins for row in largelist bin. 
		/// </summary>
		public void RunWithDistinctBins(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "accountId");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);	

			// Initialize large list operator.
			Aerospike.Client.LargeList list = client.GetLargeList(args.policy, key, "trades", null);

			// Write trades
			Dictionary<string,Value> dict = new Dictionary<string,Value>();

			DateTime timestamp1 = new DateTime(2014, 6, 25, 12, 18, 43);
			dict["key"] = Value.Get(timestamp1.Ticks);
			dict["ticker"] = Value.Get("IBM");
			dict["qty"] = Value.Get(100);
			dict["price"] = Value.Get(BitConverter.GetBytes(181.82));
			list.Add(Value.GetAsMap(dict));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			dict["key"] = Value.Get(timestamp2.Ticks);
			dict["ticker"] = Value.Get("GE");
			dict["qty"] = Value.Get(500);
			dict["price"] = Value.Get(BitConverter.GetBytes(26.36));
			list.Add(Value.GetAsMap(dict));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			dict["key"] = Value.Get(timestamp3.Ticks);
			dict["ticker"] = Value.Get("AAPL");
			dict["qty"] = Value.Get(75);
			dict["price"] = Value.Get(BitConverter.GetBytes(91.85));
			list.Add(Value.GetAsMap(dict));

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
		public void RunWithSerializedBin(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "accountId");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Initialize large list operator.
			Aerospike.Client.LargeList list = client.GetLargeList(args.policy, key, "trades", null);

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
			list.Add(Value.GetAsMap(dict));

			DateTime timestamp2 = new DateTime(2014, 6, 26, 9, 33, 17);
			dict["key"] = Value.Get(timestamp2.Ticks);
			ms.SetLength(0);
			writer = new BinaryWriter(ms);
			writer.Write("GE");  // ticker
			writer.Write(500);   // qty
			writer.Write(26.36); // price
			dict["value"] = Value.Get(ms.ToArray());
			list.Add(Value.GetAsMap(dict));

			DateTime timestamp3 = new DateTime(2014, 6, 27, 14, 40, 19);
			dict["key"] = Value.Get(timestamp3.Ticks);
			ms.SetLength(0);
			writer = new BinaryWriter(ms);
			writer.Write("AAPL");  // ticker
			writer.Write(75);      // qty
			writer.Write(91.85);   // price
			dict["value"] = Value.Get(ms.ToArray());
			list.Add(Value.GetAsMap(dict));

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
	}
}
