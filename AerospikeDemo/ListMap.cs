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
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class ListMap : SyncExample
	{
		public ListMap(Console console) : base(console)
		{
		}

		/// <summary>
		/// Write List and Map objects directly instead of relying on C# serializer.
		/// This functionality is only supported in Aerospike 3.0 servers.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("List/Map functions are not supported by the connected Aerospike server.");
				return;
			}
			TestListStrings(client, args);
			TestListComplex(client, args);
			TestMapStrings(client, args);
			TestMapComplex(client, args);
			TestListMapCombined(client, args);
		}

		/// <summary>
		/// Write/Read ArrayList<String> directly instead of relying on java serializer.
		/// </summary>
		private void TestListStrings(AerospikeClient client, Arguments args)
		{
			console.Info("Read/Write ArrayList<String>");
			Key key = new Key(args.ns, args.set, "listkey1");
			client.Delete(args.writePolicy, key);

			List<object> list = new List<object>();
			list.Add("string1");
			list.Add("string2");
			list.Add("string3");

			Bin bin = Bin.AsList(args.GetBinName("listbin1"), list);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			List<object> receivedList = (List<object>) record.GetValue(bin.name);

			ValidateSize(3, receivedList.Count);
			Validate("string1", receivedList[0]);
			Validate("string2", receivedList[1]);
			Validate("string3", receivedList[2]);

			console.Info("Read/Write ArrayList<String> successful.");
		}

		/// <summary>
		/// Write/Read ArrayList<Object> directly instead of relying on C# serializer.
		/// </summary>
		private void TestListComplex(AerospikeClient client, Arguments args)
		{
			console.Info("Read/Write ArrayList<Object>");
			Key key = new Key(args.ns, args.set, "listkey2");
			client.Delete(args.writePolicy, key);

			byte[] blob = new byte[] {3, 52, 125};
			List<object> list = new List<object>();
			list.Add("string1");
			list.Add(2);
			list.Add(blob);

			Bin bin = Bin.AsList(args.GetBinName("listbin2"), list);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			List<object> receivedList = (List<object>) record.GetValue(bin.name);

			ValidateSize(3, receivedList.Count);
			Validate("string1", receivedList[0]);
			// Server convert numbers to long, so must expect long.
			Validate(2L, receivedList[1]);
			Validate(blob, (byte[])receivedList[2]);

			console.Info("Read/Write ArrayList<Object> successful.");
		}

		/// <summary>
		/// Write/Read HashMap<String,String> directly instead of relying on java serializer.
		/// </summary>
		private void TestMapStrings(AerospikeClient client, Arguments args)
		{
			console.Info("Read/Write HashMap<String,String>");
			Key key = new Key(args.ns, args.set, "mapkey1");
			client.Delete(args.writePolicy, key);

			Dictionary<object, object> map = new Dictionary<object, object>();
			map["key1"] = "string1";
			map["key2"] = "string2";
			map["key3"] = "string3";

			Bin bin = Bin.AsMap(args.GetBinName("mapbin1"), map);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			Dictionary<object, object> receivedMap = (Dictionary<object, object>)record.GetValue(bin.name);

			ValidateSize(3, receivedMap.Count);
			Validate("string1", receivedMap["key1"]);
			Validate("string2", receivedMap["key2"]);
			Validate("string3", receivedMap["key3"]);

			console.Info("Read/Write HashMap<String,String> successful");
		}

		/// <summary>
		/// Write/Read HashMap<Object,Object> directly instead of relying on java serializer.
		/// </summary>
		private void TestMapComplex(AerospikeClient client, Arguments args)
		{
			console.Info("Read/Write HashMap<Object,Object>");
			Key key = new Key(args.ns, args.set, "mapkey2");
			client.Delete(args.writePolicy, key);

			byte[] blob = new byte[] {3, 52, 125};
			List<int> list = new List<int>();
			list.Add(100034);
			list.Add(12384955);
			list.Add(3);
			list.Add(512);

			Dictionary<object, object> map = new Dictionary<object, object>();
			map["key1"] = "string1";
			map["key2"] = 2;
			map["key3"] = blob;
			map["key4"] = list;

			Bin bin = Bin.AsMap(args.GetBinName("mapbin2"), map);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			Dictionary<object, object> receivedMap = (Dictionary<object, object>)record.GetValue(bin.name);

			ValidateSize(4, receivedMap.Count);
			Validate("string1", receivedMap["key1"]);
			// Server convert numbers to long, so must expect long.
			Validate(2L, receivedMap["key2"]);
			Validate(blob, (byte[])receivedMap["key3"]);

			IList receivedInner = (IList)receivedMap["key4"];
			ValidateSize(4, receivedInner.Count);
			Validate(100034L, receivedInner[0]);
			Validate(12384955L, receivedInner[1]);
			Validate(3L, receivedInner[2]);
			Validate(512L, receivedInner[3]);

			console.Info("Read/Write HashMap<Object,Object> successful");
		}

		/// <summary>
		/// Write/Read List/HashMap combination directly instead of relying on java serializer.
		/// </summary>
		private void TestListMapCombined(AerospikeClient client, Arguments args)
		{
			console.Info("Read/Write List/HashMap");
			Key key = new Key(args.ns, args.set, "listmapkey");
			client.Delete(args.writePolicy, key);

			byte[] blob = new byte[] {3, 52, 125};
			List<object> inner = new List<object>();
			inner.Add("string2");
			inner.Add(5);

			Dictionary<object, object> innerMap = new Dictionary<object, object>();
			innerMap["a"] = 1;
			innerMap[2] = "b";
			innerMap[3] = blob;
			innerMap["list"] = inner;

			List<object> list = new List<object>();
			list.Add("string1");
			list.Add(8);
			list.Add(inner);
			list.Add(innerMap);

			Bin bin = Bin.AsList(args.GetBinName("listmapbin"), list);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			List<object> received = (List<object>) record.GetValue(bin.name);

			ValidateSize(4, received.Count);
			Validate("string1", received[0]);
			// Server convert numbers to long, so must expect long.
			Validate(8L, received[1]);

			List<object> receivedInner = (List<object>)received[2];
			ValidateSize(2, receivedInner.Count);
			Validate("string2", receivedInner[0]);
			Validate(5L, receivedInner[1]);

			Dictionary<object, object> receivedMap = (Dictionary<object, object>)received[3];
			ValidateSize(4, receivedMap.Count);
			Validate(1L, receivedMap["a"]);
			Validate("b", receivedMap[2L]);
			Validate(blob, (byte[])receivedMap[3L]);

			List<object> receivedInner2 = (List<object>)receivedMap["list"];
			ValidateSize(2, receivedInner2.Count);
			Validate("string2", receivedInner2[0]);
			Validate(5L, receivedInner2[1]);

			console.Info("Read/Write List/HashMap successful");
		}

		private static void ValidateSize(int expected, int received)
		{
			if (received != expected)
			{
				throw new Exception(string.Format("Size mismatch: expected={0} received={1}", expected, received));
			}
		}

		private static void Validate(object expected, object received)
		{
			if (!received.Equals(expected))
			{
				throw new Exception(string.Format("Mismatch: expected={0} received={1}", expected, received));
			}
		}

		private static void Validate(byte[] expected, byte[] received)
		{
			string expectedString = Util.BytesToString(expected);
			string receivedString = Util.BytesToString(received);

			if (!expectedString.Equals(receivedString))
			{
				throw new Exception(string.Format("Mismatch: expected={0} received={1}", expectedString, receivedString));
			}
		}
	}
}
