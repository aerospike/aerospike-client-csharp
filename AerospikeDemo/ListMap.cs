/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using System;
using System.Collections;
using System.Collections.Generic;

namespace Aerospike.Demo
{
	public class ListMap : SyncExample
	{
		public ListMap(Console console) : base(console)
		{
		}

		/// <summary>
		/// Write List and Map objects.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			TestListStrings(client, args);
			TestListComplex(client, args);
			TestMapStrings(client, args);
			TestMapComplex(client, args);
			TestListMapCombined(client, args);

#if BINARY_FORMATTER
			TestListCompoundBlob(client, args);
			TestListCompoundList(client, args);
#endif
		}

		/// <summary>
		/// Write/Read ArrayList<String> directly instead of relying on default serializer.
		/// </summary>
		private void TestListStrings(IAerospikeClient client, Arguments args)
		{
			console.Info("Read/Write ArrayList<String>");
			Key key = new Key(args.ns, args.set, "listkey1");
			client.Delete(args.writePolicy, key);

			List<object> list = new List<object>();
			list.Add("string1");
			list.Add("string2");
			list.Add("string3");

			Bin bin = new Bin(args.GetBinName("listbin1"), list);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			List<object> receivedList = (List<object>)record.GetValue(bin.name);

			ValidateSize(3, receivedList.Count);
			Validate("string1", receivedList[0]);
			Validate("string2", receivedList[1]);
			Validate("string3", receivedList[2]);

			console.Info("Read/Write ArrayList<String> successful.");
		}

		/// <summary>
		/// Write/Read ArrayList<Object> directly instead of relying on default serializer.
		/// </summary>
		private void TestListComplex(IAerospikeClient client, Arguments args)
		{
			console.Info("Read/Write ArrayList<Object>");
			Key key = new Key(args.ns, args.set, "listkey2");
			client.Delete(args.writePolicy, key);

			byte[] blob = new byte[] { 3, 52, 125 };
			List<object> list = new List<object>();
			list.Add("string1");
			list.Add(2);
			list.Add(blob);

			Bin bin = new Bin(args.GetBinName("listbin2"), list);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			List<object> receivedList = (List<object>)record.GetValue(bin.name);

			ValidateSize(3, receivedList.Count);
			Validate("string1", receivedList[0]);
			// Server convert numbers to long, so must expect long.
			Validate(2L, receivedList[1]);
			Validate(blob, (byte[])receivedList[2]);

			console.Info("Read/Write ArrayList<Object> successful.");
		}

		/// <summary>
		/// Write/Read HashMap<String,String> directly instead of relying on default serializer.
		/// </summary>
		private void TestMapStrings(IAerospikeClient client, Arguments args)
		{
			console.Info("Read/Write HashMap<String,String>");
			Key key = new Key(args.ns, args.set, "mapkey1");
			client.Delete(args.writePolicy, key);

			Dictionary<object, object> map = new Dictionary<object, object>();
			map["key1"] = "string1";
			map["key2"] = "string2";
			map["key3"] = "string3";

			Bin bin = new Bin(args.GetBinName("mapbin1"), map);
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
		/// Write/Read HashMap<Object,Object> directly instead of relying on default serializer.
		/// </summary>
		private void TestMapComplex(IAerospikeClient client, Arguments args)
		{
			console.Info("Read/Write HashMap<Object,Object>");
			Key key = new Key(args.ns, args.set, "mapkey2");
			client.Delete(args.writePolicy, key);

			byte[] blob = new byte[] { 3, 52, 125 };
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

			Bin bin = new Bin(args.GetBinName("mapbin2"), map);
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
		/// Write/Read List/HashMap combination directly instead of relying on default serializer.
		/// </summary>
		private void TestListMapCombined(IAerospikeClient client, Arguments args)
		{
			console.Info("Read/Write List/HashMap");
			Key key = new Key(args.ns, args.set, "listmapkey");
			client.Delete(args.writePolicy, key);

			byte[] blob = new byte[] { 3, 52, 125 };
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

			Bin bin = new Bin(args.GetBinName("listmapbin"), list);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			List<object> received = (List<object>)record.GetValue(bin.name);

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

#if BINARY_FORMATTER
		/// <summary>
		/// Write/Read list of compound objects using blob for entire list 
		/// which is slow becauses it uses the default serializer.
		/// </summary>
		private void TestListCompoundBlob(IAerospikeClient client, Arguments args)
		{
			console.Info("Read/Write ArrayList<CompoundObject> using blob");
			Key key = new Key(args.ns, args.set, "listkey4");
			client.Delete(args.writePolicy, key);

			List<CompoundObject> list = new List<CompoundObject>();
			list.Add(new CompoundObject("string1", 7));
			list.Add(new CompoundObject("string2", 9));
			list.Add(new CompoundObject("string3", 54));

			Bin bin = new Bin("listbin", list);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			IList receivedList = (IList)record.GetValue(bin.name);

			ValidateSize(3, receivedList.Count);
			Validate(list[0], receivedList[0]);
			Validate(list[1], receivedList[1]);
			Validate(list[2], receivedList[2]);

			console.Info("Read/Write ArrayList<CompoundObject> successful.");
		}

		/// <summary>
		/// Write/Read list of compound objects using Aerospike list type with blob entries (Bin.AsList()).
		/// </summary>
		private void TestListCompoundList(AerospikeClient client, Arguments args)
		{
			console.Info("Read/Write ArrayList<CompoundObject> using list with blob entries");
			Key key = new Key(args.ns, args.set, "listkey5");
			client.Delete(args.writePolicy, key);

			List<CompoundObject> list = new List<CompoundObject>();
			list.Add(new CompoundObject("string1", 7));
			list.Add(new CompoundObject("string2", 9));
			list.Add(new CompoundObject("string3", 54));

			Bin bin = new Bin("listbin", list);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			IList receivedList = (IList)record.GetValue(bin.name);

			ValidateSize(3, receivedList.Count);
			Validate(list[0], receivedList[0]);
			Validate(list[1], receivedList[1]);
			Validate(list[2], receivedList[2]);

			console.Info("Read/Write ArrayList<CompoundObject> successful.");
		}
#endif

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

	[Serializable]
	class CompoundObject
	{
		public string a;
		public int b;

		public CompoundObject(string a, int b)
		{
			this.a = a;
			this.b = b;
		}

		public override bool Equals(object other)
		{
			CompoundObject o = (CompoundObject)other;
			return this.a.Equals(o.a) && this.b == o.b;
		}

		public override int GetHashCode()
		{
			return a.GetHashCode() + b;
		}
	}
}
