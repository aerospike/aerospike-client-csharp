using System;
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
			Validate((byte)2, receivedList[1]);
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
			Dictionary<object, object> map = new Dictionary<object, object>();
			map["key1"] = "string1";
			map["key2"] = 2;
			map["key3"] = blob;

			Bin bin = Bin.AsMap(args.GetBinName("mapbin2"), map);
			client.Put(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);
			Dictionary<object, object> receivedMap = (Dictionary<object, object>)record.GetValue(bin.name);

			ValidateSize(3, receivedMap.Count);
			Validate("string1", receivedMap["key1"]);
			// Server convert numbers to long, so must expect long.
			Validate((byte)2, receivedMap["key2"]);
			Validate(blob, (byte[])receivedMap["key3"]);

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
			Validate((byte)8, received[1]);

			List<object> receivedInner = (List<object>)received[2];
			ValidateSize(2, receivedInner.Count);
			Validate("string2", receivedInner[0]);
			Validate((byte)5, receivedInner[1]);

			Dictionary<object, object> receivedMap = (Dictionary<object, object>)received[3];
			ValidateSize(4, receivedMap.Count);
			Validate((byte)1, receivedMap["a"]);
			Validate("b", receivedMap[(byte)2]);
			Validate(blob, (byte[])receivedMap[(byte)3]);

			List<object> receivedInner2 = (List<object>)receivedMap["list"];
			ValidateSize(2, receivedInner2.Count);
			Validate("string2", receivedInner2[0]);
			Validate((byte)5, receivedInner2[1]);

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