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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;

namespace Aerospike.Test
{
	[TestClass]
	public class TestListMap : TestSync
	{
		[TestMethod]
		public void ListStrings()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "listkey1");
			client.Delete(null, key);

			List<string> list = ["string1", "string2", "string3"];

			Bin bin = new(Suite.GetBinName("listbin1"), list);
			client.Put(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			IList receivedList = (IList) record.GetValue(bin.name);

			Assert.AreEqual(3, receivedList.Count);
			Assert.AreEqual("string1", receivedList[0]);
			Assert.AreEqual("string2", receivedList[1]);
			Assert.AreEqual("string3", receivedList[2]);
		}

		[TestMethod]
		public void ListComplex()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "listkey2");
			client.Delete(null, key);

			string geopoint = "{ \"type\": \"Point\", \"coordinates\": [0.00, 0.00] }";

			byte[] blob = [3, 52, 125];
			List<object> list = ["string1", 2, blob, Value.GetAsGeoJSON(geopoint)];

			Bin bin = new(Suite.GetBinName("listbin2"), list);
			client.Put(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			IList receivedList = (IList)record.GetValue(bin.name);

			Assert.AreEqual(4, receivedList.Count);
			Assert.AreEqual("string1", receivedList[0]);
			// Server convert numbers to long, so must expect long.
			Assert.AreEqual(2L, receivedList[1]);
			CollectionAssert.AreEqual(blob, (byte[])receivedList[2]);
			Assert.AreEqual(Value.GetAsGeoJSON(geopoint), (Value)receivedList[3]);
		}

		[TestMethod]
		public void MapStrings()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mapkey1");
			client.Delete(null, key);

			Dictionary<string, string> map = new()
			{
				["key1"] = "string1",
				["key2"] = "loooooooooooooooooooooooooongerstring2",
				["key3"] = "string3"
			};

			Bin bin = new(Suite.GetBinName("mapbin1"), map);
			client.Put(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			IDictionary receivedMap = (IDictionary) record.GetValue(bin.name);

			Assert.AreEqual(3, receivedMap.Count);
			Assert.AreEqual("string1", receivedMap["key1"]);
			Assert.AreEqual("loooooooooooooooooooooooooongerstring2", receivedMap["key2"]);
			Assert.AreEqual("string3", receivedMap["key3"]);
		}

		[TestMethod]
		public void MapComplex()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mapkey2");
			client.Delete(null, key);

			byte[] blob = [3, 52, 125];
			IList<int> list = [100034, 12384955, 3, 512];

			DateTime dt = new(2019, 9, 23, 11, 24, 1);
			Decimal dc = new(1.1);

			Dictionary<object, object> map = new()
			{
				["key1"] = "string1",
				["key2"] = 2,
				["key3"] = blob,
				["key4"] = list,
				["key5"] = true,
				["key6"] = false
			};
#if BINARY_FORMATTER
			map["key7"] = dt;
			map["key8"] = dc;
#endif

            Bin bin = new(Suite.GetBinName("mapbin2"), map);
			client.Put(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			IDictionary receivedMap = (IDictionary) record.GetValue(bin.name);

#if BINARY_FORMATTER
			Assert.AreEqual(8, receivedMap.Count);
#else
            Assert.AreEqual(6, receivedMap.Count);
#endif
			Assert.AreEqual("string1", receivedMap["key1"]);
			// Server convert numbers to long, so must expect long.
			Assert.AreEqual(2L, receivedMap["key2"]);
			CollectionAssert.AreEqual(blob, (byte[])receivedMap["key3"]);

			IList receivedInner = (IList)receivedMap["key4"];
			Assert.AreEqual(4, receivedInner.Count);
			Assert.AreEqual(100034L, receivedInner[0]);
			Assert.AreEqual(12384955L, receivedInner[1]);
			Assert.AreEqual(3L, receivedInner[2]);
			Assert.AreEqual(512L, receivedInner[3]);

			Assert.IsTrue((bool?)receivedMap["key5"]);
			Assert.IsFalse((bool?)receivedMap["key6"]);
#if BINARY_FORMATTER
			Assert.AreEqual(dt, receivedMap["key7"]);
			Assert.AreEqual(dc, receivedMap["key8"]);
#endif
		}

		[TestMethod]
		public void ListMapCombined()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "listmapkey");
			client.Delete(null, key);

			byte[] blob = [3, 52, 125];
			List<object> inner = ["string2", 5];

			Dictionary<object, object> innerMap = new()
			{
				["a"] = 1,
				[2] = "b",
				[3] = blob,
				["list"] = inner
			};

			List<object> list = ["string1", 8, inner, innerMap];

			Bin bin = new(Suite.GetBinName("listmapbin"), list);
			client.Put(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			IList received = (IList) record.GetValue(bin.name);

			Assert.AreEqual(4, received.Count);
			Assert.AreEqual("string1", received[0]);
			// Server convert numbers to long, so must expect long.
			Assert.AreEqual(8L, received[1]);

			IList receivedInner = (IList)received[2];
			Assert.AreEqual(2, receivedInner.Count);
			Assert.AreEqual("string2", receivedInner[0]);
			Assert.AreEqual(5L, receivedInner[1]);

			IDictionary receivedMap = (IDictionary)received[3];
			Assert.AreEqual(4, receivedMap.Count);
			Assert.AreEqual(1L, receivedMap["a"]);
			Assert.AreEqual("b", receivedMap[2L]);
			CollectionAssert.AreEqual(blob, (byte[])receivedMap[3L]);

			IList receivedInner2 = (IList)receivedMap["list"];
			Assert.AreEqual(2, receivedInner2.Count);
			Assert.AreEqual("string2", receivedInner2[0]);
			Assert.AreEqual(5L, receivedInner2[1]);
		}
	};
}
