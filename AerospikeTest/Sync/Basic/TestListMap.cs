﻿/* 
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
using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;
using Microsoft.AspNetCore.DataProtection.KeyManagement;

namespace Aerospike.Test
{
	[TestClass]
	public class TestListMap : TestSync
	{
		[TestMethod]
		public async Task ListStrings()
		{
			Key key = new Key(args.ns, args.set, "listkey1");
			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				List<string> list = new List<string>();
				list.Add("string1");
				list.Add("string2");
				list.Add("string3");

				Bin bin = new Bin(args.GetBinName("listbin1"), list);
				client.Put(null, key, bin);

				Record record = client.Get(null, key, bin.name);
				IList receivedList = (IList)record.GetValue(bin.name);

				Assert.AreEqual(3, receivedList.Count);
				Assert.AreEqual("string1", receivedList[0]);
				Assert.AreEqual("string2", receivedList[1]);
				Assert.AreEqual("string3", receivedList[2]);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				List<string> list = new List<string>();
				list.Add("string1");
				list.Add("string2");
				list.Add("string3");

				Bin bin = new Bin(args.GetBinName("listbin1"), list);
				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);

				Record record = await asyncAwaitClient.Get(null, key, new[] { bin.name }, CancellationToken.None);
				IList receivedList = (IList)record.GetValue(bin.name);

				Assert.AreEqual(3, receivedList.Count);
				Assert.AreEqual("string1", receivedList[0]);
				Assert.AreEqual("string2", receivedList[1]);
				Assert.AreEqual("string3", receivedList[2]);
			}
		}

		[TestMethod]
		public async Task ListComplex()
		{
			Key key = new Key(args.ns, args.set, "listkey2");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				string geopoint = "{ \"type\": \"Point\", \"coordinates\": [0.00, 0.00] }";

				byte[] blob = new byte[] { 3, 52, 125 };
				List<object> list = new List<object>();
				list.Add("string1");
				list.Add(2);
				list.Add(blob);
				list.Add(Value.GetAsGeoJSON(geopoint));

				Bin bin = new Bin(args.GetBinName("listbin2"), list);
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
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				string geopoint = "{ \"type\": \"Point\", \"coordinates\": [0.00, 0.00] }";

				byte[] blob = new byte[] { 3, 52, 125 };
				List<object> list = new List<object>();
				list.Add("string1");
				list.Add(2);
				list.Add(blob);
				list.Add(Value.GetAsGeoJSON(geopoint));

				Bin bin = new Bin(args.GetBinName("listbin2"), list);
				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);

				Record record = await asyncAwaitClient.Get(null, key, new[] { bin.name }, CancellationToken.None);
				IList receivedList = (IList)record.GetValue(bin.name);

				Assert.AreEqual(4, receivedList.Count);
				Assert.AreEqual("string1", receivedList[0]);
				// Server convert numbers to long, so must expect long.
				Assert.AreEqual(2L, receivedList[1]);
				CollectionAssert.AreEqual(blob, (byte[])receivedList[2]);
				Assert.AreEqual(Value.GetAsGeoJSON(geopoint), (Value)receivedList[3]);
			}
		}

		[TestMethod]
		public async Task MapStrings()
		{
			Key key = new Key(args.ns, args.set, "mapkey1");
			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				Dictionary<string, string> map = new Dictionary<string, string>();
				map["key1"] = "string1";
				map["key2"] = "loooooooooooooooooooooooooongerstring2";
				map["key3"] = "string3";

				Bin bin = new Bin(args.GetBinName("mapbin1"), map);
				client.Put(null, key, bin);

				Record record = client.Get(null, key, bin.name);
				IDictionary receivedMap = (IDictionary)record.GetValue(bin.name);

				Assert.AreEqual(3, receivedMap.Count);
				Assert.AreEqual("string1", receivedMap["key1"]);
				Assert.AreEqual("loooooooooooooooooooooooooongerstring2", receivedMap["key2"]);
				Assert.AreEqual("string3", receivedMap["key3"]);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				Dictionary<string, string> map = new Dictionary<string, string>();
				map["key1"] = "string1";
				map["key2"] = "loooooooooooooooooooooooooongerstring2";
				map["key3"] = "string3";

				Bin bin = new Bin(args.GetBinName("mapbin1"), map);
				client.Put(null, key, bin);

				Record record = await asyncAwaitClient.Get(null, key, new[] { bin.name }, CancellationToken.None);
				IDictionary receivedMap = (IDictionary)record.GetValue(bin.name);

				Assert.AreEqual(3, receivedMap.Count);
				Assert.AreEqual("string1", receivedMap["key1"]);
				Assert.AreEqual("loooooooooooooooooooooooooongerstring2", receivedMap["key2"]);
				Assert.AreEqual("string3", receivedMap["key3"]);
			}
		}

		[TestMethod]
		public async Task MapComplex()
		{
			Key key = new Key(args.ns, args.set, "mapkey2");
			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] blob = new byte[] { 3, 52, 125 };
				IList<int> list = new List<int>();
				list.Add(100034);
				list.Add(12384955);
				list.Add(3);
				list.Add(512);

				DateTime dt = new DateTime(2019, 9, 23, 11, 24, 1);
				Decimal dc = new decimal(1.1);

				Dictionary<object, object> map = new Dictionary<object, object>();
				map["key1"] = "string1";
				map["key2"] = 2;
				map["key3"] = blob;
				map["key4"] = list;
				map["key5"] = true;
				map["key6"] = false;
#if BINARY_FORMATTER
			map["key7"] = dt;
			map["key8"] = dc;
#endif

				Bin bin = new Bin(args.GetBinName("mapbin2"), map);
				client.Put(null, key, bin);

				Record record = client.Get(null, key, bin.name);
				IDictionary receivedMap = (IDictionary)record.GetValue(bin.name);

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

				Assert.AreEqual(true, receivedMap["key5"]);
				Assert.AreEqual(false, receivedMap["key6"]);
#if BINARY_FORMATTER
			Assert.AreEqual(dt, receivedMap["key7"]);
			Assert.AreEqual(dc, receivedMap["key8"]);
#endif

			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] blob = new byte[] { 3, 52, 125 };
				IList<int> list = new List<int>();
				list.Add(100034);
				list.Add(12384955);
				list.Add(3);
				list.Add(512);

				DateTime dt = new DateTime(2019, 9, 23, 11, 24, 1);
				Decimal dc = new decimal(1.1);

				Dictionary<object, object> map = new Dictionary<object, object>();
				map["key1"] = "string1";
				map["key2"] = 2;
				map["key3"] = blob;
				map["key4"] = list;
				map["key5"] = true;
				map["key6"] = false;
#if BINARY_FORMATTER
			map["key7"] = dt;
			map["key8"] = dc;
#endif

				Bin bin = new Bin(args.GetBinName("mapbin2"), map);
				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);

				Record record = await asyncAwaitClient.Get(null, key, new[] { bin.name }, CancellationToken.None);
				IDictionary receivedMap = (IDictionary)record.GetValue(bin.name);

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

				Assert.AreEqual(true, receivedMap["key5"]);
				Assert.AreEqual(false, receivedMap["key6"]);
#if BINARY_FORMATTER
			Assert.AreEqual(dt, receivedMap["key7"]);
			Assert.AreEqual(dc, receivedMap["key8"]);
#endif
			}
		}

		[TestMethod]
		public async Task ListMapCombined()
		{
			Key key = new Key(args.ns, args.set, "listmapkey");
			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

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
				client.Put(null, key, bin);

				Record record = client.Get(null, key, bin.name);
				IList received = (IList)record.GetValue(bin.name);

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
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

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
				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);

				Record record = await asyncAwaitClient.Get(null, key, new[] { bin.name }, CancellationToken.None);
				IList received = (IList)record.GetValue(bin.name);

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
		}
	};
}
