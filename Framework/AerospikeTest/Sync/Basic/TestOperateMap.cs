/* 
 * Copyright 2012-2018 Aerospike, Inc.
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

namespace Aerospike.Test
{
	[TestClass]
	public class TestOperateMap : TestSync
	{
		private const string binName = "opmapbin";

		[TestMethod]
		public void OperateMapPut()
		{
			if (! args.ValidateMap()) {
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey1");		
			client.Delete(null, key);
		
			MapPolicy putMode = MapPolicy.Default;
			MapPolicy addMode = new MapPolicy(MapOrder.UNORDERED, MapWriteMode.CREATE_ONLY);
			MapPolicy updateMode = new MapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE_ONLY);
			MapPolicy orderedUpdateMode = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE_ONLY);
		
			// Calling put() multiple times performs poorly because the server makes
			// a copy of the map for each call, but we still need to test it.
			// putItems() should be used instead for best performance.
			Record record = client.Operate(null, key,
					MapOperation.Put(putMode, binName, Value.Get(11), Value.Get(789)),
					MapOperation.Put(putMode, binName, Value.Get(10), Value.Get(999)),
					MapOperation.Put(addMode, binName, Value.Get(12), Value.Get(500)),
					MapOperation.Put(addMode, binName, Value.Get(15), Value.Get(1000)),
					// Ordered type should be ignored since map has already been created in first put().
					MapOperation.Put(orderedUpdateMode, binName, Value.Get(10), Value.Get(1)),
					MapOperation.Put(updateMode, binName, Value.Get(15), Value.Get(5))
					);
		
			AssertRecordFound(key, record);
				
			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];	
			Assert.AreEqual(1, size);
		
			size = (long)results[i++];	
			Assert.AreEqual(2, size);
		
			size = (long)results[i++];	
			Assert.AreEqual(3, size);
		
			size = (long)results[i++];	
			Assert.AreEqual(4, size);
		
			size = (long)results[i++];	
			Assert.AreEqual(4, size);
		
			size = (long)results[i++];	
			Assert.AreEqual(4, size);

			record = client.Get(null, key, binName);
		
			IDictionary map = record.GetMap(binName);	
			Assert.AreEqual(4, map.Count);
			Assert.AreEqual(1L, map[10L]);
		}

		[TestMethod]
		public void OperateMapPutItems()
		{
			if (!args.ValidateMap())
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey2");
			client.Delete(null, key);

			Dictionary<Value, Value> addMap = new Dictionary<Value, Value>();
			addMap[Value.Get(12)] = Value.Get("myval");
			addMap[Value.Get(-8734)] = Value.Get("str2");
			addMap[Value.Get(1)] = Value.Get("my default");

			Dictionary<Value, Value> putMap = new Dictionary<Value, Value>();
			putMap[Value.Get(12)] = Value.Get("myval12222");
			putMap[Value.Get(13)] = Value.Get("str13");

			Dictionary<Value, Value> updateMap = new Dictionary<Value, Value>();
			updateMap[Value.Get(13)] = Value.Get("myval2");

			Dictionary<Value, Value> replaceMap = new Dictionary<Value, Value>();
			replaceMap[Value.Get(12)] = Value.Get(23);
			replaceMap[Value.Get(-8734)] = Value.Get("changed");

			MapPolicy putMode = MapPolicy.Default;
			MapPolicy addMode = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.CREATE_ONLY);
			MapPolicy updateMode = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE_ONLY);

			Record record = client.Operate(null, key,
				MapOperation.PutItems(addMode, binName, addMap),
				MapOperation.PutItems(putMode, binName, putMap),
				MapOperation.PutItems(updateMode, binName, updateMap),
				MapOperation.PutItems(updateMode, binName, replaceMap),
				MapOperation.GetByKey(binName, Value.Get(1), MapReturnType.VALUE),
				MapOperation.GetByKey(binName, Value.Get(-8734), MapReturnType.VALUE),
				MapOperation.GetByKeyRange(binName, Value.Get(12), Value.Get(15), MapReturnType.KEY_VALUE)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(3, size);

			size = (long)results[i++];
			Assert.AreEqual(4, size);

			size = (long)results[i++];
			Assert.AreEqual(4, size);

			size = (long)results[i++];
			Assert.AreEqual(4, size);

			string str = (string)results[i++];
			Assert.AreEqual("my default", str);

			str = (string)results[i++];
			Assert.AreEqual("changed", str);

			IList list = (IList)results[i++];
			Assert.AreEqual(2, list.Count);
		}

		[TestMethod]
		public void OperateMapMixed()
		{
			// Test normal operations with map operations.
			if (!args.ValidateMap())
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey2");
			client.Delete(null, key);

			Dictionary<Value, Value> itemMap = new Dictionary<Value, Value>();
			itemMap[Value.Get(12)] = Value.Get("myval");
			itemMap[Value.Get(-8734)] = Value.Get("str2");
			itemMap[Value.Get(1)] = Value.Get("my default");
			itemMap[Value.Get(7)] = Value.Get(1);

			Record record = client.Operate(null, key,
				MapOperation.PutItems(new MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE), binName, itemMap),
				Operation.Put(new Bin("otherbin", "hello"))
				);

			AssertRecordFound(key, record);

			long size = record.GetLong(binName);
			Assert.AreEqual(4, size);

			record = client.Operate(null, key,
				MapOperation.GetByKey(binName, Value.Get(12), MapReturnType.INDEX),
				Operation.Append(new Bin("otherbin", Value.Get("goodbye"))),
				Operation.Get("otherbin")
				);

			AssertRecordFound(key, record);

			long index = record.GetLong(binName);
			Assert.AreEqual(3, index);

			IList results = record.GetList("otherbin");
			string val = (string)results[1];
			Assert.AreEqual("hellogoodbye", val);
		}

		[TestMethod]
		public void OperateMapSwitch()
		{
			// Switch from unordered map to a key ordered map.
			if (!args.ValidateMap())
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey4");
			client.Delete(null, key);

			Record record = client.Operate(null, key,
				MapOperation.Put(MapPolicy.Default, binName, Value.Get(4), Value.Get(4)),
				MapOperation.Put(MapPolicy.Default, binName, Value.Get(3), Value.Get(3)),
				MapOperation.Put(MapPolicy.Default, binName, Value.Get(2), Value.Get(2)),
				MapOperation.Put(MapPolicy.Default, binName, Value.Get(1), Value.Get(1)),
				MapOperation.GetByIndex(binName, 2, MapReturnType.KEY_VALUE),
				MapOperation.GetByIndexRange(binName, 0, 10, MapReturnType.KEY_VALUE)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 3;

			long size = (long)results[i++];
			Assert.AreEqual(4, size);

			IList list = (IList)results[i++];
			KeyValuePair<object,object> entry = (KeyValuePair<object,object>)list[0];
			Assert.AreEqual(3L, entry.Key);
			Assert.AreEqual(3L, entry.Value);

			list = (IList)results[i++];
			Assert.AreEqual(4, list.Count);
			entry = (KeyValuePair<object,object>)list[1];
			Assert.AreEqual(2L, entry.Key);

			record = client.Operate(null, key,
				MapOperation.SetMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), binName),
				MapOperation.GetByKeyRange(binName, Value.Get(3), Value.Get(5), MapReturnType.COUNT),
				MapOperation.GetByKeyRange(binName, Value.Get(-5), Value.Get(2), MapReturnType.KEY_VALUE),
				MapOperation.GetByIndexRange(binName, 0, 10, MapReturnType.KEY_VALUE));

			AssertRecordFound(key, record);

			results = record.GetList(binName);
			i = 0;

			object obj = results[i++];
			Assert.IsNull(obj);

			long val = (long)results[i++];
			Assert.AreEqual(2, val);

			list = (IList)results[i++];
			Assert.AreEqual(1, list.Count);
			entry = (KeyValuePair<object,object>)list[0];
			Assert.AreEqual(1L, entry.Value);

			list = (IList)results[i++];
			entry = (KeyValuePair<object,object>)list[3];
			Assert.AreEqual(4L, entry.Key);
		}

		[TestMethod]
		public void OperateMapRank()
		{
			// Test rank.
			if (!args.ValidateMap())
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey6");
			client.Delete(null, key);

			Dictionary<Value, Value> inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("Charlie")] = Value.Get(55);
			inputMap[Value.Get("Jim")] = Value.Get(98);
			inputMap[Value.Get("John")] = Value.Get(76);
			inputMap[Value.Get("Harry")] = Value.Get(82);

			// Write values to empty map.
			Record record = client.Operate(null, key, MapOperation.PutItems(MapPolicy.Default, binName, inputMap));

			AssertRecordFound(key, record);

			// Increment some user scores.
			record = client.Operate(null, key,
				MapOperation.Increment(MapPolicy.Default, binName, Value.Get("John"), Value.Get(5)),
				MapOperation.Decrement(MapPolicy.Default, binName, Value.Get("Jim"), Value.Get(4))
				);

			AssertRecordFound(key, record);

			// Get scores.
			record = client.Operate(null, key,
				MapOperation.GetByRankRange(binName, -2, 2, MapReturnType.KEY),
				MapOperation.GetByRankRange(binName, 0, 2, MapReturnType.KEY_VALUE),
				MapOperation.GetByRank(binName, 0, MapReturnType.VALUE),
				MapOperation.GetByRank(binName, 2, MapReturnType.KEY),
				MapOperation.GetByValueRange(binName, Value.Get(90), Value.Get(95), MapReturnType.RANK),
				MapOperation.GetByValueRange(binName, Value.Get(90), Value.Get(95), MapReturnType.COUNT),
				MapOperation.GetByValueRange(binName, Value.Get(90), Value.Get(95), MapReturnType.KEY_VALUE),
				MapOperation.GetByValueRange(binName, Value.Get(81), Value.Get(82), MapReturnType.KEY),
				MapOperation.GetByValue(binName, Value.Get(77), MapReturnType.KEY),
				MapOperation.GetByValue(binName, Value.Get(81), MapReturnType.RANK),
				MapOperation.GetByKey(binName, Value.Get("Charlie"), MapReturnType.RANK),
				MapOperation.GetByKey(binName, Value.Get("Charlie"), MapReturnType.REVERSE_RANK)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			IList list = (IList)results[i++];
			string str;
			long val;

			str = (string)list[0];
			Assert.AreEqual("Harry", str);
			str = (string)list[1];
			Assert.AreEqual("Jim", str);

			list = (IList)results[i++];
			KeyValuePair<object,object> entry = (KeyValuePair<object,object>)list[0];
			Assert.AreEqual("Charlie", entry.Key);
			Assert.AreEqual(55L, entry.Value);
			entry = (KeyValuePair<object,object>)list[1];
			Assert.AreEqual("John", entry.Key);
			Assert.AreEqual(81L, entry.Value);

			val = (long)results[i++];
			Assert.AreEqual(55, val);

			str = (string)results[i++];
			Assert.AreEqual("Harry", str);

			list = (IList)results[i++];
			val = (long)list[0];
			Assert.AreEqual(3, val);

			val = (long)results[i++];
			Assert.AreEqual(1, val);

			list = (IList)results[i++];
			entry = (KeyValuePair<object,object>)list[0];
			Assert.AreEqual("Jim", entry.Key);
			Assert.AreEqual(94L, entry.Value);

			list = (IList)results[i++];
			str = (string)list[0];
			Assert.AreEqual("John", str);

			list = (IList)results[i++];
			Assert.AreEqual(0, list.Count);

			list = (IList)results[i++];
			val = (long)list[0];
			Assert.AreEqual(1, val);

			val = (long)results[i++];
			Assert.AreEqual(0, val);

			val = (long)results[i++];
			Assert.AreEqual(3, val);
		}

		[TestMethod]
		public void OperateMapRemove()
		{
			// Test remove.
			if (!args.ValidateMap())
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey7");
			client.Delete(null, key);

			Dictionary<Value, Value> inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("Charlie")] = Value.Get(55);
			inputMap[Value.Get("Jim")] = Value.Get(98);
			inputMap[Value.Get("John")] = Value.Get(76);
			inputMap[Value.Get("Harry")] = Value.Get(82);
			inputMap[Value.Get("Sally")] = Value.Get(79);
			inputMap[Value.Get("Lenny")] = Value.Get(84);
			inputMap[Value.Get("Abe")] = Value.Get(88);

			List<Value> removeItems = new List<Value>();
			removeItems.Add(Value.Get("Sally"));
			removeItems.Add(Value.Get("UNKNOWN"));
			removeItems.Add(Value.Get("Lenny"));

			Record record = client.Operate(null, key,
				MapOperation.PutItems(MapPolicy.Default, binName, inputMap),
				MapOperation.RemoveByKey(binName, Value.Get("NOTFOUND"), MapReturnType.VALUE),
				MapOperation.RemoveByKey(binName, Value.Get("Jim"), MapReturnType.VALUE),
				MapOperation.RemoveByKeyList(binName, removeItems, MapReturnType.COUNT),
				MapOperation.RemoveByValue(binName, Value.Get(55), MapReturnType.KEY),
				MapOperation.Size(binName));

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long val = (long)results[i++];
			Assert.AreEqual(7, val);

			object obj = results[i++];
			Assert.IsNull(obj);

			val = (long)results[i++];
			Assert.AreEqual(98, val);

			val = (long)results[i++];
			Assert.AreEqual(2, val);

			IList list = (IList)results[i++];
			Assert.AreEqual(1, list.Count);
			Assert.AreEqual("Charlie", (string)list[0]);

			val = (long)results[i++];
			Assert.AreEqual(3, val);
		}

		[TestMethod]
		public void OperateMapRemoveRange()
		{
			// Test remove ranges.
			if (!args.ValidateMap())
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey8");
			client.Delete(null, key);

			Dictionary<Value, Value> inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("Charlie")] = Value.Get(55);
			inputMap[Value.Get("Jim")] = Value.Get(98);
			inputMap[Value.Get("John")] = Value.Get(76);
			inputMap[Value.Get("Harry")] = Value.Get(82);
			inputMap[Value.Get("Sally")] = Value.Get(79);
			inputMap[Value.Get("Lenny")] = Value.Get(84);
			inputMap[Value.Get("Abe")] = Value.Get(88);

			Record record = client.Operate(null, key,
				MapOperation.PutItems(MapPolicy.Default, binName, inputMap),
				MapOperation.RemoveByKeyRange(binName, Value.Get("J"), Value.Get("K"), MapReturnType.COUNT),
				MapOperation.RemoveByValueRange(binName, Value.Get(80), Value.Get(85), MapReturnType.COUNT),
				MapOperation.RemoveByIndexRange(binName, 0, 2, MapReturnType.COUNT),
				MapOperation.RemoveByRankRange(binName, 0, 2, MapReturnType.COUNT)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long val = (long)results[i++];
			Assert.AreEqual(7, val);

			val = (long)results[i++];
			Assert.AreEqual(2, val);

			val = (long)results[i++];
			Assert.AreEqual(2, val);

			val = (long)results[i++];
			Assert.AreEqual(2, val);

			val = (long)results[i++];
			Assert.AreEqual(1, val);
		}

		[TestMethod]
		public void OperateMapClear()
		{
			// Test clear.
			if (!args.ValidateMap())
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey9");
			client.Delete(null, key);

			Dictionary<Value, Value> inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("Charlie")] = Value.Get(55);
			inputMap[Value.Get("Jim")] = Value.Get(98);

			Record record = client.Operate(null, key, MapOperation.PutItems(MapPolicy.Default, binName, inputMap));

			AssertRecordFound(key, record);

			long size = record.GetLong(binName);
			Assert.AreEqual(2, size);

			record = client.Operate(null, key,
				MapOperation.Clear(binName),
				MapOperation.Size(binName)
				);

			IList results = record.GetList(binName);
			size = (long)results[1];
			Assert.AreEqual(0, size);
		}

		[TestMethod]
		public void OperateMapScore()
		{
			// Test score.
			if (!args.ValidateMap())
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opmkey10");
			client.Delete(null, key);

			MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE);

			Dictionary<Value, Value> inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("weiling")] = Value.Get(0);
			inputMap[Value.Get("briann")] = Value.Get(0);
			inputMap[Value.Get("brianb")] = Value.Get(0);
			inputMap[Value.Get("meher")] = Value.Get(0);

			// Create map.
			Record record = client.Operate(null, key, MapOperation.PutItems(mapPolicy, binName, inputMap));

			AssertRecordFound(key, record);

			// Change scores
			record = client.Operate(null, key,
				MapOperation.Increment(mapPolicy, binName, Value.Get("weiling"), Value.Get(10)),
				MapOperation.Increment(mapPolicy, binName, Value.Get("briann"), Value.Get(20)),
				MapOperation.Increment(mapPolicy, binName, Value.Get("brianb"), Value.Get(1)),
				MapOperation.Increment(mapPolicy, binName, Value.Get("meher"), Value.Get(20))
				);

			AssertRecordFound(key, record);

			// Query top 3 scores
			record = client.Operate(null, key, MapOperation.GetByRankRange(binName, -3, 3, MapReturnType.KEY));

			AssertRecordFound(key, record);

			// Remove people with score 10 and display top 3 again
			record = client.Operate(null, key,
				MapOperation.RemoveByValue(binName, Value.Get(10), MapReturnType.KEY),
				MapOperation.GetByRankRange(binName, -3, 3, MapReturnType.KEY)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;
			IList list = (IList)results[i++];
			string s = (string)list[0];
			Assert.AreEqual("weiling", s);

			list = (IList)results[i++];
			s = (string)list[0];
			Assert.AreEqual("brianb", s);
			s = (string)list[1];
			Assert.AreEqual("briann", s);
			s = (string)list[2];
			Assert.AreEqual("meher", s);
		}	
	}
}
