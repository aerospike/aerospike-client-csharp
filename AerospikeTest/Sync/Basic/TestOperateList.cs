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
using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using System.Security.Policy;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Test
{
	[TestClass]
	public class TestOperateList : TestSync
	{
		private const string binName = "oplistbin";

		[TestMethod]
		public void OperateList1()
		{
			Key key = new Key(args.ns, args.set, "oplkey1");

			client.Delete(null, key);

			// Calling append() multiple times performs poorly because the server makes
			// a copy of the list for each call, but we still need to test it.
			// Using appendItems() should be used instead for best performance.
			Record record = client.Operate(null, key,
				ListOperation.Append(binName, Value.Get(55)),
				ListOperation.Append(binName, Value.Get(77)),
				ListOperation.Pop(binName, -1),
				ListOperation.Size(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			long size = (long)list[0];
			Assert.AreEqual(1, size);

			size = (long)list[1];
			Assert.AreEqual(2, size);

			long val = (long)list[2];
			Assert.AreEqual(77, val);

			size = (long)list[3];
			Assert.AreEqual(1, size);
		}

		[TestMethod]
		public void OperateList2()
		{
			Key key = new Key(args.ns, args.set, "oplkey2");

			client.Delete(null, key);

			IList itemList = new List<Value>();
			itemList.Add(Value.Get(12));
			itemList.Add(Value.Get(-8734));
			itemList.Add(Value.Get("my string"));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(binName, itemList),
				Operation.Put(new Bin("otherbin", "hello"))
				);

			AssertRecordFound(key, record);

			record = client.Operate(null, key,
				ListOperation.Insert(binName, -1, Value.Get(8)),
				Operation.Append(new Bin("otherbin", Value.Get("goodbye"))),
				Operation.Get("otherbin"),
				ListOperation.GetRange(binName, 0, 4),
				ListOperation.GetRange(binName, 3)
				);

			AssertRecordFound(key, record);

			string val = record.GetString("otherbin");
			Assert.AreEqual("hellogoodbye", val);

			IList list = record.GetList(binName);

			long size = (long)list[0];
			Assert.AreEqual(4, size);

			IList rangeList = (IList)list[1];
			long lval = (long)rangeList[0];
			Assert.AreEqual(12, lval);

			lval = (long)rangeList[1];
			Assert.AreEqual(-8734, lval);

			lval = (long)rangeList[2];
			Assert.AreEqual(8, lval);

			val = (string)rangeList[3];
			Assert.AreEqual("my string", val);

			rangeList = (IList)list[2];
			val = (string)rangeList[0];
			Assert.AreEqual("my string", val);
		}

		[TestMethod]
		public void OperateList3()
		{
			// Test out of bounds conditions
			Key key = new Key(args.ns, args.set, "oplkey3");

			client.Delete(null, key);

			IList itemList = new List<Value>();
			itemList.Add(Value.Get("str1"));
			itemList.Add(Value.Get("str2"));
			itemList.Add(Value.Get("str3"));
			itemList.Add(Value.Get("str4"));
			itemList.Add(Value.Get("str5"));
			itemList.Add(Value.Get("str6"));
			itemList.Add(Value.Get("str7"));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(binName, itemList),
				ListOperation.Get(binName, 2),
				ListOperation.GetRange(binName, 6, 4),
				ListOperation.GetRange(binName, -7, 3),
				ListOperation.GetRange(binName, 0, 2),
				ListOperation.GetRange(binName, -2, 4)
				//ListOperation.Get(binName, 7), causes entire command to fail.
				//ListOperation.GetRange(binName, 7, 1), causes entire command to fail.
				//ListOperation.GetRange(binName, -8, 1) causes entire command to fail.
				//ListOperation.Get(binName, -8), causes entire command to fail.
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			long size = (long)list[0];
			Assert.AreEqual(7, size);

			Assert.AreEqual("str3", (string)list[1]);

			IList rangeList = (IList)list[2];
			Assert.AreEqual(1, rangeList.Count);
			Assert.AreEqual("str7", (string)rangeList[0]);

			rangeList = (IList)list[3];
			Assert.AreEqual(3, rangeList.Count);
			Assert.AreEqual("str1", (string)rangeList[0]);
			Assert.AreEqual("str2", (string)rangeList[1]);
			Assert.AreEqual("str3", (string)rangeList[2]);

			rangeList = (IList)list[4];
			Assert.AreEqual(2, rangeList.Count);
			Assert.AreEqual("str1", (string)rangeList[0]);
			Assert.AreEqual("str2", (string)rangeList[1]);

			rangeList = (IList)list[5];
			Assert.AreEqual(2, rangeList.Count);
			Assert.AreEqual("str6", (string)rangeList[0]);
			Assert.AreEqual("str7", (string)rangeList[1]);
		}

		[TestMethod]
		public void OperateList4()
		{
			// Test all value types.
			Key key = new Key(args.ns, args.set, "oplkey4");

			client.Delete(null, key);

			IList inputList = new List<Value>();
			inputList.Add(Value.Get(12));
			inputList.Add(Value.Get(-8734.81));
			inputList.Add(Value.Get("my string"));

			IDictionary inputMap = new Dictionary<int, string>();
			inputMap[9] = "data 9";
			inputMap[-2] = "data -2";

			byte[] bytes = System.Text.Encoding.UTF8.GetBytes("string bytes");

			IList itemList = new List<Value>();
			itemList.Add(Value.Get(true));
			itemList.Add(Value.Get(55));
			itemList.Add(Value.Get("string value"));
			itemList.Add(Value.Get(inputList));
			itemList.Add(Value.Get(bytes));
			itemList.Add(Value.Get(99.99));
			itemList.Add(Value.Get(inputMap));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(binName, itemList),
				ListOperation.GetRange(binName, 0, 100),
				ListOperation.Set(binName, 1, Value.Get("88")),
				ListOperation.Get(binName, 1),
				ListOperation.PopRange(binName, -2, 1),
				ListOperation.PopRange(binName, -1),
				ListOperation.Remove(binName, 3),
				ListOperation.RemoveRange(binName, 0, 1),
				ListOperation.RemoveRange(binName, 2),
				ListOperation.Size(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			long size = (long)list[0];
			Assert.AreEqual(7, size);

			IList rangeList = (IList)list[1];
			Assert.IsTrue((bool)rangeList[0]);
			Assert.AreEqual(55, (long)rangeList[1]);
			Assert.AreEqual("string value", (string)rangeList[2]);

			IList subList = (IList)rangeList[3];
			Assert.AreEqual(3, subList.Count);
			Assert.AreEqual(12, (long)subList[0]);
			Assert.AreEqual(-8734.81, (double)subList[1], 0.00001);
			Assert.AreEqual("my string", (string)subList[2]);

			byte[] bt = (byte[])rangeList[4];
			CollectionAssert.AreEqual(bytes, bt, "bytes not equal");

			Assert.AreEqual(99.99, (double)rangeList[5], 0.00001);

			IDictionary subMap = (IDictionary)rangeList[6];
			Assert.AreEqual(2, subMap.Count);
			Assert.AreEqual("data 9", (string)subMap[9L]);
			Assert.AreEqual("data -2", (string)subMap[-2L]);

			// Set does not return a result.
			Assert.AreEqual("88", (string)list[2]);

			subList = (IList)list[3];
			Assert.AreEqual(1, subList.Count);
			Assert.AreEqual(99.99, (double)(double?)subList[0], 0.00001);

			subList = (IList)list[4];
			Assert.AreEqual(1, subList.Count);
			Assert.IsTrue(subList[0] is IDictionary);

			Assert.AreEqual(1, (long)list[5]);
			Assert.AreEqual(1, (long)list[6]);
			Assert.AreEqual(1, (long)list[7]);

			size = (long)list[8];
			Assert.AreEqual(2, size);
		}

		[TestMethod]
		public void OperateList5()
		{
			// Test trim.
			Key key = new Key(args.ns, args.set, "oplkey5");

			client.Delete(null, key);

			IList itemList = new List<Value>();
			itemList.Add(Value.Get("s11"));
			itemList.Add(Value.Get("s22222"));
			itemList.Add(Value.Get("s3333333"));
			itemList.Add(Value.Get("s4444444444"));
			itemList.Add(Value.Get("s5555555555555555"));

			Record record = client.Operate(null, key,
				ListOperation.InsertItems(binName, 0, itemList),
				ListOperation.Trim(binName, -5, 5),
				ListOperation.Trim(binName, 1, -5),
				ListOperation.Trim(binName, 1, 2)
				//ListOperation.Trim(binName, 11, 6) causes entire command to fail.
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			long size = (long)list[0];
			Assert.AreEqual(5, size);

			size = (long)list[1];
			Assert.AreEqual(0, size);

			size = (long)list[2];
			Assert.AreEqual(1, size);

			size = (long)list[3];
			Assert.AreEqual(2, size);
		}

		[TestMethod]
		public void OperateList6()
		{
			// Test clear.
			Key key = new Key(args.ns, args.set, "oplkey6");

			client.Delete(null, key);

			WritePolicy policy = new WritePolicy();
			policy.respondAllOps = true;

			IList itemList = new List<Value>();
			itemList.Add(Value.Get("s11"));
			itemList.Add(Value.Get("s22222"));
			itemList.Add(Value.Get("s3333333"));
			itemList.Add(Value.Get("s4444444444"));
			itemList.Add(Value.Get("s5555555555555555"));

			Record record = client.Operate(policy, key,
				Operation.Put(new Bin("otherbin", 11)),
				Operation.Get("otherbin"),
				ListOperation.AppendItems(binName, itemList),
				ListOperation.Clear(binName),
				ListOperation.Size(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList("otherbin");
			Assert.AreEqual(2, list.Count);
			Assert.IsNull(list[0]);
			Assert.AreEqual(11, (long)(long)list[1]);

			list = record.GetList(binName);

			long size = (long)list[0];
			Assert.AreEqual(5, size);

			// clear() does not return value by default, but we set respondAllOps, so it returns null.
			Assert.IsNull(list[1]);

			size = (long)list[2];
			Assert.AreEqual(0, size);
		}

		[TestMethod]
		public void OperateList7()
		{
			// Test null values.
			Key key = new Key(args.ns, args.set, "oplkey7");

			client.Delete(null, key);

			WritePolicy policy = new WritePolicy();
			policy.respondAllOps = true;

			IList itemList = new List<Value>();
			itemList.Add(Value.Get("s11"));
			itemList.Add(Value.AsNull);
			itemList.Add(Value.Get("s3333333"));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(binName, itemList),
				ListOperation.Get(binName, 0),
				ListOperation.Get(binName, 1),
				ListOperation.Get(binName, 2)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(3, size);

			string str = (string)results[i++];
			Assert.AreEqual("s11", str);

			str = (string)results[i++];
			Assert.IsNull(str);

			str = (string)results[i++];
			Assert.AreEqual("s3333333", str);
		}

		[TestMethod]
		public void OperateList8()
		{
			// Test null values.
			Key key = new Key(args.ns, args.set, "oplkey8");

			client.Delete(null, key);

			WritePolicy policy = new WritePolicy();
			policy.respondAllOps = true;

			IList itemList = new List<Value>();
			itemList.Add(Value.Get(1));
			itemList.Add(Value.Get(2));
			itemList.Add(Value.Get(3));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(binName, itemList),
				ListOperation.Increment(binName, 2),
				ListOperation.Increment(ListPolicy.Default, binName, 2),
				ListOperation.Increment(binName, 1, Value.Get(7)),
				ListOperation.Increment(ListPolicy.Default, binName, 1, Value.Get(7)),
				ListOperation.Get(binName, 0)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(3, size);

			long val = (long)results[i++];
			Assert.AreEqual(4, val);

			val = (long)results[i++];
			Assert.AreEqual(5, val);

			val = (long)results[i++];
			Assert.AreEqual(9, val);

			val = (long)results[i++];
			Assert.AreEqual(16, val);

			val = (long)results[i++];
			Assert.AreEqual(1, val);
		}

		[TestMethod]
		public void OperateListSwitchSort()
		{
			Key key = new Key(args.ns, args.set, "oplkey9");

			client.Delete(null, key);

			IList itemList = new List<Value>();
			itemList.Add(Value.Get(4));
			itemList.Add(Value.Get(3));
			itemList.Add(Value.Get(1));
			itemList.Add(Value.Get(5));
			itemList.Add(Value.Get(2));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(ListPolicy.Default, binName, itemList),
				ListOperation.GetByIndex(binName, 3, ListReturnType.VALUE)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(5L, size);

			long val = (long)results[i++];
			Assert.AreEqual(5L, val);

			IList valueList = new List<Value>();
			valueList.Add(Value.Get(4));
			valueList.Add(Value.Get(2));

			// Sort list.
			record = client.Operate(null, key,
				ListOperation.SetOrder(binName, ListOrder.ORDERED),
				ListOperation.GetByValue(binName, Value.Get(3), ListReturnType.INDEX),
				ListOperation.GetByValueRange(binName, Value.Get(-1), Value.Get(3), ListReturnType.COUNT),
				ListOperation.GetByValueRange(binName, Value.Get(-1), Value.Get(3), ListReturnType.EXISTS),
				ListOperation.GetByValueList(binName, valueList, ListReturnType.RANK),
				ListOperation.GetByIndex(binName, 3, ListReturnType.VALUE),
				ListOperation.GetByIndexRange(binName, -2, 2, ListReturnType.VALUE),
				ListOperation.GetByRank(binName, 0, ListReturnType.VALUE),
				ListOperation.GetByRankRange(binName, 2, 3, ListReturnType.VALUE)
				);

			AssertRecordFound(key, record);

			results = record.GetList(binName);
			i = 0;

			IList list = (IList)results[i++];
			Assert.AreEqual(2L, list[0]);

			val = (long)results[i++];
			Assert.AreEqual(2L, val);

			bool b = (bool)results[i++];
			Assert.IsTrue(b);

			list = (IList)results[i++];
			Assert.AreEqual(2L, list.Count);
			Assert.AreEqual(3L, list[0]);
			Assert.AreEqual(1L, list[1]);

			val = (long)results[i++];
			Assert.AreEqual(4L, val);

			list = (IList)results[i++];
			Assert.AreEqual(2L, list.Count);
			Assert.AreEqual(4L, list[0]);
			Assert.AreEqual(5L, list[1]);

			val = (long)results[i++];
			Assert.AreEqual(1L, val);

			list = (IList)results[i++];
			Assert.AreEqual(3L, list.Count);
			Assert.AreEqual(3L, list[0]);
			Assert.AreEqual(4L, list[1]);
			Assert.AreEqual(5L, list[2]);
		}

		[TestMethod]
		public void OperateListSort()
		{
			Key key = new Key(args.ns, args.set, "oplkey10");

			client.Delete(null, key);

			IList itemList = new List<Value>();
			itemList.Add(Value.Get(-44));
			itemList.Add(Value.Get(33));
			itemList.Add(Value.Get(-1));
			itemList.Add(Value.Get(33));
			itemList.Add(Value.Get(-2));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(ListPolicy.Default, binName, itemList),
				ListOperation.Sort(binName, ListSortFlags.DROP_DUPLICATES),
				ListOperation.Size(binName)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(5L, size);

			long val = (long)results[i++];
			Assert.AreEqual(4L, val);
		}

		[TestMethod]
		public void OperateListRemove()
		{
			Key key = new Key(args.ns, args.set, "oplkey11");

			client.Delete(null, key);

			IList itemList = new List<Value>();
			itemList.Add(Value.Get(-44));
			itemList.Add(Value.Get(33));
			itemList.Add(Value.Get(-1));
			itemList.Add(Value.Get(33));
			itemList.Add(Value.Get(-2));
			itemList.Add(Value.Get(0));
			itemList.Add(Value.Get(22));
			itemList.Add(Value.Get(11));
			itemList.Add(Value.Get(14));
			itemList.Add(Value.Get(6));

			IList valueList = new List<Value>();
			valueList.Add(Value.Get(-45));
			valueList.Add(Value.Get(14));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(ListPolicy.Default, binName, itemList),
				ListOperation.RemoveByValue(binName, Value.Get(0), ListReturnType.INDEX),
				ListOperation.RemoveByValueList(binName, valueList, ListReturnType.VALUE),
				ListOperation.RemoveByValueRange(binName, Value.Get(33), Value.Get(100), ListReturnType.VALUE),
				ListOperation.RemoveByIndex(binName, 1, ListReturnType.VALUE),
				ListOperation.RemoveByIndexRange(binName, 100, 101, ListReturnType.VALUE),
				ListOperation.RemoveByRank(binName, 0, ListReturnType.VALUE),
				ListOperation.RemoveByRankRange(binName, 3, 1, ListReturnType.VALUE)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(10L, size);

			IList list = (IList)results[i++];
			Assert.AreEqual(1L, list.Count);
			Assert.AreEqual(5L, list[0]);

			list = (IList)results[i++];
			Assert.AreEqual(1L, list.Count);
			Assert.AreEqual(14L, list[0]);

			list = (IList)results[i++];
			Assert.AreEqual(2L, list.Count);
			Assert.AreEqual(33L, list[0]);
			Assert.AreEqual(33L, list[1]);

			long val = (long)results[i++];
			Assert.AreEqual(-1L, val);

			list = (IList)results[i++];
			Assert.AreEqual(0L, list.Count);

			val = (long)results[i++];
			Assert.AreEqual(-44L, val);

			list = (IList)results[i++];
			Assert.AreEqual(1L, list.Count);
			Assert.AreEqual(22L, list[0]);
		}

		[TestMethod]
		public void OperateListInverted()
		{
			Key key = new Key(args.ns, args.set, "oplkey12");

			client.Delete(null, key);

			IList itemList = new List<Value>();
			itemList.Add(Value.Get(4));
			itemList.Add(Value.Get(3));
			itemList.Add(Value.Get(1));
			itemList.Add(Value.Get(5));
			itemList.Add(Value.Get(2));

			IList valueList = new List<Value>();
			valueList.Add(Value.Get(4));
			valueList.Add(Value.Get(2));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(ListPolicy.Default, binName, itemList),
				ListOperation.GetByValue(binName, Value.Get(3), ListReturnType.INDEX | ListReturnType.INVERTED),
				ListOperation.GetByValueRange(binName, Value.Get(-1), Value.Get(3), ListReturnType.COUNT | ListReturnType.INVERTED),
				ListOperation.GetByValueList(binName, valueList, ListReturnType.RANK | ListReturnType.INVERTED),
				ListOperation.GetByIndexRange(binName, -2, 2, ListReturnType.VALUE | ListReturnType.INVERTED),
				ListOperation.GetByRankRange(binName, 2, 3, ListReturnType.VALUE | ListReturnType.INVERTED)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(5L, size);

			IList list = (IList)results[i++];
			Assert.AreEqual(4L, list.Count);
			Assert.AreEqual(0L, list[0]);
			Assert.AreEqual(2L, list[1]);
			Assert.AreEqual(3L, list[2]);
			Assert.AreEqual(4L, list[3]);

			long val = (long)results[i++];
			Assert.AreEqual(3L, val);

			list = (IList)results[i++];
			Assert.AreEqual(3L, list.Count);
			Assert.AreEqual(0L, list[0]);
			Assert.AreEqual(2L, list[1]);
			Assert.AreEqual(4L, list[2]);

			list = (IList)results[i++];
			Assert.AreEqual(3L, list.Count);
			Assert.AreEqual(4L, list[0]);
			Assert.AreEqual(3L, list[1]);
			Assert.AreEqual(1L, list[2]);

			list = (IList)results[i++];
			Assert.AreEqual(2L, list.Count);
			Assert.AreEqual(1L, list[0]);
			Assert.AreEqual(2L, list[1]);
		}

		[TestMethod]
		public void OperateListGetRelative()
		{
			Key key = new Key(args.ns, args.set, "oplkey13");

			client.Delete(null, key);

			List<Value> itemList = new List<Value>();
			itemList.Add(Value.Get(0));
			itemList.Add(Value.Get(4));
			itemList.Add(Value.Get(5));
			itemList.Add(Value.Get(9));
			itemList.Add(Value.Get(11));
			itemList.Add(Value.Get(15));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT), binName, itemList),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(5), 0, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(5), 1, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(5), -1, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(3), 0, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(3), 3, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(3), -3, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(5), 0, 2, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(5), 1, 1, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(5), -1, 2, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(3), 0, 1, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(3), 3, 7, ListReturnType.VALUE),
				ListOperation.GetByValueRelativeRankRange(binName, Value.Get(3), -3, 2, ListReturnType.VALUE)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(6L, size);

			IList list = (IList)results[i++];
			Assert.AreEqual(4L, list.Count);
			Assert.AreEqual(5L, list[0]);
			Assert.AreEqual(9L, list[1]);
			Assert.AreEqual(11L, list[2]);
			Assert.AreEqual(15L, list[3]);

			list = (IList)results[i++];
			Assert.AreEqual(3L, list.Count);
			Assert.AreEqual(9L, list[0]);
			Assert.AreEqual(11L, list[1]);
			Assert.AreEqual(15L, list[2]);

			list = (IList)results[i++];
			Assert.AreEqual(5L, list.Count);
			Assert.AreEqual(4L, list[0]);
			Assert.AreEqual(5L, list[1]);
			Assert.AreEqual(9L, list[2]);
			Assert.AreEqual(11L, list[3]);
			Assert.AreEqual(15L, list[4]);

			list = (IList)results[i++];
			Assert.AreEqual(5L, list.Count);
			Assert.AreEqual(4L, list[0]);
			Assert.AreEqual(5L, list[1]);
			Assert.AreEqual(9L, list[2]);
			Assert.AreEqual(11L, list[3]);
			Assert.AreEqual(15L, list[4]);

			list = (IList)results[i++];
			Assert.AreEqual(2L, list.Count);
			Assert.AreEqual(11L, list[0]);
			Assert.AreEqual(15L, list[1]);

			list = (IList)results[i++];
			Assert.AreEqual(6L, list.Count);
			Assert.AreEqual(0L, list[0]);
			Assert.AreEqual(4L, list[1]);
			Assert.AreEqual(5L, list[2]);
			Assert.AreEqual(9L, list[3]);
			Assert.AreEqual(11L, list[4]);
			Assert.AreEqual(15L, list[5]);

			list = (IList)results[i++];
			Assert.AreEqual(2L, list.Count);
			Assert.AreEqual(5L, list[0]);
			Assert.AreEqual(9L, list[1]);

			list = (IList)results[i++];
			Assert.AreEqual(1L, list.Count);
			Assert.AreEqual(9L, list[0]);

			list = (IList)results[i++];
			Assert.AreEqual(2L, list.Count);
			Assert.AreEqual(4L, list[0]);
			Assert.AreEqual(5L, list[1]);

			list = (IList)results[i++];
			Assert.AreEqual(1L, list.Count);
			Assert.AreEqual(4L, list[0]);

			list = (IList)results[i++];
			Assert.AreEqual(2L, list.Count);
			Assert.AreEqual(11L, list[0]);
			Assert.AreEqual(15L, list[1]);

			list = (IList)results[i++];
			Assert.AreEqual(0L, list.Count);
		}

		[TestMethod]
		public void OperateListRemoveRelative()
		{
			Key key = new Key(args.ns, args.set, "oplkey14");

			client.Delete(null, key);

			List<Value> itemList = new List<Value>();
			itemList.Add(Value.Get(0));
			itemList.Add(Value.Get(4));
			itemList.Add(Value.Get(5));
			itemList.Add(Value.Get(9));
			itemList.Add(Value.Get(11));
			itemList.Add(Value.Get(15));

			Record record = client.Operate(null, key,
				ListOperation.AppendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT), binName, itemList),
				ListOperation.RemoveByValueRelativeRankRange(binName, Value.Get(5), 0, ListReturnType.VALUE),
				ListOperation.RemoveByValueRelativeRankRange(binName, Value.Get(5), 1, ListReturnType.VALUE),
				ListOperation.RemoveByValueRelativeRankRange(binName, Value.Get(5), -1, ListReturnType.VALUE),
				ListOperation.RemoveByValueRelativeRankRange(binName, Value.Get(3), -3, 1, ListReturnType.VALUE),
				ListOperation.RemoveByValueRelativeRankRange(binName, Value.Get(3), -3, 2, ListReturnType.VALUE),
				ListOperation.RemoveByValueRelativeRankRange(binName, Value.Get(3), -3, 3, ListReturnType.VALUE)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long size = (long)results[i++];
			Assert.AreEqual(6L, size);

			IList list = (IList)results[i++];
			Assert.AreEqual(4L, list.Count);
			Assert.AreEqual(5L, list[0]);
			Assert.AreEqual(9L, list[1]);
			Assert.AreEqual(11L, list[2]);
			Assert.AreEqual(15L, list[3]);

			list = (IList)results[i++];
			Assert.AreEqual(0L, list.Count);

			list = (IList)results[i++];
			Assert.AreEqual(1L, list.Count);
			Assert.AreEqual(4L, list[0]);

			list = (IList)results[i++];
			Assert.AreEqual(0L, list.Count);

			list = (IList)results[i++];
			Assert.AreEqual(0L, list.Count);

			list = (IList)results[i++];
			Assert.AreEqual(1L, list.Count);
			Assert.AreEqual(0L, list[0]);
		}

		[TestMethod]
		public void OperateListPartial()
		{
			Key key = new Key(args.ns, args.set, "oplkey15");

			client.Delete(null, key);

			List<Value> itemList = new List<Value>();
			itemList.Add(Value.Get(0));
			itemList.Add(Value.Get(4));
			itemList.Add(Value.Get(5));
			itemList.Add(Value.Get(9));
			itemList.Add(Value.Get(9));
			itemList.Add(Value.Get(11));
			itemList.Add(Value.Get(15));
			itemList.Add(Value.Get(0));

			Record record = client.Operate(null, key,
					ListOperation.AppendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.PARTIAL | ListWriteFlags.NO_FAIL), binName, itemList),
					ListOperation.AppendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL), "bin2", itemList)
					);

			AssertRecordFound(key, record);

			long size = record.GetLong(binName);
			Assert.AreEqual(6L, size);

			size = record.GetLong("bin2");
			Assert.AreEqual(0L, size);

			itemList = new List<Value>();
			itemList.Add(Value.Get(11));
			itemList.Add(Value.Get(3));

			record = client.Operate(null, key,
					ListOperation.AppendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.PARTIAL | ListWriteFlags.NO_FAIL), binName, itemList),
					ListOperation.AppendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL), "bin2", itemList)
					);

			AssertRecordFound(key, record);

			size = record.GetLong(binName);
			Assert.AreEqual(7L, size);

			size = record.GetLong("bin2");
			Assert.AreEqual(2L, size);
		}

		[TestMethod]
		public void OperateListInfinity()
		{
			Key key = new Key(args.ns, args.set, "oplkey16");

			client.Delete(null, key);

			List<Value> itemList = new List<Value>();
			itemList.Add(Value.Get(0));
			itemList.Add(Value.Get(4));
			itemList.Add(Value.Get(5));
			itemList.Add(Value.Get(9));
			itemList.Add(Value.Get(11));
			itemList.Add(Value.Get(15));

			Record record = client.Operate(null, key,
					ListOperation.AppendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT), binName, itemList)
					);

			AssertRecordFound(key, record);

			long size = record.GetLong(binName);
			Assert.AreEqual(6L, size);

			itemList = new List<Value>();
			itemList.Add(Value.Get(11));
			itemList.Add(Value.Get(3));

			record = client.Operate(null, key,
					ListOperation.GetByValueRange(binName, Value.Get(10), Value.INFINITY, ListReturnType.VALUE)
					);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long val = (long)results[i++];
			Assert.AreEqual(11L, val);

			val = (long)results[i++];
			Assert.AreEqual(15L, val);
		}

		[TestMethod]
		public void OperateListWildcard()
		{
			Key key = new Key(args.ns, args.set, "oplkey17");

			client.Delete(null, key);

			List<Value> i1 = new List<Value>();
			i1.Add(Value.Get("John"));
			i1.Add(Value.Get(55));

			List<Value> i2 = new List<Value>();
			i2.Add(Value.Get("Jim"));
			i2.Add(Value.Get(95));

			List<Value> i3 = new List<Value>();
			i3.Add(Value.Get("Joe"));
			i3.Add(Value.Get(80));

			List<Value> itemList = new List<Value>();

			itemList.Add(Value.Get(i1));
			itemList.Add(Value.Get(i2));
			itemList.Add(Value.Get(i3));

			Record record = client.Operate(null, key,
					ListOperation.AppendItems(binName, itemList)
					);

			AssertRecordFound(key, record);

			long size = record.GetLong(binName);
			Assert.AreEqual(3L, size);

			itemList = new List<Value>();
			itemList.Add(Value.Get("Jim"));
			itemList.Add(Value.WILDCARD);

			record = client.Operate(null, key,
					ListOperation.GetByValue(binName, Value.Get(itemList), ListReturnType.VALUE)
					);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			IList items = (IList)results[i++];
			String s = (String)items[0];
			Assert.AreEqual("Jim", s);

			long v = (long)items[1];
			Assert.AreEqual(95L, v);
		}

		[TestMethod]
		public void OperateNestedList()
		{
			Key key = new Key(args.ns, args.set, "oplkey18");

			client.Delete(null, key);

			IList<Value> l1 = new List<Value>();
			l1.Add(Value.Get(7));
			l1.Add(Value.Get(9));
			l1.Add(Value.Get(5));

			IList<Value> l2 = new List<Value>();
			l2.Add(Value.Get(1));
			l2.Add(Value.Get(2));
			l2.Add(Value.Get(3));

			IList<Value> l3 = new List<Value>();
			l3.Add(Value.Get(6));
			l3.Add(Value.Get(5));
			l3.Add(Value.Get(4));
			l3.Add(Value.Get(1));

			List<Value> inputList = new List<Value>();
			inputList.Add(Value.Get(l1));
			inputList.Add(Value.Get(l2));
			inputList.Add(Value.Get(l3));

			// Create list.
			client.Put(null, key, new Bin(binName, inputList));

			// Append value to last list and retrieve all lists.
			Record record = client.Operate(null, key,
				ListOperation.Append(binName, Value.Get(11), CTX.ListIndex(-1)),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long count = (long)results[i++];
			Assert.AreEqual(5, count);

			IList list = (IList)results[i++];
			Assert.AreEqual(3, list.Count);

			// Test last nested list.
			list = (IList)list[2];
			Assert.AreEqual(5, list.Count);
			Assert.AreEqual(6, (long)(long?)list[0]);
			Assert.AreEqual(5, (long)(long?)list[1]);
			Assert.AreEqual(4, (long)(long?)list[2]);
			Assert.AreEqual(1, (long)(long?)list[3]);
			Assert.AreEqual(11, (long)(long?)list[4]);
		}

		[TestMethod]
		public void OperateNestedListMap()
		{
			Key key = new Key(args.ns, args.set, "oplkey19");

			client.Delete(null, key);

			IList<Value> l11 = new List<Value>();
			l11.Add(Value.Get(7));
			l11.Add(Value.Get(9));
			l11.Add(Value.Get(5));

			IList<Value> l12 = new List<Value>();
			l12.Add(Value.Get(13));

			IList<Value> l1 = new List<Value>();
			l1.Add(Value.Get(l11));
			l1.Add(Value.Get(l12));

			IList<Value> l21 = new List<Value>();
			l21.Add(Value.Get(9));

			IList<Value> l22 = new List<Value>();
			l22.Add(Value.Get(2));
			l22.Add(Value.Get(4));

			IList<Value> l23 = new List<Value>();
			l23.Add(Value.Get(6));
			l23.Add(Value.Get(1));
			l23.Add(Value.Get(9));

			IList<Value> l2 = new List<Value>();
			l2.Add(Value.Get(l21));
			l2.Add(Value.Get(l22));
			l2.Add(Value.Get(l23));

			Dictionary<Value, Value> inputMap = new Dictionary<Value, Value>();
			inputMap[Value.Get("key1")] = Value.Get(l1);
			inputMap[Value.Get("key2")] = Value.Get(l2);

			// Create list.
			client.Put(null, key, new Bin(binName, inputMap));

			// Append value to last list and retrieve map.
			Record record = client.Operate(null, key,
				ListOperation.Append(binName, Value.Get(11), CTX.MapKey(Value.Get("key2")), CTX.ListRank(0)),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long count = (long)results[i++];
			Assert.AreEqual(3, count);

			IDictionary map = (IDictionary)results[i++];
			Assert.AreEqual(2, map.Count);

			// Test affected nested list.
			IList list = (IList)map["key2"];
			Assert.AreEqual(3, list.Count);

			list = (IList)list[1];
			Assert.AreEqual(3, list.Count);
			Assert.AreEqual(2, (long)(long?)list[0]);
			Assert.AreEqual(4, (long)(long?)list[1]);
			Assert.AreEqual(11, (long)(long?)list[2]);
		}

		[TestMethod]
		public void OperateListCreateContext()
		{
			Key key = new Key(args.ns, args.set, "oplkey20");

			client.Delete(null, key);

			IList<Value> l1 = new List<Value>();
			l1.Add(Value.Get(7));
			l1.Add(Value.Get(9));
			l1.Add(Value.Get(5));

			IList<Value> l2 = new List<Value>();
			l2.Add(Value.Get(1));
			l2.Add(Value.Get(2));
			l2.Add(Value.Get(3));

			IList<Value> l3 = new List<Value>();
			l3.Add(Value.Get(6));
			l3.Add(Value.Get(5));
			l3.Add(Value.Get(4));
			l3.Add(Value.Get(1));

			List<Value> inputList = new List<Value>();
			inputList.Add(Value.Get(l1));
			inputList.Add(Value.Get(l2));
			inputList.Add(Value.Get(l3));

			// Create list.
			Record record = client.Operate(null, key,
				ListOperation.AppendItems(new ListPolicy(ListOrder.ORDERED, 0), binName, inputList),
				Operation.Get(binName)
				);

			// Append value to new list created after the original 3 lists.
			record = client.Operate(null, key,
				ListOperation.Append(binName, Value.Get(2), CTX.ListIndexCreate(3, ListOrder.ORDERED, false)),
				//ListOperation.Create(binName, ListOrder.ORDERED, false, CTX.ListIndex(3)),
				//ListOperation.Append(binName, Value.Get(2), CTX.ListIndex(3)),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			int i = 0;

			long count = (long)results[i++];
			Assert.AreEqual(1, count);

			IList list = (IList)results[i++];
			Assert.AreEqual(4, list.Count);

			// Test last nested list.
			list = (IList)list[1];
			Assert.AreEqual(1, list.Count);
			Assert.AreEqual(2, (long)list[0]);
		}

		[TestMethod]
		public void OperateListBounded()
		{
			Key key = new Key(args.ns, args.set, "oplkey21");

			client.Delete(null, key);

			List<Value> inputList = new List<Value>();
			inputList.Add(Value.Get(55));
			inputList.Add(Value.Get(11));
			inputList.Add(Value.Get(33));

			// Create list.
			Record record = client.Operate(null, key,
				ListOperation.AppendItems(binName, inputList)
				);

			// Define bounded list insertion policy.
			ListPolicy listPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.INSERT_BOUNDED);

			// Insert values to new list that are in bounds.
			record = client.Operate(null, key,
				ListOperation.Insert(listPolicy, binName, 1, Value.Get(22)),  // Insert at index 1.
				ListOperation.Insert(listPolicy, binName, -1, Value.Get(44)), // Insert at last offset in list.
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			// Expect list: [55, 22, 11, 44, 33]
			IList results = record.GetList(binName);
			IList list = (IList)results[2];
			Assert.AreEqual(5, list.Count);
			Assert.AreEqual(55L, list[0]);
			Assert.AreEqual(22L, list[1]);
			Assert.AreEqual(11L, list[2]);
			Assert.AreEqual(44L, list[3]);
			Assert.AreEqual(33L, list[4]);

			// Insert value that is out of bounds (index 6).
			// Note that index 5 would have worked since the server allows 
			// insertion at the exact end of list.
			try
			{
				record = client.Operate(null, key,
					ListOperation.Insert(listPolicy, binName, 6, Value.Get(20)),
					Operation.Get(binName)
					);
				//results = record.GetList(binName);
				//list = (IList)results[1];
				Assert.Fail("List insert should have failed");
			}
			catch (AerospikeException ae)
			{
				// AerospikeException with result code OP_NOT_APPLICABLE is expected.
				if (ae.Result != ResultCode.OP_NOT_APPLICABLE)
				{
					throw;
				}
			}
		}

		[TestMethod]
		public void OperateListOrder()
		{
			var keyList = new Key[8];
			for (int i = 0; i < 8; i++)
			{
				Key key = new(args.ns, args.set, i);
				keyList[i] = key;
				client.Delete(null, key);

				Bin[] bins =
				{
					new Bin("l", new int[] { 1, 2, 3, 4, 5, 6, 7, 8 }),
					new Bin("r", new int[] { 8, 7, 6, 5, 4, 3, 2, 1 })
				};
				client.Put(null, key, bins);
			}

			var result0 = client.Operate(null, keyList[0], ExpOperation.Read("l", Exp.Build(ListExp.GetByValueRelativeRankRange(ListReturnType.NONE, Exp.Val(1), Exp.Val(1), Exp.Val(4), Exp.ListBin("l"))), ExpReadFlags.DEFAULT));
			Console.WriteLine(result0);

			var result1 = client.Operate(null, keyList[1], ExpOperation.Read("r", Exp.Build(ListExp.GetByValueRelativeRankRange(ListReturnType.NONE, Exp.Val(1), Exp.Val(1), Exp.Val(4), Exp.ListBin("r"))), ExpReadFlags.DEFAULT));
			Console.WriteLine(result1);

			var result2 = client.Operate(null, keyList[2], ExpOperation.Read("l", Exp.Build(ListExp.GetByValueRelativeRankRange(ListReturnType.NONE, Exp.Val(1), Exp.Val(1), Exp.Val(4), Exp.ListBin("l"), Exp.Val(true))), ExpReadFlags.DEFAULT));
			Console.WriteLine(result2);
			/*# Record: {'l': [1, 2, 3, 4, 5, 6, 7, 8], 'r': [8, 7, 6, 5, 4, 3, 2, 1]}

			>>> cli.operate(key, [expops.expression_read('l', exp.ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_VALUE, 1, 1, 4, 'l').compile())])
			(('test', 'inverted', 0, bytearray(b'La\x10\x99\x81\xa0\xc7R\x02\xdbwKo\x85u\xb0z\xddyu')), { 'ttl': 430231, 'gen': 3}, { 'l': [5, 4, 3, 2]})                   # [2, 3, 4, 5]
			>>> cli.operate(key, [expops.expression_read('r', exp.ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_VALUE, 1, 1, 4, 'r').compile())])
			(('test', 'inverted', 0, bytearray(b'La\x10\x99\x81\xa0\xc7R\x02\xdbwKo\x85u\xb0z\xddyu')), { 'ttl': 430217, 'gen': 3}, { 'r': [5, 4, 3, 2]})
			>>> cli.operate(key, [expops.expression_read('l', exp.ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_VALUE, 1, 1, 4, 'l', True).compile())])
			(('test', 'inverted', 0, bytearray(b'La\x10\x99\x81\xa0\xc7R\x02\xdbwKo\x85u\xb0z\xddyu')), { 'ttl': 430179, 'gen': 3}, { 'l': [1, 6, 7, 8]})
			>>> cli.operate(key, [expops.expression_read('r', exp.ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_VALUE, 1, 1, 4, 'r', True).compile())])
			(('test', 'inverted', 0, bytearray(b'La\x10\x99\x81\xa0\xc7R\x02\xdbwKo\x85u\xb0z\xddyu')), { 'ttl': 430169, 'gen': 3}, { 'r': [8, 7, 6, 1]})

			>>> cli.operate(key, [expops.expression_read('l', exp.ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, 4, 'l', False).compile())])
			(('test', 'inverted', 0, bytearray(b'La\x10\x99\x81\xa0\xc7R\x02\xdbwKo\x85u\xb0z\xddyu')), { 'ttl': 429832, 'gen': 3}, { 'l': [5, 6, 7, 8]})
			>>> cli.operate(key, [expops.expression_read('r', exp.ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, 4, 'r', False).compile())])
			(('test', 'inverted', 0, bytearray(b'La\x10\x99\x81\xa0\xc7R\x02\xdbwKo\x85u\xb0z\xddyu')), { 'ttl': 429803, 'gen': 3}, { 'r': [5, 6, 7, 8]})                   # [8, 7, 6, 5]
			>>> cli.operate(key, [expops.expression_read('l', exp.ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, 4, 'l', True).compile())])
			(('test', 'inverted', 0, bytearray(b'La\x10\x99\x81\xa0\xc7R\x02\xdbwKo\x85u\xb0z\xddyu')), { 'ttl': 429819, 'gen': 3}, { 'l': [1, 2, 3, 4]})
			>>> cli.operate(key, [expops.expression_read('r', exp.ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, 4, 'r', True).compile())])
			(('test', 'inverted', 0, bytearray(b'La\x10\x99\x81\xa0\xc7R\x02\xdbwKo\x85u\xb0z\xddyu')), { 'ttl': 429798, 'gen': 3}, { 'r': [4, 3, 2, 1]})*/
		}
	}
}
