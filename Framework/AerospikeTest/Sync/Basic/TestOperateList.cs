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
		public virtual void OperateListInverted()
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
	}
}
