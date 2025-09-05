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
using System.Linq;

namespace Aerospike.Test
{
	[TestClass]
	public class TestSerialize : TestSync
	{
		private static readonly string binName = Suite.GetBinName("serialbin");

#if BINARY_FORMATTER
		[TestMethod]
		public void SerializeArray()
		{
			Key key = new(Suite.ns, Suite.set, "serialarraykey");

			// Delete record if it already exists.
			client.Delete(null, key);

			int[] array = new int[10000];

			for (int i = 0; i < 10000; i++)
			{
				array[i] = i * i;
			}

			Bin bin = new(binName, new Value.BlobValue(array));

			// Do a test that pushes this complex object through the serializer
			client.Put(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			AssertRecordFound(key, record);

			int[] received = null;

			try
			{
				received = (int[])record.GetValue(bin.name);
			}
			catch (Exception)
			{
				Assert.Fail("Assert.Failed to parse returned value: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey + " bin=" + bin.name);
			}

			Assert.IsNotNull(received);
			Assert.AreEqual(10000, received.Length);

			for (int i = 0; i < 10000; i++)
			{
				if (received[i] != i * i)
				{
					Assert.Fail("Mismatch: index=" + i + " expected=" + (i * i) + " received=" + received[i]);
				}
			}
		}
#endif

		[TestMethod]
		public void SerializeList()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "seriallistkey");

			// Delete record if it already exists.
			client.Delete(null, key);

			List<string> list = ["string1", "string2", "string3"];

			Bin bin = new(binName, list);

			client.Put(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			AssertRecordFound(key, record);

			IList received = null;

			try
			{
				received = (IList) record.GetValue(bin.name);
			}
			catch (Exception)
			{
				Assert.Fail("Assert.Failed to parse returned value: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey + " bin=" + bin.name);
			}

			Assert.IsNotNull(received);
			Assert.AreEqual(3, received.Count);
			int max = received.Count;

			for (int i = 0; i < max; i++)
			{
				string expected = "string" + (i + 1);
				if (!received[i].Equals(expected))
				{
					object obj = received[i];
					Assert.Fail("Mismatch: index=" + i + " expected=" + expected + " received=" + obj);
				}
			}
		}

#if BINARY_FORMATTER
		[TestMethod]
		public void SerializeComplex()
		{
			Key key = new(Suite.ns, Suite.set, "serialcomplexkey");

			// Delete record if it already exists.
			client.Delete(null, key);

			List<object> inner = new List<object>();
			inner.Add("string2");
			inner.Add(8);

			Dictionary<object, object> innerMap = new Dictionary<object, object>();
			innerMap["a"] = 1;
			innerMap[2] = "b";
			innerMap["list"] = inner;

			List<object> list = new List<object>();
			list.Add("string1");
			list.Add(4);
			list.Add(inner);
			list.Add(innerMap);

			Bin bin = new(Suite.GetBinName("complexbin"), new Value.BlobValue(list));

			client.Put(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			AssertRecordFound(key, record);

			IList received = null;

			try
			{
				received = (IList) record.GetValue(bin.name);
			}
			catch (Exception)
			{
				Assert.Fail("Assert.Failed to parse returned value: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey + " bin=" + bin.name);
			}

			Assert.IsNotNull(received);
			Assert.AreEqual(list.Count, received.Count);
			Assert.AreEqual(list[0], received[0]);
			Assert.AreEqual(list[1], received[1]);
			CollectionAssert.AreEqual((IList)list[2], (IList)received[2]);

			IDictionary exp = (IDictionary)list[3];
			IDictionary rec = (IDictionary)received[3];

			Assert.AreEqual(exp["a"], rec["a"]);
			Assert.AreEqual(exp[2], rec[2]);
			CollectionAssert.AreEqual((IList)exp["list"], (IList)rec["list"]);
		}
#endif
	}
}
