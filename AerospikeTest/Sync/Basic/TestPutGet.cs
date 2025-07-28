﻿/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestPutGet : TestSync
	{
		[TestMethod]
		public void PutGet()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "putgetkey");
			Record record;

			if (SuiteHelpers.singleBin)
			{
				Bin bin = new("", "value");

				client.Put(null, key, bin);
				record = client.Get(null, key);
				AssertBinEqual(key, record, bin);
			}
			else {
				Bin bin1 = new("bin1", "value1");
				Bin bin2 = new("bin2", "value2");

				client.Put(null, key, bin1, bin2);
				record = client.Get(null, key);
				AssertBinEqual(key, record, bin1);
				AssertBinEqual(key, record, bin2);
			}

			record = client.GetHeader(null, key);
			AssertRecordFound(key, record);

			// Generation should be greater than zero.  Make sure it's populated.
			if (record.generation == 0)
			{
				Assert.Fail("Invalid record header: generation=" + record.generation + " expiration=" + record.expiration);
			}
		}

		[TestMethod]
		public void PutGetBytes()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "putgetbyteskey");
			Record record;

			if (SuiteHelpers.singleBin)
			{
				Bin bin = new("", "value"u8.ToArray().AsMemory());

				client.Put(null, key, bin);
				record = client.Get(null, key);
				AssertBinBlobEqual(key, record, bin);
			}
			else
			{
				Bin bin1 = new("bin1", (Memory<byte>)"value1"u8.ToArray());
				Bin bin2 = new("bin2", (ReadOnlyMemory<byte>)"value2"u8.ToArray());

				client.Put(null, key, bin1, bin2);
				record = client.Get(null, key);
				AssertBinBlobEqual(key, record, bin1);
				AssertBinBlobEqual(key, record, bin2);
			}

			record = client.GetHeader(null, key);
			AssertRecordFound(key, record);

			// Generation should be greater than zero.  Make sure it's populated.
			if (record.generation == 0)
			{
				Assert.Fail("Invalid record header: generation=" + record.generation + " expiration=" + record.expiration);
			}
		}

		[TestMethod]
		public void PutGetHeader()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "getHeader");
			client.Put(null, key, new Bin("mybin", "myvalue"));

			Record record = client.GetHeader(null, key);
			AssertRecordFound(key, record);

			// Generation should be greater than zero.  Make sure it's populated.
			if (record.generation == 0) 
			{
				Assert.Fail("Invalid record header: generation=" + record.generation + " expiration=" + record.expiration);
			}
		}

		[TestMethod]
		public void PutGetBool()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "pgb");
			Bin bin1 = new("bin1", false);
			Bin bin2 = new("bin2", true);
			Bin bin3 = new("bin3", 0);
			Bin bin4 = new("bin4", 1);

			client.Put(null, key, bin1, bin2, bin3, bin4);

			Record record = client.Get(null, key);
			bool b = record.GetBool(bin1.name);
			Assert.IsFalse(b);
			b = record.GetBool(bin2.name);
			Assert.IsTrue(b);
			b = record.GetBool(bin3.name);
			Assert.IsFalse(b);
			b = record.GetBool(bin4.name);
			Assert.IsTrue(b);
		}

		[TestMethod]
		public void PutGetGeoJson()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "geo");
			Bin geoBin = new("geo", Value.GetAsGeoJSON("{\"type\": \"Point\", \"coordinates\": [42.34, 58.62]}"));
			client.Put(null, key, geoBin);
			Record r = client.Get(null, key);
			Assert.AreEqual(r.GetValue("geo").GetType(), geoBin.value.GetType());
		}

		[TestMethod, TestCategory("Enterprise")]
		public void PutGetCompression()
		{
			WritePolicy writePolicy = new()
			{
				compress = true
			};

			Policy policy = new()
			{
				compress = true
			};

					Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "putgetc");
					Record record;

			List<string> list = [];
			int[] iterator = Enumerable.Range(0, 2000).ToArray();
			foreach (int i in iterator)
			{
				list.Add(i.ToString());
			}

			Bin bin1 = new("bin", list);

			client.Put(writePolicy, key, bin1);
			record = client.Get(policy, key);
			var bin1List = bin1.value.Object;
			record.bins.TryGetValue("bin", out object recordBin);
			CollectionAssert.AreEquivalent((List<string>)bin1List, (List<object>)recordBin);

			record = client.GetHeader(policy, key);
			AssertRecordFound(key, record);

			// Generation should be greater than zero.  Make sure it's populated.
			if (record.generation == 0)
			{
				Assert.Fail("Invalid record header: generation=" + record.generation + " expiration=" + record.expiration);
			}
		}
	}
}
