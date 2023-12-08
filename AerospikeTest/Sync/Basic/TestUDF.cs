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
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections;
using System.Reflection;

namespace Aerospike.Test
{
	[TestClass]
	public class TestUDF : TestSync
	{
		[ClassInitialize()]
		public static void Register(TestContext testContext)
		{
			if (!args.testProxy || (args.testProxy && nativeClient != null))
			{
				Assembly assembly = Assembly.GetExecutingAssembly();
				RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
				task.Wait();
			}
		}

		[TestMethod]
		public void WriteUsingUdf()
		{
			Key key = new Key(args.ns, args.set, "udfkey1");
			Bin bin = new Bin(args.GetBinName("udfbin1"), "string value");

			client.Execute(null, key, "record_example", "writeBin", Value.Get(bin.name), bin.value);

			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin);
		}

		[TestMethod]
		public void WriteIfGenerationNotChanged()
		{
			Key key = new Key(args.ns, args.set, "udfkey2");
			Bin bin = new Bin(args.GetBinName("udfbin2"), "string value");

			// Seed record.
			client.Put(null, key, bin);

			// Get record generation.
			long gen = (long)client.Execute(null, key, "record_example", "getGeneration");

			// Write record if generation has not changed.
			client.Execute(null, key, "record_example", "writeIfGenerationNotChanged", Value.Get(bin.name), bin.value, Value.Get(gen));
		}

		[TestMethod]
		public void WriteIfNotExists()
		{
			Key key = new Key(args.ns, args.set, "udfkey3");
			string binName = "udfbin3";

			// Delete record if it already exists.
			client.Delete(null, key);

			// Write record only if not already exists. This should succeed.
			client.Execute(null, key, "record_example", "writeUnique", Value.Get(binName), Value.Get("first"));

			// Verify record written.
			Record record = client.Get(null, key, binName);
			AssertBinEqual(key, record, binName, "first");

			// Write record second time. This should Assert.Fail.
			client.Execute(null, key, "record_example", "writeUnique", Value.Get(binName), Value.Get("second"));

			// Verify record not written.
			record = client.Get(null, key, binName);
			AssertBinEqual(key, record, binName, "first");
		}

		[TestMethod]
		public void WriteWithValidation()
		{
			Key key = new Key(args.ns, args.set, "udfkey4");
			string binName = "udfbin4";

			// Lua function writeWithValidation accepts number between 1 and 10.
			// Write record with valid value.
			client.Execute(null, key, "record_example", "writeWithValidation", Value.Get(binName), Value.Get(4));

			// Write record with invalid value.		
			try
			{
				client.Execute(null, key, "record_example", "writeWithValidation", Value.Get(binName), Value.Get(11));
				Assert.Fail("UDF should not have succeeded!");
			}
			catch (Exception)
			{
			}
		}

		[TestMethod]
		public void WriteListMapUsingUdf()
		{
			Key key = new Key(args.ns, args.set, "udfkey5");

			List<object> inner = new List<object>();
			inner.Add("string2");
			inner.Add(8L);

			Dictionary<object, object> innerMap = new Dictionary<object, object>();
			innerMap["a"] = 1L;
			innerMap[2L] = "b";
			innerMap["list"] = inner;

			List<object> list = new List<object>();
			list.Add("string1");
			list.Add(4L);
			list.Add(inner);
			list.Add(innerMap);

			string binName = args.GetBinName("udfbin5");

			client.Execute(null, key, "record_example", "writeBin", Value.Get(binName), Value.Get(list));

			IList received = (IList)client.Execute(null, key, "record_example", "readBin", Value.Get(binName));
			Assert.IsNotNull(received);

			Assert.AreEqual(list.Count, received.Count);
			Assert.AreEqual(list[0], received[0]);
			Assert.AreEqual(list[1], received[1]);
			CollectionAssert.AreEqual((IList)list[2], (IList)received[2]);

			IDictionary exp = (IDictionary)list[3];
			IDictionary rec = (IDictionary)received[3];

			Assert.AreEqual(exp["a"], rec["a"]);
			Assert.AreEqual(exp[2L], rec[2L]);
			CollectionAssert.AreEqual((IList)exp["list"], (IList)rec["list"]);
		}

		[TestMethod]
		public void AppendListUsingUdf()
		{
			Key key = new Key(args.ns, args.set, "udfkey6");

			List<object> inner = new List<object>();
			inner.Add("string2");
			inner.Add(8L);

			Dictionary<object, object> innerMap = new Dictionary<object, object>();
			innerMap["a"] = 1L;
			innerMap[2L] = "b";
			innerMap["list"] = inner;

			List<object> list = new List<object>();
			list.Add("string1");
			list.Add(4L);
			list.Add(inner);
			list.Add(innerMap);

			string binName = args.GetBinName("udfbin6");

			// Write list.
			client.Execute(null, key, "record_example", "writeBin", Value.Get(binName), Value.Get(list));

			// Append value to list.
			string value = "appended value";
			client.Execute(null, key, "record_example", "appendListBin", Value.Get(binName), Value.Get(value));

			Record record = client.Get(null, key, binName);
			AssertRecordFound(key, record);

			object received = record.GetValue(binName);
			Assert.IsNotNull(received);
			Assert.IsInstanceOfType(received, typeof(IList));
			IList reclist = (IList)received;
			Assert.AreEqual(5, reclist.Count);
			object obj = reclist[4];
			Assert.IsInstanceOfType(obj, typeof(string));
			Assert.AreEqual(value, (string)obj);
		}

#if BINARY_FORMATTER
		[TestMethod]
		public void WriteBlobUsingUdf()
		{
			Key key = new Key(args.ns, args.set, "udfkey7");
			string binName = args.GetBinName("udfbin7");
			byte[] blob;

			// Create packed blob using standard C# tools.
			using (MemoryStream ms = new MemoryStream())
			{
				BinaryFormatter formatter = new BinaryFormatter();
				formatter.Serialize(ms, 9845);
				formatter.Serialize(ms, "Hello world.");
				blob = ms.ToArray();
			}

			client.Execute(null, key, "record_example", "writeBin", Value.Get(binName), Value.Get(blob));
			byte[] received = (byte[])client.Execute(null, key, "record_example", "readBin", Value.Get(binName));
			CollectionAssert.AreEqual(blob, received);
		}
#endif

		[TestMethod]
		public void BatchUDF()
		{
			Key[] keys = new Key[]
			{
				new Key(args.ns, args.set, 20000),
				new Key(args.ns, args.set, 20001)
			};

			client.Delete(null, null, keys);

			BatchResults br = client.Execute(null, null, keys, "record_example", "writeBin", Value.Get("B5"), Value.Get("value5"));
			Assert.IsTrue(br.status);

			Record[] records = client.Get(null, keys, "B5");
			Assert.AreEqual(2, records.Length);

			foreach (Record r in records)
			{
				Assert.IsNotNull(r);
				Assert.AreEqual("value5", r.GetString("B5"));
			}
		}

		[TestMethod]
		public void BatchUDFError()
		{
			Key[] keys = new Key[] { new Key(args.ns, args.set, 20002), new Key(args.ns, args.set, 20003) };

			client.Delete(null, null, keys);

			BatchResults br = client.Execute(null, null, keys, "record_example", "writeWithValidation", Value.Get("B5"), Value.Get(999));
			Assert.IsFalse(br.status);

			foreach (BatchRecord r in br.records)
			{
				Assert.IsNotNull(r);
				Assert.AreEqual(ResultCode.UDF_BAD_RESPONSE, r.resultCode);

				string msg = r.record.GetUDFError();
				//System.out.println(msg);
				Assert.IsNotNull(msg);
			}
		}

		[TestMethod]
		public void BatchUDFComplex()
		{
			string bin = "B5";

			Value[] a1 = new Value[] { Value.Get(bin), Value.Get("value1") };
			Value[] a2 = new Value[] { Value.Get(bin), Value.Get(5) };
			Value[] a3 = new Value[] { Value.Get(bin), Value.Get(999) };

			BatchUDF b1 = new BatchUDF(new Key(args.ns, args.set, 20004), "record_example", "writeBin", a1);
			BatchUDF b2 = new BatchUDF(new Key(args.ns, args.set, 20005), "record_example", "writeWithValidation", a2);
			BatchUDF b3 = new BatchUDF(new Key(args.ns, args.set, 20005), "record_example", "writeWithValidation", a3);

			List<BatchRecord> records = new List<BatchRecord>();
			records.Add(b1);
			records.Add(b2);
			records.Add(b3);

			bool status = client.Operate(null, records);

			Assert.IsFalse(status); // b3 results in an error.
			AssertBinEqual(b1.key, b1.record, bin, 0);
			AssertBinEqual(b2.key, b2.record, bin, 0);
			Assert.AreEqual(ResultCode.UDF_BAD_RESPONSE, b3.resultCode);

			BatchRead b4 = new BatchRead(new Key(args.ns, args.set, 20004), true);
			BatchRead b5 = new BatchRead(new Key(args.ns, args.set, 20005), true);

			records.Clear();
			records.Add(b4);
			records.Add(b5);

			status = client.Operate(null, records);

			Assert.IsTrue(status);
			AssertBinEqual(b4.key, b4.record, bin, "value1");
			AssertBinEqual(b5.key, b5.record, bin, 5);
		}
	}
}
