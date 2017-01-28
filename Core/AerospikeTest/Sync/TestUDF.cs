/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	public class UDFInit : TestSync
	{
		public UDFInit()
		{
			Assembly assembly = typeof(UDFInit).GetTypeInfo().Assembly;
			RegisterTask task = client.Register(null, assembly, "AerospikeTest.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();
		}
	}

	public class TestUDF : TestSync, Xunit.IClassFixture<UDFInit>
	{
		UDFInit fixture;

		public TestUDF(UDFInit fixture)
		{
			this.fixture = fixture;
		}

		[Xunit.Fact]
		public void WriteUsingUdf()
		{
			Key key = new Key(args.ns, args.set, "udfkey1");
			Bin bin = new Bin(args.GetBinName("udfbin1"), "string value");

			client.Execute(null, key, "record_example", "writeBin", Value.Get(bin.name), bin.value);

			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin);
		}

		[Xunit.Fact]
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

		[Xunit.Fact]
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

		[Xunit.Fact]
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
				Fail("UDF should not have succeeded!");
			}
			catch (Exception)
			{
			}
		}

		[Xunit.Fact]
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

			IList received = (IList) client.Execute(null, key, "record_example", "readBin", Value.Get(binName));
			Xunit.Assert.NotNull(received);

			Xunit.Assert.Equal(list.Count, received.Count);
			Xunit.Assert.Equal(list[0], received[0]);
			Xunit.Assert.Equal(list[1], received[1]);
			Xunit.Assert.Equal((IList)list[2], (IList)received[2]);

			IDictionary exp = (IDictionary)list[3];
			IDictionary rec = (IDictionary)received[3];

			Xunit.Assert.Equal(exp["a"], rec["a"]);
			Xunit.Assert.Equal(exp[2L], rec[2L]);
			Xunit.Assert.Equal((IList)exp["list"], (IList)rec["list"]);
		}

		[Xunit.Fact]
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
			Xunit.Assert.NotNull(received);
			Xunit.Assert.IsType(typeof(List<object>), received);
			IList reclist = (IList)received;
			Xunit.Assert.Equal(5, reclist.Count);
			object obj = reclist[4];
			Xunit.Assert.IsType(typeof(string), obj);
			Xunit.Assert.Equal(value, (string)obj);
		}

#if NETFRAMEWORK
		[Xunit.Fact]
		public void WriteBlobUsingUdf()
		{
			Key key = new Key(args.ns, args.set, "udfkey7");
			string binName = args.GetBinName("udfbin7");
			byte[] blob;

			// Create packed blob using standard C# tools.
			using (MemoryStream ms = new MemoryStream())
			{
				Formatter.Default.Serialize(ms, 9845);
				Formatter.Default.Serialize(ms, "Hello world.");
				blob = ms.ToArray();
			}

			client.Execute(null, key, "record_example", "writeBin", Value.Get(binName), Value.Get(blob));
			byte[] received = (byte[])client.Execute(null, key, "record_example", "readBin", Value.Get(binName));
			Xunit.Assert.Equal(blob, received);
		}
#endif
	}
}
