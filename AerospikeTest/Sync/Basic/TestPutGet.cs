/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
			Key key = new Key(args.ns, args.set, "putgetkey");
			Record record;

			if (args.singleBin) 
			{
				Bin bin = new Bin("", "value");
			
				client.Put(null, key, bin);
				record = client.Get(null, key);
				AssertBinEqual(key, record, bin);			
			}
			else {
				Bin bin1 = new Bin("bin1", "value1");
				Bin bin2 = new Bin("bin2", "value2");
			
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
		public void PutGetBool()
		{
			Key key = new Key(args.ns, args.set, "pgb");
			Bin bin1 = new Bin("bin1", false);
			Bin bin2 = new Bin("bin2", true);
			Bin bin3 = new Bin("bin3", 0);
			Bin bin4 = new Bin("bin4", 1);

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
	}
}
