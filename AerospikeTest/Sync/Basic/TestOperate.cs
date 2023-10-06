/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
	public class TestOperate : TestSync
	{
		[TestMethod]
		public void Operate()
		{
			// Write initial record.
			Key key = new Key(args.ns, args.set, "opkey");
			Bin bin1 = new Bin("optintbin", 7);
			Bin bin2 = new Bin("optstringbin", "string value");
			client.Put(null, key, bin1, bin2);

			// Add integer, write new string and read record.
			Bin bin3 = new Bin(bin1.name, 4);
			Bin bin4 = new Bin(bin2.name, "new string");
			Record record = client.Operate(null, key, Operation.Add(bin3), Operation.Put(bin4), Operation.Get());
			AssertBinEqual(key, record, bin3.name, 11);
			AssertBinEqual(key, record, bin4);
		}

		[TestMethod]
		public void OperateDelete()
		{
			// Write initial record.
			Key key = new Key(args.ns, args.set, "opkey");
			Bin bin1 = new Bin("optintbin1", 1);

			client.Put(null, key, bin1);

			// Read bin1 and then delete all.
			Record record = client.Operate(null, key,
				Operation.Get(bin1.name),
				Operation.Delete());

			AssertBinEqual(key, record, bin1.name, 1);

			// Verify record is gone.
			Assert.IsFalse(client.Exists(null, key));

			// Rewrite record.
			Bin bin2 = new Bin("optintbin2", 2);

			client.Put(null, key, bin1, bin2);

			// Read bin 1 and then delete all followed by a write of bin2.
			record = client.Operate(null, key,
				Operation.Get(bin1.name),
				Operation.Delete(),
				Operation.Put(bin2),
				Operation.Get(bin2.name));

			AssertBinEqual(key, record, bin1.name, 1);

			// Read record.
			record = client.Get(null, key);

			AssertBinEqual(key, record, bin2.name, 2);
			Assert.IsTrue(record.bins.Count == 1);
		}

		[TestMethod]
		public void OperateInvalidNamespace()
		{
			List<BatchRecord> records = new()
			{
				new BatchUDF(new Key("invalid", args.set, 1), "test_ops", "rec_create",
					new Value[] { Value.Get(new Dictionary<String, String>() {
						{
							"bin1_str", "a"
						}
					})
				}),
				new BatchWrite(new Key(args.ns, args.set, 2),
				new Operation[] { Operation.Put(new Bin("bin1_str", "aa")) }),
				new BatchWrite(new Key(args.ns, args.set, 3),
				new Operation[] { Operation.Put(new Bin("bin1_str", "aaa")) })
			};

			BatchPolicy bp = new()
			{
				respondAllKeys = false
			};

			try
			{
				bool isSucceed = client.Operate(bp, records);
				if (isSucceed)
				{
					Console.WriteLine("Batch passed");
				}
				else
				{
					Console.WriteLine("Some operations failed");
				}
			}
			catch (Exception)
			{
				throw;
			}
			finally {
				foreach (BatchRecord br in records) 
				{
					Console.WriteLine($"key: {br.key}, result: {br.resultCode}, record: {br.record}\n");
				}
			}
		}
	}
}
