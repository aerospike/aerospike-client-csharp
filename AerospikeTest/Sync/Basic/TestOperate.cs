﻿/* 
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opkey");
			Bin bin1 = new("optintbin", 7);
			Bin bin2 = new("optstringbin", "string value");
			client.Put(null, key, bin1, bin2);

			// Add integer, write new string and read record.
			Bin bin3 = new(bin1.name, 4);
			Bin bin4 = new(bin2.name, "new string");
			Record record = client.Operate(null, key, Operation.Add(bin3), Operation.Put(bin4), Operation.Get());
			AssertBinEqual(key, record, bin3.name, 11);
			AssertBinEqual(key, record, bin4);
		}

		[TestMethod]
		public void OperateDelete()
		{
			// Write initial record.
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opkey");
			Bin bin1 = new("optintbin1", 1);

			client.Put(null, key, bin1);

			// Read bin1 and then delete all.
			Record record = client.Operate(null, key,
				Operation.Get(bin1.name),
				Operation.Delete());

			AssertBinEqual(key, record, bin1.name, 1);

			// Verify record is gone.
			Assert.IsFalse(client.Exists(null, key));

			// Rewrite record.
			Bin bin2 = new("optintbin2", 2);

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
			Assert.AreEqual(1, record.bins.Count);
		}
	}
}
