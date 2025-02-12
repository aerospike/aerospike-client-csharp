/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestTouch : TestSync
	{
		[TestMethod]
		public void TouchOperate()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "TouchOperate");
			Bin bin = new(Suite.GetBinName("touchbin"), "touchvalue");

			WritePolicy writePolicy = new()
			{
				expiration = 1
			};

			client.Put(writePolicy, key, bin);

			writePolicy.expiration = 2;
			
			Record record = client.Operate(writePolicy, key, Operation.Touch(), Operation.GetHeader());
			AssertRecordFound(key, record);
			Assert.AreNotEqual(0, record.expiration);

			Util.Sleep(1000);

			record = client.Get(null, key, bin.name);
			AssertRecordFound(key, record);

			Util.Sleep(3000);

			record = client.Get(null, key, bin.name);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void Touch()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "touch");
			Bin bin = new(Suite.GetBinName("touchbin"), "touchvalue");

			WritePolicy writePolicy = new()
			{
				expiration = 1
			};

			client.Put(writePolicy, key, bin);

			writePolicy.expiration = 2;
			client.Touch(writePolicy, key);

			Util.Sleep(1000);

			var record = client.GetHeader(writePolicy, key);
			AssertRecordFound(key, record);
			Assert.AreNotEqual(0, record.expiration);

			Util.Sleep(3000);

			record = client.GetHeader(null, key);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void Touched()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "touched");

			client.Delete(null, key);

			WritePolicy writePolicy1 = new()
			{
				expiration = 10
			};
			WritePolicy writePolicy = writePolicy1;

			bool touched = client.Touched(writePolicy, key);
			Assert.IsFalse(touched);

			Bin bin = new("touchBin", "touchValue");
			client.Put(writePolicy, key, bin);

			touched = client.Touched(writePolicy, key);
			Assert.IsTrue(touched);
		}
	}
}
