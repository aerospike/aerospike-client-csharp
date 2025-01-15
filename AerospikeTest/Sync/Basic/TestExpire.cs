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
	public class TestExpire : TestSync
	{
		private static readonly string binName = Suite.GetBinName("expirebin");

		[TestMethod]
		public void Expire()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "expirekey1");
			Bin bin = new(binName, "expirevalue");

			// Specify that record expires 2 seconds after it's written.
			WritePolicy writePolicy = new()
			{
				expiration = 2
			};
			client.Put(writePolicy, key, bin);

			// Read the record before it expires, showing it is there.	
			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin);

			// Read the record after it expires, showing it's gone.
			Util.Sleep(3 * 1000);
			record = client.Get(null, key, bin.name);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void NoExpire()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "expirekey2");
			Bin bin = new(binName, "noexpirevalue");

			// Specify that record NEVER expires. 
			// The "Never Expire" value is -1, or 0xFFFFFFFF.
			WritePolicy writePolicy = new()
			{
				expiration = -1
			};
			client.Put(writePolicy, key, bin);

			// Read the record, showing it is there.
			Record record = client.Get(null, key, bin.name);
			AssertBinEqual(key, record, bin);

			// Read this Record after the Default Expiration, showing it is still there.
			// We should have set the Namespace TTL at 5 sec.
			Util.Sleep(10 * 1000);
			record = client.Get(null, key, bin.name);
			Assert.IsNotNull(record);
		}

		[TestMethod]
		public void ResetReadTtl()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "expirekey3");
			Bin bin = new(binName, "expirevalue");

			// Specify that record expires 2 seconds after it's written.
			WritePolicy writePolicy = new()
			{
				expiration = 2
			};
			client.Put(writePolicy, key, bin);

			// Read the record before it expires and reset read ttl.
			Util.Sleep(1000);
			Policy readPolicy = new()
			{
				readTouchTtlPercent = 80
			};
			Record record = client.Get(readPolicy, key, bin.name);
			AssertBinEqual(key, record, bin);

			// Read the record again, but don't reset read ttl.
			Util.Sleep(1000);
			readPolicy.readTouchTtlPercent = -1;
			record = client.Get(readPolicy, key, bin.name);
			AssertBinEqual(key, record, bin);

			// Read the record after it expires, showing it's gone.
			Util.Sleep(2000);
			record = client.Get(null, key, bin.name);
			Assert.IsNull(record);
		}
	}
}
