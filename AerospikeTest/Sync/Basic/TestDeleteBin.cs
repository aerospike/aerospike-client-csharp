/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
	public class TestDeleteBin : TestSync
	{
		[TestMethod]
		public void DeleteBin()
		{
			Key key = new Key(args.ns, args.set, "delbinkey");
			string binName1 = args.GetBinName("bin1");
			string binName2 = args.GetBinName("bin2");
			Bin bin1 = new Bin(binName1, "value1");
			Bin bin2 = new Bin(binName2, "value2");
			client.Put(null, key, bin1, bin2);

			bin1 = Bin.AsNull(binName1); // Set bin value to null to drop bin.
			client.Put(null, key, bin1);

			Record record = client.Get(null, key, bin1.name, bin2.name, "bin3");
			AssertRecordFound(key, record);

			if (record.GetValue("bin1") != null)
			{
				Assert.Fail("bin1 still exists.");
			}

			object v2 = record.GetValue("bin2");
			Assert.IsNotNull(v2);
			Assert.AreEqual("value2", v2);
		}
	}
}
