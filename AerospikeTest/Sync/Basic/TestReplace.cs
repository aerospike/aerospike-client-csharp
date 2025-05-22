/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
	public class TestReplace : TestSync
	{
		[TestMethod]
		public void Replace()
		{
			Util.Sleep(5 * 30 * 1000);
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "replacekey");
			Bin bin1 = new("bin1", "value1");
			Bin bin2 = new("bin2", "value2");
			Bin bin3 = new("bin3", "value3");

			client.Put(null, key, bin1, bin2);

			WritePolicy policy = new()
			{
				recordExistsAction = RecordExistsAction.REPLACE
			};
			client.Put(policy, key, bin3);

			Record record = client.Get(null, key);
			AssertRecordFound(key, record);

			if (record.GetValue(bin1.name) != null)
			{
				Assert.Fail(bin1.name + " found when it should have been deleted.");
			}

			if (record.GetValue(bin2.name) != null)
			{
				Assert.Fail(bin2.name + " found when it should have been deleted.");
			}
			AssertBinEqual(key, record, bin3);
		}

		[TestMethod]
		public void ReplaceOnly()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "replaceonlykey");
			Bin bin = new("bin", "value");

			// Delete record if it already exists.
			client.Delete(null, key);

			try
			{
				WritePolicy policy = new()
				{
					recordExistsAction = RecordExistsAction.REPLACE_ONLY
				};
				client.Put(policy, key, bin);

				Assert.Fail("Failure. This command should have resulted in an error.");
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.KEY_NOT_FOUND_ERROR)
				{
					throw;
				}
			}
		}
	}
}
