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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestGeneration : TestSync
	{
		[TestMethod]
		public async Task Generation()
		{
			Key key = new Key(args.ns, args.set, "genkey");
			string binName = args.GetBinName("genbin");

			if (!args.testAsyncAwait)
			{
				// Delete record if it already exists.
				client.Delete(null, key);

				// Set some values for the same record.
				Bin bin = new Bin(binName, "genvalue1");

				client.Put(null, key, bin);

				bin = new Bin(binName, "genvalue2");

				client.Put(null, key, bin);

				// Retrieve record and its generation count.
				Record record = client.Get(null, key, bin.name);
				AssertBinEqual(key, record, bin);

				// Set record and fail if it's not the expected generation.
				bin = new Bin(binName, "genvalue3");

				WritePolicy writePolicy = new WritePolicy();
				writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
				writePolicy.generation = record.generation;
				client.Put(writePolicy, key, bin);

				// Set record with invalid generation and check results .
				bin = new Bin(binName, "genvalue4");
				writePolicy.generation = 9999;

				try
				{
					client.Put(writePolicy, key, bin);
					Assert.Fail("Should have received generation error instead of success.");
				}
				catch (AerospikeException ae)
				{
					if (ae.Result != ResultCode.GENERATION_ERROR)
					{
						Assert.Fail("Unexpected return code: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey + " bin=" + bin.name + " value=" + bin.value + " code=" + ae.Result);
					}
				}

				// Verify results.
				record = client.Get(null, key, bin.name);
				AssertBinEqual(key, record, bin.name, "genvalue3");
			}
			else
			{
				// Delete record if it already exists.
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				// Set some values for the same record.
				Bin bin = new Bin(binName, "genvalue1");

				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);

				bin = new Bin(binName, "genvalue2");

				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);

				// Retrieve record and its generation count.
				Record record = await asyncAwaitClient.Get(null, key, new[] { bin.name }, CancellationToken.None);
				AssertBinEqual(key, record, bin);

				// Set record and fail if it's not the expected generation.
				bin = new Bin(binName, "genvalue3");

				WritePolicy writePolicy = new WritePolicy();
				writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
				writePolicy.generation = record.generation;
				await asyncAwaitClient.Put(writePolicy, key, new[] { bin }, CancellationToken.None);

				// Set record with invalid generation and check results .
				bin = new Bin(binName, "genvalue4");
				writePolicy.generation = 9999;

				try
				{
					await asyncAwaitClient.Put(writePolicy, key, new[] { bin }, CancellationToken.None);
					Assert.Fail("Should have received generation error instead of success.");
				}
				catch (AerospikeException ae)
				{
					if (ae.Result != ResultCode.GENERATION_ERROR)
					{
						Assert.Fail("Unexpected return code: namespace=" + key.ns + " set=" + key.setName + " key=" + key.userKey + " bin=" + bin.name + " value=" + bin.value + " code=" + ae.Result);
					}
				}

				// Verify results.
				record = await asyncAwaitClient.Get(null, key, new[] { bin.name }, CancellationToken.None);
				AssertBinEqual(key, record, bin.name, "genvalue3");
			}
		}
	}
}
