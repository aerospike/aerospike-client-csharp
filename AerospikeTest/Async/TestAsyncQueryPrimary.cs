/*
 * Copyright 2012-2026 Aerospike, Inc.
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
	public class TestAsyncQueryPrimary : TestAsync
	{
		private const string setName = "async-query-primary";
		private const string valueBin = "value";
		private const string payloadBin = "payload";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			WritePolicy policy = new()
			{
				sendKey = true
			};

			for (int i = 1; i <= 3; i++)
			{
				Key key = new(SuiteHelpers.ns, setName, "async-primary-" + i);
				client.Put(policy, key, new Bin(valueBin, i));
			}

			byte[] largePayload = new byte[17 * 1024];
			Array.Fill(largePayload, (byte)'y');
			client.Put(policy, new Key(SuiteHelpers.ns, setName, "async-primary-large"), new Bin(payloadBin, largePayload));
		}

		[ClassCleanup]
		public static void Destroy()
		{
			for (int i = 1; i <= 3; i++)
			{
				client.Delete(null, new Key(SuiteHelpers.ns, setName, "async-primary-" + i));
			}

			client.Delete(null, new Key(SuiteHelpers.ns, setName, "async-primary-large"));
		}

		[TestMethod]
		public void AsyncQueryPrimarySetWithSequenceListener()
		{
			Statement stmt = new()
			{
				Namespace = SuiteHelpers.ns,
				SetName = setName,
				MaxRecords = 20,
				BinNames = [valueBin, payloadBin]
			};

			client.Query(null, new PrimaryRecordSequenceHandler(this), stmt);
			WaitTillComplete();
		}

		private class PrimaryRecordSequenceHandler(TestAsyncQueryPrimary parent) : RecordSequenceListener
		{
			private readonly HashSet<string> keys = new();
			private int valueCount;
			private int payloadCount;

			public void OnRecord(Key key, Record record)
			{
				if (!parent.AssertNotNull(key))
				{
					return;
				}

				if (!parent.AssertNotNull(record))
				{
					return;
				}

				if (!parent.AssertNotNull(key.userKey))
				{
					return;
				}

				keys.Add(key.userKey.ToString());

				if (record.bins.ContainsKey(valueBin))
				{
					valueCount++;
				}

				if (record.bins.ContainsKey(payloadBin))
				{
					payloadCount++;
					byte[] payload = record.GetValue(payloadBin) as byte[];
					if (!parent.AssertNotNull(payload))
					{
						return;
					}

					parent.AssertEquals(17 * 1024, payload.Length);
				}
			}

			public void OnSuccess()
			{
				parent.AssertEquals(4, keys.Count);
				parent.AssertEquals(3, valueCount);
				parent.AssertEquals(1, payloadCount);
				parent.AssertEquals(true, keys.Contains("async-primary-large"));
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}
	}
}
