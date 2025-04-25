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

namespace Aerospike.Test
{
	public class TestSync
	{
		public static readonly IAerospikeClient client = SuiteHelpers.client;

		public static void AssertBinEqual(Key key, Record record, Bin bin)
		{
			AssertRecordFound(key, record);

			object received = record.GetValue(bin.name);
			object expected = bin.value.Object;

			if (received == null || !received.Equals(expected))
			{
				Assert.Fail("Data mismatch: Expected " + expected + ". Received " + received);
			}
		}

		public static void AssertBinBlobEqual(Key key, Record record, Bin bin)
		{
			AssertRecordFound(key, record);

			byte[] received = (byte[])record.GetValue(bin.name);

			byte[] expected = bin.value.Object switch
			{
				byte[] bytes => bytes,
				Memory<byte> mem => mem.ToArray(),
				ReadOnlyMemory<byte> roMem => roMem.ToArray(),
				_ => throw new ArgumentException("Unexpected type"),
			};

			if (received == null || !received.SequenceEqual(expected))
			{
				Assert.Fail($"Data mismatch: Expected [{string.Join(", ", expected)}]. Received [{string.Join(", ", received ?? [])}]");
			}
		}

		public static void AssertBinEqual(Key key, Record record, String binName, Object expected)
		{
			AssertRecordFound(key, record);

			object received = record.GetValue(binName);

			if (received == null || !received.Equals(expected))
			{
				Assert.Fail("Data mismatch: Expected " + expected + ". Received " + received);
			}
		}

		public static void AssertBinEqual(Key key, Record record, String binName, int expected)
		{
			AssertRecordFound(key, record);

			int received = record.GetInt(binName);

			if (received != expected)
			{
				Assert.Fail("Data mismatch: Expected " + expected + ". Received " + received);
			}
		}

		public static void AssertRecordFound(Key key, Record record)
		{
			if (record == null)
			{
				Assert.Fail("Failed to get: namespace=" + SuiteHelpers.ns + " set=" + SuiteHelpers.set + " key=" + key.userKey);
			}
		}
	}
}
