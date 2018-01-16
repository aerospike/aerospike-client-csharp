﻿/* 
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
using Aerospike.Client;

namespace Aerospike.Test
{
	[Xunit.Collection("AerospikeClient")]
	public class TestSync
	{
		public static Args args = Args.Instance;
		public static Client.AerospikeClient client = args.client;

		public static void AssertBinEqual(Key key, Record record, Bin bin)
		{
			AssertRecordFound(key, record);

			object received = record.GetValue(bin.name);
			object expected = bin.value.Object;

			if (received == null || !received.Equals(expected))
			{
				Fail("Data mismatch: Expected " + expected + ". Received " + received);
			}
		}

		public static void AssertBinEqual(Key key, Record record, String binName, Object expected)
		{
			AssertRecordFound(key, record);

			object received = record.GetValue(binName);

			if (received == null || !received.Equals(expected))
			{
				Fail("Data mismatch: Expected " + expected + ". Received " + received);
			}
		}

		public static void AssertBinEqual(Key key, Record record, String binName, int expected)
		{
			AssertRecordFound(key, record);

			int received = record.GetInt(binName);

			if (received != expected)
			{
				Fail("Data mismatch: Expected " + expected + ". Received " + received);
			}
		}

		public static void AssertRecordFound(Key key, Record record)
		{
			if (record == null)
			{
				Fail("Failed to get: namespace=" + args.ns + " set=" + args.set + " key=" + key.userKey);
			}
 		}

		public static void Fail(string message)
		{
			Xunit.Assert.True(false, message);
		}
	}
}
