/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using Grpc.Core;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsync
	{
		public static Args args = Args.Instance;
		public static IAsyncClient client = args.asyncClient;
		public static AsyncClient nativeClient = args.nativeAsync;

		private AsyncMonitor monitor = new AsyncMonitor();

		public bool AssertBinEqual(Key key, Record record, Bin bin)
		{
			if (!AssertRecordFound(key, record))
			{
				return false;
			}

			object received = record.GetValue(bin.name);
			object expected = bin.value.Object;

			if (received == null || !received.Equals(expected))
			{
				monitor.SetError("Data mismatch: Expected " + expected + ". Received " + received);
				return false;
			}
			return true;
		}

		public bool AssertBinEqual(Key key, Record record, string binName, object expected)
		{
			if (!AssertRecordFound(key, record))
			{
				return false;
			}

			object received = record.GetValue(binName);

			if (received == null || !received.Equals(expected))
			{
				monitor.SetError("Data mismatch: Expected " + expected + ". Received " + received);
				return false;
			}
			return true;
		}

		public bool AssertBinEqual(Key key, Record record, string binName, int expected)
		{
			if (!AssertRecordFound(key, record))
			{
				return false;
			}

			int received = record.GetInt(binName);

			if (received != expected)
			{
				monitor.SetError("Data mismatch: Expected " + expected + ". Received " + received);
				return false;
			}
			return true;
		}

		public bool AssertRecordFound(Key key, Record record)
		{
			if (record == null)
			{
				monitor.SetError("Failed to get: namespace=" + args.ns + " set=" + args.set + " key=" + key.userKey);
				return false;
			}
			return true;
		}

		public bool AssertBetween(long begin, long end, long value)
		{
			if (!(value >= begin && value <= end))
			{
				monitor.SetError("Range " + value + " not between " + begin + " and " + end);
				return false;
			}
			return true;
		}

		public bool AssertEquals(long expected, long received)
		{
			if (expected != received)
			{
				monitor.SetError("Data mismatch: Expected " + expected + ". Received " + received);
				return false;
			}
			return true;
		}

		public bool AssertEquals(object expected, object received)
		{
			if (!expected.Equals(received))
			{
				monitor.SetError("Data mismatch: Expected " + expected + ". Received " + received);
				return false;
			}
			return true;
		}

		public bool AssertEquals(bool expected, bool received)
		{
			if (expected != received)
			{
				monitor.SetError("Data mismatch: Expected " + expected + ". Received " + received);
				return false;
			}
			return true;
		}

		public bool AssertGreaterThanZero(long value)
		{
			if (value <= 0)
			{
				monitor.SetError("Value not greater than zero");
				return false;
			}
			return true;
		}

		public bool AssertNotNull(object obj)
		{
			if (obj == null)
			{
				monitor.SetError("Object is null");
				return false;
			}
			return true;
		}

		public bool AssertNull(object obj)
		{
			if (obj != null)
			{
				monitor.SetError("Object is not null");
				return false;
			}
			return true;
		}

		public bool AssertTrue(bool b)
		{
			if (!b)
			{
				monitor.SetError("Value is false");
				return false;
			}
			return true;
		}

		public void SetError(Exception e)
		{
			monitor.SetError(e);
		}

		public void WaitTillComplete()
		{
			if (!args.testProxy)
			{
				monitor.WaitTillComplete();
			}
		}

		public void NotifyCompleted()
		{
			monitor.NotifyCompleted();
		}
	}
}
