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
	public class TestAsyncGetHeader : TestAsync
	{
		private static readonly string binName = "headerbin";
		private static readonly CancellationTokenSource tokenSource = new();

		[TestMethod]
		public void AsyncGetHeaderListener()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-get-header");
			Bin bin = new(binName, "header-value");

			client.Put(null, new PutThenGetHeaderHandler(this, key), key, bin);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncGetHeaderWithTask()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-get-header-task");
			Bin bin = new(binName, "header-task-value");

			client.Put(null, tokenSource.Token, key, bin).Wait();
			Record record = client.GetHeader(null, tokenSource.Token, key).Result;

			Assert.IsNotNull(record);
			Assert.IsTrue(record.generation > 0);
		}

		[TestMethod]
		public void AsyncGetHeaderNotFound()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-get-header-missing");

			client.GetHeader(null, new MissingHeaderHandler(this), key);
			WaitTillComplete();
		}

		private class PutThenGetHeaderHandler(TestAsyncGetHeader parent, Key key) : WriteListener
		{
			public void OnSuccess(Key writeKey)
			{
				client.GetHeader(null, new HeaderHandler(parent, key), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class HeaderHandler(TestAsyncGetHeader parent, Key key) : RecordListener
		{
			public void OnSuccess(Key readKey, Record record)
			{
				if (!parent.AssertRecordFound(key, record))
				{
					parent.NotifyCompleted();
					return;
				}

				if (!parent.AssertGreaterThanZero(record.generation))
				{
					parent.NotifyCompleted();
					return;
				}

				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class MissingHeaderHandler(TestAsyncGetHeader parent) : RecordListener
		{
			public void OnSuccess(Key key, Record record)
			{
				if (!parent.AssertRecordNotFound(key, record))
				{
					parent.NotifyCompleted();
					return;
				}

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
