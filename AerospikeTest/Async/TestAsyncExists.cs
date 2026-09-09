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
	public class TestAsyncExists : TestAsync
	{
		private static readonly string binName = "existbin";
		private static readonly CancellationTokenSource tokenSource = new();

		[TestMethod]
		public void AsyncExistsListener()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-exists-key");
			Bin bin = new(binName, "exists-value");

			client.Put(null, new PutThenExistsHandler(this, key), key, bin);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncExistsNotFound()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-exists-missing");

			client.Exists(null, new NotFoundHandler(this), key);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncExistsWithTask()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-exists-task");
			Bin bin = new(binName, "task-value");

			client.Put(null, tokenSource.Token, key, bin).Wait();
			bool exists = client.Exists(null, tokenSource.Token, key).Result;

			Assert.IsTrue(exists);
		}

		private class PutThenExistsHandler(TestAsyncExists parent, Key key) : WriteListener
		{
			public void OnSuccess(Key writeKey)
			{
				client.Exists(null, new ExistsHandler(parent), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class ExistsHandler(TestAsyncExists parent) : ExistsListener
		{
			public void OnSuccess(Key key, bool exists)
			{
				if (!parent.AssertTrue(exists))
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

		private class NotFoundHandler(TestAsyncExists parent) : ExistsListener
		{
			public void OnSuccess(Key key, bool exists)
			{
				if (!parent.AssertEquals(false, exists))
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
