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
	public class TestAsyncDelete : TestAsync
	{
		private const string BinName = "adelbin";

		[TestMethod]
		public void AsyncDeleteListener()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-delete-listener");
			client.Put(null, key, new Bin(BinName, "delete-me"));

			client.Delete(null, new DeleteHandler(this, key), key);
			WaitTillComplete();
		}

		private class DeleteHandler(TestAsyncDelete parent, Key key) : DeleteListener
		{
			public void OnSuccess(Key deletedKey, bool existed)
			{
				if (!parent.AssertTrue(existed))
				{
					parent.NotifyCompleted();
					return;
				}

				client.Exists(null, new ExistsAfterDeleteHandler(parent), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class ExistsAfterDeleteHandler(TestAsyncDelete parent) : ExistsListener
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
