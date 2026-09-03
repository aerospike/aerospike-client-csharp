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
	[TestClass]
	public class TestAsyncTouch : TestAsync
	{
		[TestMethod]
		public void AsyncTouched()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "doesNotExistAsyncTouch");

			client.Touched(null, new TouchListener(this), key);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncTouchExistingRecord()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "async-touch-existing");
			Bin bin = new("touchbin", "touch-value");

			client.Put(null, new PutThenTouchHandler(this, key), key, bin);
			WaitTillComplete();
		}

		private class PutThenTouchHandler(TestAsyncTouch parent, Key key) : WriteListener
		{
			public void OnSuccess(Key writeKey)
			{
				client.GetHeader(null, new HeaderBeforeTouchHandler(parent, key), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class HeaderBeforeTouchHandler(TestAsyncTouch parent, Key key) : RecordListener
		{
			private int generation;

			public void OnSuccess(Key readKey, Record record)
			{
				if (!parent.AssertRecordFound(key, record))
				{
					parent.NotifyCompleted();
					return;
				}

				generation = record.generation;
				client.Touch(null, new TouchExistingHandler(parent, key, generation), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class TouchExistingHandler(TestAsyncTouch parent, Key key, int generationBefore) : WriteListener
		{
			public void OnSuccess(Key writeKey)
			{
				client.GetHeader(null, new HeaderAfterTouchHandler(parent, key, generationBefore), key);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class HeaderAfterTouchHandler(TestAsyncTouch parent, Key key, int generationBefore) : RecordListener
		{
			public void OnSuccess(Key readKey, Record record)
			{
				if (!parent.AssertRecordFound(key, record))
				{
					parent.NotifyCompleted();
					return;
				}

				if (!parent.AssertTrue(record.generation > generationBefore))
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

		private class TouchListener(TestAsyncTouch parent) : ExistsListener
		{
			public void OnSuccess(Key key, bool exists)
			{
				Assert.IsFalse(exists);
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
