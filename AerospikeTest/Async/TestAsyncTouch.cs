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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncTouch : TestAsync
	{
		[TestMethod]
		public void AsyncTouched()
		{
			Key key = new(args.ns, args.set, "doesNotExistAsyncTouch");

			client.Touched(null, new TouchListener(this), key);
			WaitTillComplete();
		}

		private class TouchListener : ExistsListener
		{
			private readonly TestAsyncTouch parent;

			public TouchListener(TestAsyncTouch parent)
			{
				this.parent = parent;
			}

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
