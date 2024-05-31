/* 
 * Copyright 2012-2021 Aerospike, Inc.
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

namespace Aerospike.Test
{
	public class Test
	{
		public static void TestException(Action action, int expectedErrorCode)
		{
			try
			{
				action();
				Assert.Fail("Expected AerospikeException");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(expectedErrorCode, e.Result);
			}
		}

		public static async Task ThrowsAerospikeException(Func<Task> action, int expectedResultCode)
		{
			if (action == null)
			{
				throw new ArgumentNullException(nameof(action));
			}

			try
			{
				await action().ConfigureAwait(false);
				Assert.Fail("Expected AerospikeException");
			}
			catch (AggregateException ex)
			{
				foreach (var e in ex.InnerExceptions) { 
					if (e is AerospikeException ae)
					{
						Assert.AreEqual(expectedResultCode, ae.Result);
					}
				}
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(expectedResultCode, e.Result);
			}
		}
	}
}
