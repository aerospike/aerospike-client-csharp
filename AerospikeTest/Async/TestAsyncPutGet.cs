/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
	public class TestAsyncPutGet : TestAsync
	{
		private static readonly string binName = Suite.GetBinName("putgetbin");
		private static readonly CancellationTokenSource tokenSource = new();

		[TestMethod]
		public void AsyncPutGet()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "putgetkey1");
			Bin bin = new(binName, "value1");

			client.Put(null, new WriteHandler(this, bin), key, bin);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncPutGetWithTask()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "putgetkey2");
			Bin bin = new(binName, "value2");

			Task taskput = client.Put(null, tokenSource.Token, key, bin);
			taskput.Wait();

			Task<Record> taskget = client.Get(null, tokenSource.Token, key);
			taskget.Wait();

			TestSync.AssertBinEqual(key, taskget.Result, bin);
		}

		static void WriteListenerSuccess(Key key, Bin bin, TestAsyncPutGet parent)
		{
			try
			{
				// Write succeeded.  Now call read.
				client.Get(null, new RecordHandler(parent, bin), key);
			}
			catch (Exception e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		static void RecordHandlerSuccess(Key key, Record record, Bin bin, TestAsyncPutGet parent)
		{
			parent.AssertBinEqual(key, record, bin);
			parent.NotifyCompleted();
		}

		private class WriteHandler(TestAsyncPutGet parent, Bin bin) : WriteListener
		{
			public void OnSuccess(Key key)
			{
				WriteListenerSuccess(key, bin, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class RecordHandler(TestAsyncPutGet parent, Bin bin) : RecordListener
		{
			public void OnSuccess(Key key, Record record)
			{
				RecordHandlerSuccess(key, record, bin, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncPutWithCanel()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "putgetkey3");
			Bin bin = new(binName, "value3");

			tokenSource.Cancel();
			try
			{
				await client.Put(null, tokenSource.Token, key, bin);
			}
			catch (TaskCanceledException) // expected exception for native client
			{
			}
		}
	}
}
