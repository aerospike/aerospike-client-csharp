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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncPutGet : TestAsync
	{
		private static readonly string binName = args.GetBinName("putgetbin");
		private static CancellationTokenSource tokenSource = new();

		[TestMethod]
		public async Task AsyncPutGet()
		{
			Key key = new Key(args.ns, args.set, "putgetkey1");
			Bin bin = new Bin(binName, "value1");

			if (!args.testProxy)
			{
				client.Put(null, new WriteHandler(this, client, key, bin), key, bin);
				WaitTillComplete();
			}
			else
			{
				await client.Put(null, tokenSource.Token, key, bin);
				await WriteListenerSuccess(key, bin, this);
			}
		}

		[TestMethod]
		public void AsyncPutGetWithTask()
		{
			Key key = new Key(args.ns, args.set, "putgetkey2");
			Bin bin = new Bin(binName, "value2");

			Task taskput = client.Put(null, tokenSource.Token, key, bin);
			taskput.Wait();

			Task<Record> taskget = client.Get(null, tokenSource.Token, key);
			taskget.Wait();

			TestSync.AssertBinEqual(key, taskget.Result, bin);
		}

		static async Task WriteListenerSuccess(Key key, Bin bin, TestAsyncPutGet parent)
		{
			try
			{
				if (!args.testProxy)
				{
					// Write succeeded.  Now call read.
					client.Get(null, new RecordHandler(parent, key, bin), key);
				}
				else
				{
					var record = await client.Get(null, tokenSource.Token, key);
					RecordHandlerSuccess(key, record, bin, parent);
				}
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

		private class WriteHandler : WriteListener
		{
			private readonly TestAsyncPutGet parent;
			private IAsyncClient client;
			private Key key;
			private Bin bin;

			public WriteHandler(TestAsyncPutGet parent, IAsyncClient client, Key key, Bin bin)
			{
				this.parent = parent;
				this.client = client;
				this.key = key;
				this.bin = bin;
			}

			public void OnSuccess(Key key)
			{
				WriteListenerSuccess(key, bin, parent).Wait();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class RecordHandler : RecordListener
		{
			private readonly TestAsyncPutGet parent;
			private Key key;
			private Bin bin;

			public RecordHandler(TestAsyncPutGet parent, Key key, Bin bin)
			{
				this.parent = parent;
				this.key = key;
				this.bin = bin;
			}

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
	}
}
