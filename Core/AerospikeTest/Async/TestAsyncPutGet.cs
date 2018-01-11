/* 
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
using System.Threading;
using System.Threading.Tasks;
using Aerospike.Client;

namespace Aerospike.Test
{
	public class TestAsyncPutGet : TestAsync
	{
		private static readonly string binName = args.GetBinName("putgetbin");

		[Xunit.Fact]
		public void AsyncPutGet()
		{
			Key key = new Key(args.ns, args.set, "putgetkey1");
			Bin bin = new Bin(binName, "value1");

			client.Put(null, new WriteHandler(this, client, key, bin), key, bin);
			WaitTillComplete();
		}

		[Xunit.Fact]
		public void AsyncPutGetWithTask()
		{
			Key key = new Key(args.ns, args.set, "putgetkey2");
			Bin bin = new Bin(binName, "value2");

			CancellationTokenSource cancel = new CancellationTokenSource();
			Task taskput = client.Put(null, cancel.Token, key, bin);
			taskput.Wait();

			Task<Record> taskget = client.Get(null, cancel.Token, key);
			taskget.Wait();

			TestSync.AssertBinEqual(key, taskget.Result, bin);
		}
		
		private class WriteHandler : WriteListener
		{
			private readonly TestAsyncPutGet parent;
			private AsyncClient client;
			private Key key;
			private Bin bin;

			public WriteHandler(TestAsyncPutGet parent, AsyncClient client, Key key, Bin bin)
			{
				this.parent = parent;
				this.client = client;
				this.key = key;
				this.bin = bin;
			}

			public void OnSuccess(Key key)
			{
				try
				{
					// Write succeeded.  Now call read.
					client.Get(null, new RecordHandler(parent, key, bin), key);
				}
				catch (Exception e)
				{
					parent.SetError(e);
					parent.NotifyCompleted();
				}
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
				parent.AssertBinEqual(key, record, bin);
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
