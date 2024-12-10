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
using System.Collections;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncOperate : TestAsync
	{
		private static readonly string binName = args.GetBinName("putgetbin");
		private static CancellationTokenSource tokenSource = new();

		[TestMethod]
		public async Task AsyncOperateList()
		{
			Key key = new Key(args.ns, args.set, "aoplkey1");
			if (!args.testAsyncAwait)
			{
				client.Delete(null, new DeleteHandlerList(this, key), key);
				WaitTillComplete();
			}
			else if (args.testAsyncAwait)
			{
				var existed = await asyncAwaitClient.Delete(null, key, tokenSource.Token);
				await DeleteHandlerListSuccess(key, existed, this);
			}
		}

		static async Task DeleteHandlerListSuccess(Key key, bool existed, TestAsyncOperate parent)
		{
			IList itemList = new List<Value>();
			itemList.Add(Value.Get(55));
			itemList.Add(Value.Get(77));
			Operation[] operations = {
				ListOperation.AppendItems(binName, itemList),
				ListOperation.Pop(binName, -1),
				ListOperation.Size(binName)
			};

			if (!args.testAsyncAwait)
			{
				client.Operate(null, new ReadHandler(parent), key, operations);
			}
			else if (args.testAsyncAwait)
			{
				var record = await asyncAwaitClient.Operate(null, key, operations, tokenSource.Token);
				ReadListenerSuccess(key, record, parent);
			}
		}

		static void ReadListenerSuccess(Key key, Record record, TestAsyncOperate parent)
		{
			if (!parent.AssertRecordFound(key, record))
			{
				parent.NotifyCompleted();
				return;
			}

			IList list = record.GetList(binName);

			long size = (long)list[0];
			if (!parent.AssertEquals(2, size))
			{
				parent.NotifyCompleted();
				return;
			}

			long val = (long)list[1];
			if (!parent.AssertEquals(77, val))
			{
				parent.NotifyCompleted();
				return;
			}

			size = (long)list[2];
			parent.AssertEquals(1, size);
			parent.NotifyCompleted();
		}

		private class DeleteHandlerList : DeleteListener
		{
			private readonly TestAsyncOperate parent;
			private Key key;

			public DeleteHandlerList(TestAsyncOperate parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key key, bool existed)
			{
				DeleteHandlerListSuccess(key, existed, parent).Wait();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class ReadHandler : RecordListener
		{
			private readonly TestAsyncOperate parent;

			public ReadHandler(TestAsyncOperate parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key key, Record record)
			{
				ReadListenerSuccess(key, record, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public async Task AsyncOperateMap()
		{
			Key key = new Key(args.ns, args.set, "aopmkey1");
			if (!args.testAsyncAwait)
			{
				client.Delete(null, new DeleteHandlerMap(this, key), key);
				WaitTillComplete();
			}
			else if (args.testAsyncAwait)
			{
				var existed = await asyncAwaitClient.Delete(null, key, tokenSource.Token);
				await DeleteHandlerMapSuccess(key, existed, this);
			}
		}

		static async Task DeleteHandlerMapSuccess(Key key, bool existed, TestAsyncOperate parent)
		{
			Dictionary<Value, Value> map = new Dictionary<Value, Value>();
			map[Value.Get("a")] = Value.Get(1);
			map[Value.Get("b")] = Value.Get(2);
			map[Value.Get("c")] = Value.Get(3);
			Operation[] operations =
			{
				MapOperation.PutItems(MapPolicy.Default, binName, map),
				MapOperation.GetByRankRange(binName, -1, 1, MapReturnType.KEY_VALUE)
			};

			if (!args.testAsyncAwait)
			{
				client.Operate(null, new MapHandler(parent), key, operations);
			}
			else if (args.testAsyncAwait)
			{
				var record = await asyncAwaitClient.Operate(null, key, operations, tokenSource.Token);
				MapHandlerSuccess(key, record, parent);
			}
		}

		static void MapHandlerSuccess(Key key, Record record, TestAsyncOperate parent)
		{
			if (!parent.AssertRecordFound(key, record))
			{
				parent.NotifyCompleted();
				return;
			}

			IList results = record.GetList(binName);
			long size = (long)results[0];
			parent.AssertEquals(3, size);

			IList list = (IList)results[1];
			KeyValuePair<object, object> entry = (KeyValuePair<object, object>)list[0];
			parent.AssertEquals("c", entry.Key);
			parent.AssertEquals(3L, entry.Value);

			parent.NotifyCompleted();
		}

		private class DeleteHandlerMap : DeleteListener
		{
			private readonly TestAsyncOperate parent;
			private Key key;

			public DeleteHandlerMap(TestAsyncOperate parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key key, bool existed)
			{
				DeleteHandlerMapSuccess(key, existed, parent).Wait();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class MapHandler : RecordListener
		{
			private readonly TestAsyncOperate parent;

			public MapHandler(TestAsyncOperate parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key key, Record record)
			{
				MapHandlerSuccess(key, record, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}
	}
}
