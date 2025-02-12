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
		private static readonly string binName = Suite.GetBinName("putgetbin");

		[TestMethod]
		public void AsyncOperateList()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "aoplkey1");
			client.Delete(null, new DeleteHandlerList(this), key);
			WaitTillComplete();
		}

		static void DeleteHandlerListSuccess(Key key, TestAsyncOperate parent)
		{
			IList itemList = new List<Value>
			{
				Value.Get(55),
				Value.Get(77)
			};
			Operation[] operations = [
				ListOperation.AppendItems(binName, itemList),
				ListOperation.Pop(binName, -1),
				ListOperation.Size(binName)
			];

			client.Operate(null, new ReadHandler(parent), key, operations);
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

		private class DeleteHandlerList(TestAsyncOperate parent) : DeleteListener
		{
			public void OnSuccess(Key key, bool existed)
			{
				DeleteHandlerListSuccess(key, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class ReadHandler(TestAsyncOperate parent) : RecordListener
		{
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
		public void AsyncOperateMap()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "aopmkey1");
			client.Delete(null, new DeleteHandlerMap(this), key);
			WaitTillComplete();
		}

		static void DeleteHandlerMapSuccess(Key key, TestAsyncOperate parent)
		{
			Dictionary<Value, Value> map = new()
			{
				[Value.Get("a")] = Value.Get(1),
				[Value.Get("b")] = Value.Get(2),
				[Value.Get("c")] = Value.Get(3)
			};
			Operation[] operations =
			[
				MapOperation.PutItems(MapPolicy.Default, binName, map),
				MapOperation.GetByRankRange(binName, -1, 1, MapReturnType.KEY_VALUE)
			];

			client.Operate(null, new MapHandler(parent), key, operations);
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

		private class DeleteHandlerMap(TestAsyncOperate parent) : DeleteListener
		{
			public void OnSuccess(Key key, bool existed)
			{
				DeleteHandlerMapSuccess(key, parent);
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class MapHandler(TestAsyncOperate parent) : RecordListener
		{
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
