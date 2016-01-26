/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncOperate : TestAsync
	{
		private static readonly string binName = args.GetBinName("putgetbin");

		[TestMethod]
		public void AsyncOperateList()
		{
			Key key = new Key(args.ns, args.set, "aoplkey1");
			client.Delete(null, new DeleteHandler(this, key), key);
			WaitTillComplete();
		}

		private class DeleteHandler : DeleteListener
		{
			private readonly TestAsyncOperate parent;
			private Key key;

			public DeleteHandler(TestAsyncOperate parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key key, bool existed)
			{
				IList itemList = new List<Value>();
				itemList.Add(Value.Get(55));
				itemList.Add(Value.Get(77));

				client.Operate(null, new ReadHandler(parent), key,
					ListOperation.AppendItems(binName, itemList),
					ListOperation.Pop(binName, -1),
					ListOperation.Size(binName)
					);
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

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}
	}
}
