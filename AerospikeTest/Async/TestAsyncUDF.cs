﻿/* 
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
using System.Reflection;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncUDF : TestAsync
	{
		private static readonly string binName = Suite.GetBinName("audfbin1");
		private const string binValue = "string value";

		[ClassInitialize()]
		public static void Register(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();
		}

		[TestMethod]
		public void AsyncUDF()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "audfkey1");
			Bin bin = new(binName, binValue);

			// Write bin
			client.Execute(null, new WriteHandler(this), key, "record_example", "writeBin", Value.Get(bin.name), bin.value);
			WaitTillComplete();
		}

		private class WriteHandler(TestAsyncUDF parent) : ExecuteListener
		{
			public void OnSuccess(Key key, object obj)
			{
				// Write succeeded.  Now call read using udf.
				client.Execute(null, new ReadHandler(parent), key, "record_example", "readBin", Value.Get(binName));
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class ReadHandler(TestAsyncUDF parent) : ExecuteListener
		{
			public void OnSuccess(Key key, object received)
			{
				if (parent.AssertNotNull(received))
				{
					parent.AssertEquals(binValue, received);
				}
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchUDF()
		{
			Key[] keys =
			[
				new Key(SuiteHelpers.ns, SuiteHelpers.set, 20000),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, 20001)
			];

			client.Delete(null, null, keys);

			client.Execute(null, null, new BatchUDFHandler(this), keys, "record_example", "writeBin", Value.Get("B5"), Value.Get("value5"));

			WaitTillComplete();
		}

		private class BatchUDFHandler(TestAsyncUDF parent) : BatchRecordArrayListener
		{
			public void OnSuccess(BatchRecord[] records, bool status)
			{
				try
				{
					if (parent.AssertTrue(status))
					{
						foreach (BatchRecord r in records)
						{
							if (parent.AssertNotNull(r))
							{
								parent.AssertEquals(0, r.resultCode);
							}
						}
					}
					parent.NotifyCompleted();
				}
				catch (Exception e)
				{
					parent.SetError(e);
					parent.NotifyCompleted();
				}
			}

			public void OnFailure(BatchRecord[] records, AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		[TestMethod]
		public void AsyncBatchUDFComplex()
		{
			string bin = "B5";

			Value[] a1 = [Value.Get(bin), Value.Get("value1")];
			Value[] a2 = [Value.Get(bin), Value.Get(5)];
			Value[] a3 = [Value.Get(bin), Value.Get(999)];

			List<BatchRecord> records =
			[
				new BatchUDF(new Key(SuiteHelpers.ns, SuiteHelpers.set, 20014), "record_example", "writeBin", a1),
				new BatchUDF(new Key(SuiteHelpers.ns, SuiteHelpers.set, 20015), "record_example", "writeWithValidation", a2),
				new BatchUDF(new Key(SuiteHelpers.ns, SuiteHelpers.set, 20015), "record_example", "writeWithValidation", a3),
			];

			client.Operate(null, new BatchSeqUDFHandler(this, bin), records);

			WaitTillComplete();
		}

		static void BatchSeqUDFHandlerSuccess(TestAsyncUDF parent, string bin)
		{
			List<BatchRecord> records =
			[
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, 20014), true),
				new BatchRead(new Key(SuiteHelpers.ns, SuiteHelpers.set, 20015), true),
			];

			client.Operate(null, new BatchSeqReadHandler(parent, bin), records);
		}

		private class BatchSeqUDFHandler(TestAsyncUDF parent, string bin) : BatchRecordSequenceListener
		{
			public void OnRecord(BatchRecord br, int index)
			{
				try
				{
					switch (index)
					{
						case 0:
							parent.AssertBinEqual(br.key, br.record, bin, 0);
							break;

						case 1:
							parent.AssertBinEqual(br.key, br.record, bin, 0);
							break;

						case 2:
							parent.AssertEquals(ResultCode.UDF_BAD_RESPONSE, br.resultCode);
							break;
					}
				}
				catch (Exception e)
				{
					parent.SetError(e);
					parent.NotifyCompleted();
				}
			}

			public void OnSuccess()
			{
				BatchSeqUDFHandlerSuccess(parent, bin);
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}
	}

	class BatchSeqReadHandler(TestAsyncUDF parent, string bin) : BatchRecordSequenceListener
	{
		public void OnRecord(BatchRecord br, int index)
		{
			try
			{
				switch (index)
				{
					case 0:
						parent.AssertBinEqual(br.key, br.record, bin, "value1");
						break;

					case 1:
						parent.AssertBinEqual(br.key, br.record, bin, 5);
						break;
				}
			}
			catch (Exception e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		public void OnSuccess()
		{
			parent.NotifyCompleted();
		}

		public void OnFailure(AerospikeException ae)
		{
			parent.SetError(ae);
			parent.NotifyCompleted();
		}
	}
}
