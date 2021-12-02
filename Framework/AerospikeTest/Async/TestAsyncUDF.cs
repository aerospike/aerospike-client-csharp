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
using System.Collections.Generic;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncUDF : TestAsync
	{
		private static readonly string binName = args.GetBinName("audfbin1");
		private const string binValue = "string value";

		[ClassInitialize()]
		public static void Register(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.Resources.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();
		}

		[TestMethod]
		public void AsyncUDF()
		{
			Key key = new Key(args.ns, args.set, "audfkey1");
			Bin bin = new Bin(binName, binValue);

			// Write bin
			client.Execute(null, new WriteHandler(this, key), key, "record_example", "writeBin", Value.Get(bin.name), bin.value);
			WaitTillComplete();
		}

		private class WriteHandler : ExecuteListener
		{
			private readonly TestAsyncUDF parent;
			private Key key;

			public WriteHandler(TestAsyncUDF parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key key, object obj)
			{
				// Write succeeded.  Now call read using udf.
				client.Execute(null, new ReadHandler(parent, key), key, "record_example", "readBin", Value.Get(binName));
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class ReadHandler : ExecuteListener
		{
			private readonly TestAsyncUDF parent;
			private Key key;

			public ReadHandler(TestAsyncUDF parent, Key key)
			{
				this.parent = parent;
				this.key = key;
			}

			public void OnSuccess(Key key, object received)
			{
				if (parent.AssertNotNull(received)) {
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
			Key[] keys = new Key[]
			{
				new Key(args.ns, args.set, 20000),
				new Key(args.ns, args.set, 20001)
			};

			client.Delete(null, null, keys);

			client.Execute(null, null, new BatchUDFHandler(this), keys, "record_example", "writeBin", Value.Get("B5"), Value.Get("value5"));

			WaitTillComplete();
		}

		private class BatchUDFHandler : BatchRecordArrayListener
		{
			private readonly TestAsyncUDF parent;

			public BatchUDFHandler(TestAsyncUDF parent)
			{
				this.parent = parent;
			}

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
			List<BatchRecord> records = new List<BatchRecord>();
			string bin = "B5";

			Value[] a1 = new Value[] { Value.Get(bin), Value.Get("value1") };
			Value[] a2 = new Value[] { Value.Get(bin), Value.Get(5) };
			Value[] a3 = new Value[] { Value.Get(bin), Value.Get(999) };

			BatchUDF b1 = new BatchUDF(new Key(args.ns, args.set, 20014), "record_example", "writeBin", a1);
			BatchUDF b2 = new BatchUDF(new Key(args.ns, args.set, 20015), "record_example", "writeWithValidation", a2);
			BatchUDF b3 = new BatchUDF(new Key(args.ns, args.set, 20015), "record_example", "writeWithValidation", a3);
			BatchRead b4 = new BatchRead(new Key(args.ns, args.set, 20014), true);
			BatchRead b5 = new BatchRead(new Key(args.ns, args.set, 20015), true);

			records.Add(b1);
			records.Add(b2);
			records.Add(b3);
			records.Add(b4);
			records.Add(b5);

			client.Operate(null, new BatchSeqUDFHandler(this, bin), records);

			WaitTillComplete();
		}

		private class BatchSeqUDFHandler : BatchRecordSequenceListener
		{
			private readonly TestAsyncUDF parent;
			private string bin;

			public BatchSeqUDFHandler(TestAsyncUDF parent, string bin)
			{
				this.parent = parent;
				this.bin = bin;
			}

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

						case 3:
							parent.AssertBinEqual(br.key, br.record, bin, "value1");
							break;

						case 4:
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
}
