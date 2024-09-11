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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;
using System.Reflection;
using System.Text;

namespace Aerospike.Test
{
	[TestClass]
	public class TestTxn : TestSync
	{
		private static readonly string binName = "bin";

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			if (!args.testProxy || (args.testProxy && nativeClient != null))
			{
				Assembly assembly = Assembly.GetExecutingAssembly();
				RegisterTask task = nativeClient.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
				task.Wait();
			}
		}

		[TestMethod]
		public void TxnWrite()
		{
			Key key = new(args.ns, args.set, "mrtkey111");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val2"));

			client.Commit(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TxnWriteTwice()
		{
			Key key = new(args.ns, args.set, "mrtkey2");

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val1"));
			client.Put(wp, key, new Bin(binName, "val2"));

			client.Commit(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TxnWriteConflict()
		{
			Key key = new(args.ns, args.set, "mrtkey21");

			Txn txn1 = new();
			Txn txn2 = new();

			WritePolicy wp1 = client.WritePolicyDefault;
			WritePolicy wp2 = client.WritePolicyDefault;
			wp1.Txn = txn1;
			wp2.Txn = txn2;

			client.Put(wp1, key, new Bin(binName, "val1"));

			try
			{
				client.Put(wp2, key, new Bin(binName, "val2"));
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.MRT_BLOCKED)
				{
					throw;
				}
			}

			client.Commit(txn1);
			client.Commit(txn2);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnWriteBlock()
		{
			Key key = new(args.ns, args.set, "mrtkey3");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val2"));

			try
			{
				// This write should be blocked.
				client.Put(null, key, new Bin(binName, "val3"));
				throw new AerospikeException("Unexpected success");
			}
			catch (AerospikeException e)
			{
				if (e.Result != ResultCode.MRT_BLOCKED)
				{
					throw;
				}
			}

			client.Commit(txn);
		}

		[TestMethod]
		public void TxnWriteRead()
		{
			Key key = new(args.ns, args.set, "mrtkey4");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val2"));

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");

			client.Commit(txn);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TxnWriteAbort()
		{
			Key key = new(args.ns, args.set, "mrtkey5");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val2"));

			Policy p = client.ReadPolicyDefault;
			p.Txn = txn;
			Record record = client.Get(p, key);
			AssertBinEqual(key, record, binName, "val2");

			client.Abort(txn);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnDelete()
		{
			Key key = new(args.ns, args.set, "mrtkey6");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			wp.durableDelete = true;
			client.Delete(wp, key);

			client.Commit(txn);

			Record record = client.Get(null, key);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void TxnDeleteAbort()
		{
			Key key = new(args.ns, args.set, "mrtkey7");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			wp.durableDelete = true;
			client.Delete(wp, key);

			client.Abort(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnDeleteTwice()
		{
			Key key = new(args.ns, args.set, "mrtkey8");

			Txn txn = new();

			client.Put(null, key, new Bin(binName, "val1"));

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			wp.durableDelete = true;
			client.Delete(wp, key);
			client.Delete(wp, key);

			client.Commit(txn);

			Record record = client.Get(null, key);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void TxnTouch()
		{
			Key key = new(args.ns, args.set, "mrtkey91");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Touch(wp, key);

			client.Commit(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnTouchAbort()
		{
			Key key = new(args.ns, args.set, "mrtkey10");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Touch(wp, key);

			client.Abort(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnOperateWrite()
		{
			Key key = new(args.ns, args.set, "mrtkey11");

			client.Put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			Record record = client.Operate(wp, key,
				Operation.Put(new Bin(binName, "val2")),
				Operation.Get("bin2")
			);
			AssertBinEqual(key, record, "bin2", "bal1");

			client.Commit(txn);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TxnOperateWriteAbort()
		{
			Key key = new(args.ns, args.set, "mrtkey12");

			client.Put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			Record record = client.Operate(wp, key,
				Operation.Put(new Bin(binName, "val2")),
				Operation.Get("bin2")
			);
			AssertBinEqual(key, record, "bin2", "bal1");

			client.Abort(txn);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnUDF()
		{
			Key key = new(args.ns, args.set, "mrtkey13");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Execute(wp, key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2"));

			client.Commit(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TxnUDFAbort()
		{
			Key key = new(args.ns, args.set, "mrtkey14");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Execute(wp, key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2"));

			client.Abort(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnBatch()
		{
			Key[] keys = new Key[10];
			Bin bin = new(binName, 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = new(args.ns, args.set, i);
				keys[i] = key;

				client.Put(null, key, bin);
			}

			Record[] recs = client.Get(null, keys);
			AssertBatchEqual(keys, recs, 1);

			Txn txn = new();

			bin = new(binName, 2);

			BatchPolicy bp = BatchPolicy.WriteDefault();
			bp.Txn = txn;

			BatchResults bresults = client.Operate(bp, null, keys, Operation.Put(bin));

			if (!bresults.status)
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("Batch failed:");
				sb.Append(System.Environment.NewLine);

				foreach (BatchRecord br in bresults.records)
				{
					if (br.resultCode == 0)
					{
						sb.Append("Record: " + br.record);
					}
					else
					{
						sb.Append("ResultCode: " + br.resultCode);
					}
					sb.Append(System.Environment.NewLine);
				}

				throw new AerospikeException(sb.ToString());
			}

			client.Commit(txn);

			recs = client.Get(null, keys);
			AssertBatchEqual(keys, recs, 2);
		}

		[TestMethod]
		public void TxnBatchAbort()
		{
			var keys = new Key[10];
			Bin bin = new(binName, 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = new(args.ns, args.set, i);
				keys[i] = key;

				client.Put(null, key, bin);
			}

			Record[] recs = client.Get(null, keys);
			AssertBatchEqual(keys, recs, 1);

			Txn txn = new();

			bin = new Bin(binName, 2);

			BatchPolicy bp = BatchPolicy.WriteDefault();
			bp.Txn = txn;

			BatchResults bresults = client.Operate(bp, null, keys, Operation.Put(bin));

			if (!bresults.status)
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("Batch failed:");
				sb.Append(System.Environment.NewLine);

				foreach (BatchRecord br in bresults.records)
				{
					if (br.resultCode == 0)
					{
						sb.Append("Record: " + br.record);
					}
					else
					{
						sb.Append("ResultCode: " + br.resultCode);
					}
					sb.Append(System.Environment.NewLine);
				}

				throw new AerospikeException(sb.ToString());
			}

			client.Abort(txn);

			recs = client.Get(null, keys);
			AssertBatchEqual(keys, recs, 1);
		}

		private void AssertBatchEqual(Key[] keys, Record[] recs, int expected)
		{
			for (int i = 0; i < keys.Length; i++)
			{
				Key key = keys[i];
				Record rec = recs[i];

				Assert.IsNotNull(rec);

				int received = rec.GetInt(binName);
				Assert.AreEqual(expected, received);
			}
		}
	}
}
