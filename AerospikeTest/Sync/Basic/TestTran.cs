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
	public class TestTran : TestSync
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
		public void TranWrite()
		{
			Key key = new Key(args.ns, args.set, "mrtkey1");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			client.Put(wp, key, new Bin(binName, "val2"));

			client.Commit(tran);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TranWriteTwice()
		{
			Key key = new Key(args.ns, args.set, "mrtkey2");

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			client.Put(wp, key, new Bin(binName, "val1"));
			client.Put(wp, key, new Bin(binName, "val2"));

			client.Commit(tran);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void tranWriteConflict()
		{
			Key key = new Key(args.ns, args.set, "mrtkey21");

			Tran tran1 = new Tran();
			Tran tran2 = new Tran();

			WritePolicy wp1 = client.WritePolicyDefault;
			WritePolicy wp2 = client.WritePolicyDefault;
			wp1.Tran = tran1;
			wp2.Tran = tran2;

			client.Put(wp1, key, new Bin(binName, "val1"));

			try
			{
				client.Put(wp2, key, new Bin(binName, "val2"));
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.MRT_BLOCKED)
				{
					throw ae;
				}
			}

			client.Commit(tran1);
			client.Commit(tran2);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TranWriteBlock()
		{
			Key key = new Key(args.ns, args.set, "mrtkey3");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
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
					throw e;
				}
			}

			client.Commit(tran);
		}

		[TestMethod]
		public void TranWriteRead()
		{
			Key key = new Key(args.ns, args.set, "mrtkey4");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			client.Put(wp, key, new Bin(binName, "val2"));

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");

			client.Commit(tran);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TranWriteAbort()
		{
			Key key = new Key(args.ns, args.set, "mrtkey5");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			client.Put(wp, key, new Bin(binName, "val2"));

			Policy p = client.ReadPolicyDefault;
			p.Tran = tran;
			Record record = client.Get(p, key);
			AssertBinEqual(key, record, binName, "val2");

			client.Abort(tran);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TranDelete()
		{
			Key key = new Key(args.ns, args.set, "mrtkey6");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			wp.durableDelete = true;
			client.Delete(wp, key);

			client.Commit(tran);

			Record record = client.Get(null, key);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void TranDeleteAbort()
		{
			Key key = new Key(args.ns, args.set, "mrtkey7");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			wp.durableDelete = true;
			client.Delete(wp, key);

			client.Abort(tran);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TranDeleteTwice()
		{
			Key key = new Key(args.ns, args.set, "mrtkey8");

			Tran tran = new Tran();

			client.Put(null, key, new Bin(binName, "val1"));

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			wp.durableDelete = true;
			client.Delete(wp, key);
			client.Delete(wp, key);

			client.Commit(tran);

			Record record = client.Get(null, key);
			Assert.IsNull(record);
		}

		[TestMethod]
		public void TranTouch()
		{
			Key key = new Key(args.ns, args.set, "mrtkey9");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			client.Touch(wp, key);

			client.Commit(tran);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TranTouchAbort()
		{
			Key key = new Key(args.ns, args.set, "mrtkey10");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			client.Touch(wp, key);

			client.Abort(tran);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TranOperateWrite()
		{
			Key key = new Key(args.ns, args.set, "mrtkey11");

			client.Put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			Record record = client.Operate(wp, key,
				Operation.Put(new Bin(binName, "val2")),
				Operation.Get("bin2")
			);
			AssertBinEqual(key, record, "bin2", "bal1");

			client.Commit(tran);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TranOperateWriteAbort()
		{
			Key key = new Key(args.ns, args.set, "mrtkey12");

			client.Put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			Record record = client.Operate(wp, key,
				Operation.Put(new Bin(binName, "val2")),
				Operation.Get("bin2")
			);
			AssertBinEqual(key, record, "bin2", "bal1");

			client.Abort(tran);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TranUDF()
		{
			Key key = new Key(args.ns, args.set, "mrtkey13");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			client.Execute(wp, key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2"));

			client.Commit(tran);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TranUDFAbort()
		{
			Key key = new Key(args.ns, args.set, "mrtkey14");

			client.Put(null, key, new Bin(binName, "val1"));

			Tran tran = new Tran();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Tran = tran;
			client.Execute(wp, key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2"));

			client.Abort(tran);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TranBatch()
		{
			Key[] keys = new Key[10];
			Bin bin = new Bin(binName, 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = new Key(args.ns, args.set, i);
				keys[i] = key;

				client.Put(null, key, bin);
			}

			Record[] recs = client.Get(null, keys);
			AssertBatchEqual(keys, recs, 1);

			Tran tran = new Tran();

			bin = new Bin(binName, 2);

			BatchPolicy bp = BatchPolicy.WriteDefault();
			bp.Tran = tran;

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

			client.Commit(tran);

			recs = client.Get(null, keys);
			AssertBatchEqual(keys, recs, 2);
		}

		[TestMethod]
		public void TranBatchAbort()
		{
			Key[] keys = new Key[10];
			Bin bin = new Bin(binName, 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = new Key(args.ns, args.set, i);
				keys[i] = key;

				client.Put(null, key, bin);
			}

			Record[] recs = client.Get(null, keys);
			AssertBatchEqual(keys, recs, 1);

			Tran tran = new Tran();

			bin = new Bin(binName, 2);

			BatchPolicy bp = BatchPolicy.WriteDefault();
			bp.Tran = tran;

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

			client.Abort(tran);

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
