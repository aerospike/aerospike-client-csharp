/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	[TestClass, TestCategory("SCMode")]
	public class TestTxn : TestSync
	{
		private static readonly string binName = "bin";

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua", "record_example.lua", Language.LUA);
			task.Wait();
		}

		[TestMethod]
		public void TxnWrite()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey1");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val2"));

			client.Commit(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TxnWriteTwice()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey2");

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey021");

			Txn txn1 = new();
			Txn txn2 = new();

			WritePolicy wp1 = client.WritePolicyDefault.Clone();
			WritePolicy wp2 = client.WritePolicyDefault.Clone();
			wp1.Txn = txn1;
			wp2.Txn = txn2;

			client.Put(wp1, key, new Bin(binName, "val1"));

			try
			{
				client.Put(wp2, key, new Bin(binName, "val2"));
				throw new AerospikeException("Unexpected success");
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey3");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey4");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey5");

			client.Delete(null, key);

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val2"));

			Policy p = client.ReadPolicyDefault.Clone();
			p.Txn = txn;
			Record record = client.Get(p, key);
			AssertBinEqual(key, record, binName, "val2");

			client.Abort(txn);

			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
			Assert.AreEqual(3, record.generation);
		}

		[TestMethod]
		public void TxnDelete()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey6");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey7");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey8");

			Txn txn = new();

			client.Put(null, key, new Bin(binName, "val1"));

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey91");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Touch(wp, key);

			client.Commit(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnTouchAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey10");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Touch(wp, key);

			client.Abort(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val1");
		}

		[TestMethod]
		public void TxnOperateWrite()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey11");

			client.Put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey12");

			client.Put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey13");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Execute(wp, key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2"));

			client.Commit(txn);

			Record record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");
		}

		[TestMethod]
		public void TxnUDFAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey14");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
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
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, i);
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
				StringBuilder sb = new();
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
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, i);
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
				StringBuilder sb = new();
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

		[TestMethod]
		public void TxnWriteCommitAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey15");

			client.Put(null, key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val2"));

			Policy p = client.ReadPolicyDefault.Clone();
			p.Txn = txn;
			Record record = client.Get(p, key);
			AssertBinEqual(key, record, binName, "val2");

			client.Commit(txn);
			record = client.Get(null, key);
			AssertBinEqual(key, record, binName, "val2");

			try
			{
				var abortStatus = client.Abort(txn);
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.TXN_ALREADY_COMMITTED)
				{
					throw;
				}
			}
		}

		[TestMethod]
		public void TxnWriteReadTwoTxn()
		{
			Txn txn1 = new();
			Txn txn2 = new();

			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey16");

			client.Put(null, key, new Bin(binName, "val1"));

			var rp1 = client.ReadPolicyDefault.Clone();
			rp1.Txn = txn1;
			var record = client.Get(rp1, key);
			AssertBinEqual(key, record, binName, "val1");

			var rp2 = client.ReadPolicyDefault.Clone();
			rp2.Txn = txn2;
			record = client.Get(rp2, key);
			AssertBinEqual(key, record, binName, "val1");

			var status = client.Commit(txn1);
			Assert.AreEqual(CommitStatus.CommitStatusType.OK, status);

			status = client.Commit(txn2);
			Assert.AreEqual(CommitStatus.CommitStatusType.OK, status);
		}

		[TestMethod]
		public void TxnLUTCommit() // Test Case 38
		{
			Txn txn = new(); // T0

			Key key1 = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey17");
			Key key2 = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey18");
			Key key3 = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey19");

			client.Delete(null, key1);
			client.Delete(null, key2);
			client.Delete(null, key3);

			var wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Put(wp, key1, new Bin(binName, "val1")); // T1

			var p = client.ReadPolicyDefault.Clone();
			p.Txn = txn;
			var record = client.Get(p, key1); // T2
			Assert.AreEqual(1, record.generation);

			client.Put(wp, key1, new Bin(binName, "val11")); // T3

			record = client.Get(p, key1); // T4
			Assert.AreEqual(2, record.generation);

			client.Put(null, key2, new Bin(binName, "val1")); // T5

			record = client.Get(p, key2); // T6
			Assert.AreEqual(1, record.generation);

			client.Put(wp, key2, new Bin(binName, "val11")); // T7

			record = client.Get(p, key2); // T8
			Assert.AreEqual(2, record.generation);

			client.Put(wp, key3, new Bin(binName, "val1")); // T9

			record = client.Get(p, key3); // T10
			Assert.AreEqual(1, record.generation);

			client.Commit(txn); // T11

			record = client.Get(null, key1); // T12
			Assert.AreEqual(3, record.generation);
			record = client.Get(null, key2);
			Assert.AreEqual(3, record.generation);
			record = client.Get(null, key3);
			Assert.AreEqual(2, record.generation);
		}

		[TestMethod]
		public void TxnLUTAbort() // Test Case 39
		{
			client.Truncate(null, SuiteHelpers.ns, SuiteHelpers.set, DateTime.Now);
			
			Txn txn = new(); // T0

			Key key1 = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey20");
			Key key2 = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey21");
			Key key3 = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey22");

			//client.Delete(null, key1);
			//client.Delete(null, key2);
			//client.Delete(null, key3);

			client.Put(null, key1, new Bin(binName, "val1")); // T1

			var p = client.ReadPolicyDefault.Clone();
			p.Txn = txn;
			var record = client.Get(p, key1); // T2
			Assert.AreEqual(1, record.generation);

			var binR2O = new Bin(binName, "val2");
			client.Put(null, key2, binR2O); // T3
			record = client.Get(p, key2); // T4
			Assert.AreEqual(1, record.generation);

			var wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Put(wp, key2, new Bin(binName, "val11")); // T5

			record = client.Get(p, key2);
			Assert.AreEqual(2, record.generation);

			record = client.Get(null, key2); // T6
			Assert.AreEqual(1, record.generation);

			client.Put(wp, key3, new Bin(binName, "val3")); // T7
			record = client.Get(p, key3);
			Assert.AreEqual(1, record.generation);

			var binR1UO = new Bin(binName, "val1"); // T8
			client.Put(null, key1, binR1UO);
			record = client.Get(null, key1);
			Assert.AreEqual(2, record.generation);

			try
			{
				client.Put(wp, key1, new Bin(binName, "val1111")); // T9
				record = client.Get(p, key1);
				Assert.AreEqual(2, record.generation);
				throw new AerospikeException("Unexpected success");
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.MRT_VERSION_MISMATCH)
				{
					throw;
				}
			}

			try
			{
				client.Commit(txn); // T10
			}
			catch (AerospikeException.Commit)
			{

			}

			record = client.Get(null, key1); // T11
			Assert.AreEqual(2, record.generation);
			AssertBinEqual(key1, record, binR1UO);
			record = client.Get(null, key2);
			Assert.AreEqual(3, record.generation);
			AssertBinEqual(key2, record, binR2O);
			record = client.Get(null, key3);
			Assert.IsNull(record);

			// Cleanup
			client.Abort(txn);
		}

		[TestMethod]
		public void TxnWriteAfterCommit()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "mrtkey23");

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;
			client.Put(wp, key, new Bin(binName, "val1"));

			client.Commit(txn);

			try
			{
				client.Put(wp, key, new Bin(binName, "val1"));
				throw new AerospikeException("Unexpected success");
			}
			catch (AerospikeException ae)
			{
				if (!ae.Message.Contains("Command not allowed in current MRT state:"))
				{
					throw;
				}
			}

		}

		[TestMethod]
		public void TxnInvalidNamespace()
		{
			Key key = new("invalid", SuiteHelpers.set, "mrtkey");

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault.Clone();
			wp.Txn = txn;

			try
			{
				client.Put(wp, key, new Bin(binName, "val1"));
				client.Commit(txn);
				throw new AerospikeException("Unexpected success");
			}
			catch (AerospikeException e)
			{
				if (e.Result != ResultCode.INVALID_NAMESPACE)
				{
					throw;
				}
			}
		}

		private static void AssertBatchEqual(Key[] keys, Record[] recs, int expected)
		{
			for (int i = 0; i < keys.Length; i++)
			{
				_ = keys[i];
				Record rec = recs[i];

				Assert.IsNotNull(rec);

				int received = rec.GetInt(binName);
				Assert.AreEqual(expected, received);
			}
		}
	}
}
