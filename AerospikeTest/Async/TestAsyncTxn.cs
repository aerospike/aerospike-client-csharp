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
using static Aerospike.Client.CommitStatus;
using static Aerospike.Client.AbortStatus;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncTxn : TestAsync
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
		public void AsyncTxnWrite()
		{
			Key key = new(args.ns, args.set, "asyncTxnWrite");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, new PutHandler(this), key, new Bin(binName, "val2"));

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val2"), key);
		}

		[TestMethod]
		public void AsyncTxnWriteTwice()
		{
			Key key = new(args.ns, args.set, "asyncTxnWriteTwice");

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, new PutHandler(this), key, new Bin(binName, "val1"));
			client.Put(wp, new PutHandler(this), key, new Bin(binName, "val2"));

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val2"), key);
		}

		[TestMethod]
		public void AsyncTxnWriteBlock()
		{
			Key key = new(args.ns, args.set, "asyncTxnWriteBlock");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, new PutHandler(this), key, new Bin(binName, "val2"));

			try
			{
				// This write should be blocked.
				client.Put(null, new PutHandler(this), key, new Bin(binName, "val3"));
				throw new AerospikeException("Unexpected success");
			}
			catch (AerospikeException e)
			{
				if (e.Result != ResultCode.MRT_BLOCKED)
				{
					throw e;
				}
			}

			client.Commit(new CommitHandler(this), txn);
		}

		[TestMethod]
		public void AsyncTxnWriteRead()
		{
			Key key = new(args.ns, args.set, "asyncTxnWriteRead");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, new PutHandler(this), key, new Bin(binName, "val2"));

			client.Get(null, new GetExpectHandler(this, "val1"), key);

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val2"), key);
		}

		[TestMethod]
		public void AsyncTxnWriteAbort()
		{
			Key key = new(args.ns, args.set, "asyncTxnWriteAbort");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Put(wp, new PutHandler(this), key, new Bin(binName, "val2"));

			Policy p = client.ReadPolicyDefault;
			p.Txn = txn;
			client.Get(p, new GetExpectHandler(this, "val2"), key);

			client.Abort(new AbortHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val1"), key);
		}

		[TestMethod]
		public void AsyncTxnDelete()
		{
			Key key = new(args.ns, args.set, "asyncTxnDelete");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			wp.durableDelete = true;
			client.Delete(wp, new DeleteHandler(this), key);

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, null), key);
		}

		[TestMethod]
		public void AsyncTxnDeleteAbort()
		{
			Key key = new(args.ns, args.set, "asyncTxnDeleteAbort");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			wp.durableDelete = true;
			client.Delete(wp, new DeleteHandler(this), key);

			client.Abort(new AbortHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val1"), key);
		}

		[TestMethod]
		public void AsyncTxnDeleteTwice()
		{
			Key key = new(args.ns, args.set, "asyncTxnDeleteTwice");

			Txn txn = new();

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			wp.durableDelete = true;
			client.Delete(wp, new DeleteHandler(this), key);
			client.Delete(wp, new DeleteHandler(this), key);

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, null), key);
		}

		[TestMethod]
		public void AsyncTxnTouch()
		{
			Key key = new(args.ns, args.set, "asyncTxnTouch");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Touch(wp, new TouchHandler(this), key);

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val1"), key);
		}

		[TestMethod]
		public void AsyncTxnTouchAbort()
		{
			Key key = new(args.ns, args.set, "asyncTxnTouchAbort");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Touch(wp, new TouchHandler(this), key);

			client.Abort(new AbortHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val1"), key);
		}

		[TestMethod]
		public void AsyncTxnOperateWrite()
		{
			Key key = new(args.ns, args.set, "asyncTxnOperateWrite3");
			Bin bin2 = new("bin2", "bal1");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"), bin2);

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Operate(wp, new OperateExpectHandler(this, bin2), key,
				Operation.Put(new Bin(binName, "val2")),
				Operation.Get("bin2")
			);

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val2"), key);
		}

		[TestMethod]
		public void AsyncTxnOperateWriteAbort()
		{
			Key key = new(args.ns, args.set, "asyncTxnOperateWriteAbort");
			Bin bin2 = new("bin2", "bal1");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"), bin2);

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Operate(wp, new OperateExpectHandler(this, bin2), key,
				Operation.Put(new Bin(binName, "val2")),
				Operation.Get(bin2.name)
			);

			client.Abort(new AbortHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val1"), key);
		}

		[TestMethod]
		public void AsyncTxnUDF()
		{
			Key key = new(args.ns, args.set, "asyncTxnUDF");
			Bin bin2 = new("bin2", "bal1");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"), bin2);

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Execute(wp, new UDFHandler(this), key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2"));

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val2"), key);
		}

		[TestMethod]
		public void AsyncTxnUDFAbort()
		{
			Key key = new(args.ns, args.set, "asyncTxnUDFAbort");
			Bin bin2 = new("bin2", "bal1");

			client.Put(null, new PutHandler(this), key, new Bin(binName, "val1"));

			Txn txn = new();

			WritePolicy wp = client.WritePolicyDefault;
			wp.Txn = txn;
			client.Execute(wp, new UDFHandler(this), key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2"));

			client.Abort(new AbortHandler(this), txn);

			client.Get(null, new GetExpectHandler(this, "val1"), key);
		}

		[TestMethod]
		public void AsyncTxnBatch()
		{
			Key[] keys = new Key[10];
			Bin bin = new(binName, 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = new(args.ns, args.set, "asyncTxnBatch" + i);
				keys[i] = key;

				client.Put(null, key, bin);
			}

			client.Get(null, new BatchGetExpectHandler(this, 1), keys);

			Txn txn = new();

			bin = new(binName, 2);

			BatchPolicy bp = BatchPolicy.WriteDefault();
			bp.Txn = txn;

			client.Operate(bp, null, new BatchOperateHandler(this), keys, Operation.Put(bin));

			client.Commit(new CommitHandler(this), txn);

			client.Get(null, new BatchGetExpectHandler(this, 1), keys);
		}

		[TestMethod]
		public void AsyncTxnBatchAbort()
		{
			var keys = new Key[10];
			Bin bin = new(binName, 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = new(args.ns, args.set, "asyncTxnBatch" + i);
				keys[i] = key;

				client.Put(null, key, bin);
			}

			client.Get(null, new BatchGetExpectHandler(this, 1), keys);

			Txn txn = new();

			bin = new Bin(binName, 2);

			BatchPolicy bp = BatchPolicy.WriteDefault();
			bp.Txn = txn;

			client.Operate(bp, null, new BatchOperateHandler(this), keys, Operation.Put(bin));

			client.Abort(new AbortHandler(this), txn);

			client.Get(null, new BatchGetExpectHandler(this, 1), keys);
		}

		private class CommitHandler : CommitListener
		{
			private readonly TestAsyncTxn parent;

			public CommitHandler(TestAsyncTxn parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(CommitStatusType status)
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException.Commit e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class AbortHandler : AbortListener
		{
			private readonly TestAsyncTxn parent;

			public AbortHandler(TestAsyncTxn parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(AbortStatusType status)
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class PutHandler : WriteListener
		{
			private readonly TestAsyncTxn parent;

			public PutHandler(TestAsyncTxn parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key key)
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class GetExpectHandler : RecordListener
		{
			private readonly TestAsyncTxn parent;
			private string expect;

			public GetExpectHandler(TestAsyncTxn parent, string expect)
			{
				this.parent = parent;
				this.expect = expect;
			}

			public void OnSuccess(Key key, Record record)
			{
				if (expect != null)
				{
					if (parent.AssertBinEqual(key, record, binName, expect))
					{
						parent.NotifyCompleted();
					}
					else
					{
						parent.NotifyCompleted();
					}
				}
				else
				{
					if (parent.AssertRecordNotFound(key, record))
					{
						parent.NotifyCompleted();
					}
					else
					{
						parent.NotifyCompleted();
					}
				}
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class OperateExpectHandler : RecordListener
		{
			private readonly TestAsyncTxn parent;
			private Bin? expect;

			public OperateExpectHandler(TestAsyncTxn parent, Bin? expect)
			{
				this.parent = parent;
				this.expect = expect;
			}

			public void OnSuccess(Key key, Record record)
			{
				if (expect != null)
				{
					if (parent.AssertBinEqual(key, record, expect?.name, expect?.value.Object))
					{
						parent.NotifyCompleted();
					}
					else
					{
						parent.NotifyCompleted();
					}
				}
				else
				{
					if (parent.AssertRecordNotFound(key, record))
					{
						parent.NotifyCompleted();
					}
					else
					{
						parent.NotifyCompleted();
					}
				}
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class UDFHandler : ExecuteListener
		{
			private readonly TestAsyncTxn parent;

			public UDFHandler(TestAsyncTxn parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key key, Object obj)
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class BatchGetExpectHandler : RecordArrayListener
		{
			private readonly TestAsyncTxn parent;
			private readonly int expected;

			public BatchGetExpectHandler(TestAsyncTxn parent, int expected)
			{
				this.parent = parent;
				this.expected = expected;
			}

			public void OnSuccess(Key[] keys, Record[] records)
			{
				if (parent.AssertBatchEqual(keys, records, binName, expected))
				{
					parent.NotifyCompleted();
				}
				else 
				{ 
					parent.NotifyCompleted(); 
				}
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class BatchOperateHandler : BatchRecordArrayListener
		{
			private TestAsyncTxn parent;

			public BatchOperateHandler(TestAsyncTxn parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(BatchRecord[] records, bool status)
			{
				if (status)
				{
					parent.NotifyCompleted();
				}
				else
				{
					StringBuilder sb = new StringBuilder();
					sb.Append("Batch failed:");
					sb.Append(System.Environment.NewLine);

					foreach (BatchRecord br in records)
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
					parent.SetError(new AerospikeException(sb.ToString()));
					parent.NotifyCompleted();
				}
			}

			public void OnFailure(BatchRecord[] records, AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class TouchHandler : WriteListener
		{
			private TestAsyncTxn parent;

			public TouchHandler(TestAsyncTxn parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key key)
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private class DeleteHandler : DeleteListener
		{
			private TestAsyncTxn parent;

			public DeleteHandler(TestAsyncTxn parent)
			{
				this.parent = parent;
			}

			public void OnSuccess(Key key, bool existed)
			{
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
