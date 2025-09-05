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
using Aerospike.Client;
using System.Reflection;
using System.Text;
using static Aerospike.Client.AbortStatus;
using static Aerospike.Client.CommitStatus;

namespace Aerospike.Test
{
	[TestClass, TestCategory("SCMode")]
	public class TestAsyncTxn : TestAsync
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
		public void AsyncTxnWrite()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnWrite");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Put(txn, key, "val2"),
				new Commit(txn),
				new GetExpect(null, key, "val2")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnWriteTwice()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnWriteTwice");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(txn, key, "val1"),
				new Put(txn, key, "val2"),
				new Commit(txn),
				new GetExpect(null, key, "val2")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnWriteBlock()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnWriteBlock");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Put(txn, key, "val2"),
				new Put(null, key, "val3", ResultCode.MRT_BLOCKED), // Should be blocked
				new Commit(txn),
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnWriteRead()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnWriteRead");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Put(txn, key, "val2"),
				new GetExpect(null, key, "val1"),
				new Commit(txn),
				new GetExpect(null, key, "val2")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnWriteAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnWriteAbort");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Put(txn, key, "val2"),
				new GetExpect(txn, key, "val2"),
				new Abort(txn),
				new GetExpect(null, key, "val1")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnDelete()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnDelete");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Delete(txn, key),
				new Commit(txn),
				new GetExpect(null, key, null)
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnDeleteAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnDeleteAbort");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Delete(txn, key),
				new Abort(txn),
				new GetExpect(null, key, "val1")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnDeleteTwice()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnDeleteTwice");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Delete(txn, key),
				new Delete(txn, key),
				new Commit(txn),
				new GetExpect(null, key, null)
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnTouch()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnTouch");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Touch(txn, key),
				new Commit(txn),
				new GetExpect(null, key, "val1")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnTouchAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnTouchAbort");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Touch(txn, key),
				new Abort(txn),
				new GetExpect(null, key, "val1")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnOperateWrite()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnOperateWrite3");
			using Txn txn = new();
			Bin bin2 = new("bin2", "bal1");

			var cmds = new IRunner[]
			{
				new Put(null, key, new Bin(binName, "val1"), bin2),
				new OperateExpect(txn, key,
					bin2,
					Operation.Put(new Bin(binName, "val2")),
					Operation.Get(bin2.name)
				),
				new Commit(txn),
				new GetExpect(null, key, "val2")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnOperateWriteAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnOperateWriteAbort");
			using Txn txn = new();
			Bin bin2 = new("bin2", "bal1");

			var cmds = new IRunner[]
			{
				new Put(null, key, new Bin(binName, "val1"), bin2),
				new OperateExpect(txn, key,
					bin2,
					Operation.Put(new Bin(binName, "val2")),
					Operation.Get(bin2.name)
				),
				new Abort(txn),
				new GetExpect(null, key, "val1")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnUDF()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnUDF");
			using Txn txn = new();
			Bin bin2 = new("bin2", "bal1");

			var cmds = new IRunner[]
			{
				new Put(null, key, new Bin(binName, "val1"), bin2),
				new UDF(txn, key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2")),
				new Commit(txn),
				new GetExpect(null, key, "val2")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnUDFAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnUDFAbort");
			using Txn txn = new();
			Bin bin2 = new("bin2", "bal1");

			var cmds = new IRunner[]
			{
				new Put(null, key, new Bin(binName, "val1"), bin2),
				new UDF(txn, key, "record_example", "writeBin", Value.Get(binName), Value.Get("val2")),
				new Abort(txn),
				new GetExpect(null, key, "val1")
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnBatch()
		{
			Key[] keys = new Key[10];
			Bin bin = new(binName, 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnBatch" + i);
				keys[i] = key;

				client.Put(null, key, bin);
			}

			using Txn txn = new();
			bin = new(binName, 2);

			var cmds = new IRunner[]
			{
				new BatchGetExpect(null, keys, 1),
				new BatchOperate(txn, keys, Operation.Put(bin)),
				new Commit(txn),
				new BatchGetExpect(null, keys, 2),
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnBatchAbort()
		{
			var keys = new Key[10];
			Bin bin = new(binName, 1);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnBatch" + i);
				keys[i] = key;

				client.Put(null, key, bin);
			}

			using Txn txn = new();
			bin = new Bin(binName, 2);

			var cmds = new IRunner[]
			{
				new BatchGetExpect(null, keys, 1),
				new BatchOperate(txn, keys, Operation.Put(bin)),
				new Abort(txn),
				new BatchGetExpect(null, keys, 1),
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnWriteCommitAbort()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnCommitAbort");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new Put(txn, key, "val2"),
				new Commit(txn),
				new GetExpect(null, key, "val2"),
				new Abort(txn, ResultCode.TXN_ALREADY_COMMITTED)
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnWriteReadTwoTxn()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnWriteReadTwoTxn");
			using Txn txn1 = new();
			using Txn txn2 = new();

			var cmds = new IRunner[]
			{
				new Put(null, key, "val1"),
				new GetExpect(txn1, key, "val1"),
				new GetExpect(txn2, key, "val1"),
				new Commit(txn1),
				new Commit(txn2),
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnLUTCommit() // Test Case 38
		{
			Key key1 = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnLUTCommit1");
			Key key2 = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnLUTCommit2");
			Key key3 = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnLUTCommit3");
			using Txn txn = new(); // T1

			var cmds = new IRunner[]
			{
				new Delete(null, key1), // Prep
				new Delete(null, key2),
				new Delete(null, key3),
				new Put(txn, key1, "val1"), // T1
				new GetExpect(txn, key1, "val1", 1), // T2
				new Put(txn, key1, "val11"), // T3
				new GetExpect(txn, key1, "val11", 2), // T4
				new Put(null, key2, "val1"), // T5
				new GetExpect(txn, key2, "val1", 1), // T6
				new Put(txn, key2, "val11"), // T7
				new GetExpect(txn, key2, "val11", 2), // T8
				new Put(txn, key3, "val1"), // T9
				new GetExpect(txn, key3, "val1", 1), // T10
				new Commit(txn), // T11
				new GetExpect(null, key1, "val11", 3), // T12
				new GetExpect(null, key2, "val11", 3),
				new GetExpect(null, key3, "val1", 2)
			};

			Execute(cmds);
		}

		[TestMethod]
		public void AsyncTxnWriteAfterCommit()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "asyncTxnWriteAfter");
			using Txn txn = new();

			var cmds = new IRunner[]
			{
				new Put(txn, key, "val1"),
				new Commit(txn),
				new Sleep(1000),
				new Put(txn, key, "val1", ResultCode.MRT_EXPIRED),
			};

			Execute(cmds);
		}

		private void Execute(IRunner[] cmdArray)
		{
			Cmds a = new(this, cmdArray);
			a.RunNext();
			WaitTillComplete();
		}

		private void OnError(Exception e)
		{
			SetError(e);
			NotifyCompleted();
		}

		private void OnError(Exception e, int expectedResult)
		{
			if (e is AerospikeException ae)
			{
				if (ae.Result == expectedResult)
				{
					NotifyCompleted();
					return;
				}
			}

			OnError(e);
		}

		private void OnError()
		{
			// Error is located in monitor instance which is checked in waitTillComplete();
			NotifyCompleted();
		}

		private class Cmds(TestAsyncTxn parent, TestAsyncTxn.IRunner[] cmds) : IListener
		{
			int idx = -1;

			public void RunNext()
			{
				if (++idx == cmds.Length)
				{
					parent.NotifyCompleted();
					return;
				}

				try
				{
					cmds[idx].Run(parent, this);
				}
				catch (Exception e)
				{
					parent.OnError(e);
				}
			}

			public void OnSuccess()
			{
				RunNext();
			}

			public void OnFailure()
			{
				parent.OnError();
			}

			public void OnFailure(Exception e)
			{
				parent.OnError(e);
			}

			public void OnFailure(Exception e, int expectedResult)
			{
				parent.OnError(e, expectedResult);
			}
		}

		public class Commit : IRunner
		{
			private readonly Txn txn;
			private readonly bool throwsCommitException;

			public Commit(Txn txn)
			{
				this.txn = txn;
				this.throwsCommitException = false;
			}

			public Commit(Txn txn, bool throwsCommitException)
			{
				this.txn = txn;
				this.throwsCommitException = throwsCommitException;
			}

			public void Run(TestAsyncTxn parent, IListener listener)
			{
				client.Commit(new CommitHandler(listener, throwsCommitException), txn);
			}

			private class CommitHandler(TestAsyncTxn.IListener listener, bool throwsCommitException) : CommitListener
			{
				public void OnSuccess(CommitStatusType status)
				{
					if (status == CommitStatusType.OK)
					{
						listener.OnSuccess();
						return;
					}
					listener.OnFailure();
				}

				public void OnFailure(AerospikeException.Commit e)
				{
					if (throwsCommitException)
					{
						listener.OnSuccess();
						return;
					}

					listener.OnFailure(e);
				}
			}
		}



		public class Abort : IRunner
		{
			private readonly Txn txn;
			private readonly AbortStatusType status;
			private readonly int resultCode = 0;

			public Abort(Txn txn)
			{
				this.txn = txn;
				this.status = AbortStatusType.OK;
			}

			public Abort(Txn txn, AbortStatusType abortStatus)
			{
				this.txn = txn;
				this.status = abortStatus;
			}

			public Abort(Txn txn, int resultCode)
			{
				this.txn = txn;
				this.resultCode = resultCode;
			}

			public void Run(TestAsyncTxn parent, IListener listener)
			{
				try
				{
					client.Abort(new AbortHandler(listener, status), txn);
				}
				catch (Exception e)
				{
					parent.OnError(e, resultCode);
				}
			}

			private class AbortHandler(TestAsyncTxn.IListener listener, AbortStatus.AbortStatusType status) : AbortListener
			{
				private readonly AbortStatusType status = status;

				public void OnSuccess(AbortStatusType status)
				{
					if (status == this.status)
					{
						listener.OnSuccess();
						return;
					}
					listener.OnFailure();
				}
			}
		}

		public class Put : IRunner
		{
			private readonly Txn txn;
			private readonly Key key;
			private readonly Bin[] bins;
			private readonly int expectedResult = 0;

			public Put(Txn txn, Key key, string val)
			{
				this.txn = txn;
				this.key = key;
				this.bins = [new(binName, val)];
			}

			public Put(Txn txn, Key key, string val, int expectedResult)
			{
				this.txn = txn;
				this.key = key;
				this.bins = [new(binName, val)];
				this.expectedResult = expectedResult;
			}

			public Put(Txn txn, Key key, params Bin[] bins)
			{
				this.txn = txn;
				this.key = key;
				this.bins = bins;
			}

			public void Run(TestAsyncTxn parent, IListener listener)
			{
				WritePolicy wp = null;
				if (txn != null)
				{
					wp = client.WritePolicyDefault.Clone();
					wp.Txn = txn;
				}
				client.Put(wp, new PutHandler(listener, expectedResult), key, bins);
			}

			private class PutHandler(TestAsyncTxn.IListener listener, int expectedResult) : WriteListener
			{
				public void OnSuccess(Key key)
				{
					listener.OnSuccess();
				}

				public void OnFailure(AerospikeException e)
				{
					if (expectedResult != 0)
					{
						listener.OnFailure(e, expectedResult);
					}
					else
					{
						listener.OnFailure(e);
					}
				}
			}
		}

		public class GetExpect : IRunner
		{
			private readonly Txn txn;
			private readonly Key key;
			private readonly string expect;
			private readonly int generation;

			public GetExpect(Txn txn, Key key, string expect)
			{
				this.txn = txn;
				this.key = key;
				this.expect = expect;
				generation = 0; // Do not check generation
			}

			public GetExpect(Txn txn, Key key, string expect, int generation)
			{
				this.txn = txn;
				this.key = key;
				this.expect = expect;
				this.generation = generation;
			}

			public void Run(TestAsyncTxn parent, IListener listener)
			{
				Policy p = null;

				if (txn != null)
				{
					p = client.ReadPolicyDefault.Clone();
					p.Txn = txn;
				}
				client.Get(p, new GetExpectHandler(parent, listener, expect, generation), key);
			}

			private class GetExpectHandler(TestAsyncTxn parent, TestAsyncTxn.IListener listener, string expect, int generation) : RecordListener
			{
				public void OnSuccess(Key key, Record record)
				{
					if (generation != 0)
					{
						if (generation != record.generation)
						{
							listener.OnFailure(new AssertFailedException("Expected generation: " + generation + " but got: " + record.generation));
						}
					}

					if (expect != null)
					{
						if (parent.AssertBinEqual(key, record, binName, expect))
						{
							listener.OnSuccess();
						}
						else
						{
							listener.OnFailure();
						}
					}
					else
					{
						if (parent.AssertRecordNotFound(key, record))
						{
							listener.OnSuccess();
						}
						else
						{
							listener.OnFailure();
						}
					}
				}

				public void OnFailure(AerospikeException e)
				{
					listener.OnFailure(e);
				}
			}
		}

		public class OperateExpect(Txn txn, Key key, Bin? expect, params Operation[] ops) : IRunner
		{
			public void Run(TestAsyncTxn parent, IListener listener)
			{
				WritePolicy wp = null;

				if (txn != null)
				{
					wp = client.WritePolicyDefault.Clone();
					wp.Txn = txn;
				}
				client.Operate(wp, new OperateExpectHandler(parent, listener, expect), key, ops);
			}

			private class OperateExpectHandler(TestAsyncTxn parent, TestAsyncTxn.IListener listener, Bin? expect) : RecordListener
			{
				public void OnSuccess(Key key, Record record)
				{
					if (expect != null)
					{
						if (parent.AssertBinEqual(key, record, expect?.name, expect?.value.Object))
						{
							listener.OnSuccess();
						}
						else
						{
							listener.OnFailure();
						}
					}
					else
					{
						if (parent.AssertRecordNotFound(key, record))
						{
							listener.OnSuccess();
						}
						else
						{
							listener.OnFailure();
						}
					}
				}

				public void OnFailure(AerospikeException e)
				{
					listener.OnFailure(e);
				}
			}
		}

		public class UDF(
			Txn txn,
			Key key,
			string packageName,
			string functionName,
			params Value[] functionArgs
			) : IRunner
		{
			public void Run(TestAsyncTxn parent, IListener listener)
			{
				WritePolicy wp = null;

				if (txn != null)
				{
					wp = client.WritePolicyDefault.Clone();
					wp.Txn = txn;
				}
				client.Execute(wp, new UDFHandler(listener), key, packageName, functionName, functionArgs);
			}

			private class UDFHandler(TestAsyncTxn.IListener listener) : ExecuteListener
			{
				public void OnSuccess(Key key, Object obj)
				{
					listener.OnSuccess();
				}

				public void OnFailure(AerospikeException e)
				{
					listener.OnFailure(e);
				}
			}
		}

		public class BatchGetExpect(Txn txn, Key[] keys, int expected) : IRunner
		{
			public void Run(TestAsyncTxn parent, IListener listener)
			{
				BatchPolicy bp = null;

				if (txn != null)
				{
					bp = client.BatchPolicyDefault.Clone();
					bp.Txn = txn;
				}
				client.Get(bp, new BatchGetExpectHandler(parent, listener, expected), keys);
			}

			private class BatchGetExpectHandler(TestAsyncTxn parent, TestAsyncTxn.IListener listener, int expected) : RecordArrayListener
			{
				public void OnSuccess(Key[] keys, Record[] records)
				{
					if (parent.AssertBatchEqual(keys, records, binName, expected))
					{
						listener.OnSuccess();
					}
					else
					{
						listener.OnFailure();
					}
				}

				public void OnFailure(AerospikeException e)
				{
					listener.OnFailure(e);
				}
			}
		}

		public class BatchOperate(Txn txn, Key[] keys, params Operation[] ops) : IRunner
		{
			public void Run(TestAsyncTxn parent, IListener listener)
			{
				BatchPolicy bp = null;

				if (txn != null)
				{
					bp = client.BatchParentPolicyWriteDefault.Clone();
					bp.Txn = txn;
				}
				client.Operate(bp, null, new BatchOperateHandler(listener), keys, ops);
			}

			private class BatchOperateHandler(TestAsyncTxn.IListener listener) : BatchRecordArrayListener
			{
				public void OnSuccess(BatchRecord[] records, bool status)
				{
					if (status)
					{
						listener.OnSuccess();
					}
					else
					{
						StringBuilder sb = new();
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
						listener.OnFailure(new AerospikeException(sb.ToString()));
					}
				}

				public void OnFailure(BatchRecord[] records, AerospikeException e)
				{
					listener.OnFailure(e);
				}
			}
		}

		public class Touch(Txn txn, Key key) : IRunner
		{
			public void Run(TestAsyncTxn parent, IListener listener)
			{
				WritePolicy wp = null;

				if (txn != null)
				{
					wp = client.WritePolicyDefault.Clone();
					wp.Txn = txn;
				}
				client.Touch(wp, new TouchHandler(listener), key);
			}

			private class TouchHandler(TestAsyncTxn.IListener listener) : WriteListener
			{
				public void OnSuccess(Key key)
				{
					listener.OnSuccess();
				}

				public void OnFailure(AerospikeException e)
				{
					listener.OnFailure(e);
				}
			}
		}

		public class Delete(Txn txn, Key key) : IRunner
		{
			public void Run(TestAsyncTxn parent, IListener listener)
			{
				WritePolicy wp = null;

				if (txn != null)
				{
					wp = client.WritePolicyDefault.Clone();
					wp.Txn = txn;
					wp.durableDelete = true;
				}
				client.Delete(wp, new DeleteHandler(listener), key);
			}

			private class DeleteHandler(TestAsyncTxn.IListener listener) : DeleteListener
			{
				public void OnSuccess(Key key, bool existed)
				{
					listener.OnSuccess();
				}

				public void OnFailure(AerospikeException e)
				{
					listener.OnFailure(e);
				}
			}
		}

		public class Sleep(int sleepMillis) : IRunner
		{
			public void Run(TestAsyncTxn parent, IListener listener)
			{
				Util.Sleep(sleepMillis);
				parent.NotifyCompleted();
			}
		}

		public interface IRunner
		{
			void Run(TestAsyncTxn parent, IListener listener);
		}

		public interface IListener
		{
			void OnSuccess();
			void OnFailure();
			void OnFailure(Exception e);

			void OnFailure(Exception e, int expectedResult);
		}
	}
}
