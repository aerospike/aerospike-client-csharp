/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestOperateBit : TestSync
	{
		private const string binName = "opbbin";

		[TestMethod]
		public async Task OperateBitResize()
		{
			Key key = new Key(args.ns, args.set, "opbkey1");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Resize(BitPolicy.Default, binName, 4, BitResizeFlags.DEFAULT),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				//Console.WriteLine(ByteUtil.BytesToHexString(b));
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x00, 0x00 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Resize(BitPolicy.Default, binName, 4, BitResizeFlags.DEFAULT),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				//Console.WriteLine(ByteUtil.BytesToHexString(b));
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x00, 0x00 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitInsert()
		{
			Key key = new Key(args.ns, args.set, "opbkey2");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Insert(BitPolicy.Default, binName, 1, new byte[] { 0xFF, 0xC7 }),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0xFF, 0xC7, 0x42, 0x03, 0x04, 0x05 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] {BitOperation.Insert(BitPolicy.Default, binName, 1, new byte[] { 0xFF, 0xC7 }),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0xFF, 0xC7, 0x42, 0x03, 0x04, 0x05 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitRemove()
		{
			Key key = new Key(args.ns, args.set, "opbkey3");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Remove(BitPolicy.Default, binName, 2, 3),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] {BitOperation.Remove(BitPolicy.Default, binName, 2, 3),
					Operation.Get(binName) },
					CancellationToken.None
					); ;

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitSet()
		{
			Key key = new Key(args.ns, args.set, "opbkey1");

			if (!args.testAsyncAwait) { 
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Set(BitPolicy.Default, binName, 13, 3, new byte[] { 0xE0 }),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				//Console.WriteLine(ByteUtil.BytesToHexString(b));
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x47, 0x03, 0x04, 0x05 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Set(BitPolicy.Default, binName, 13, 3, new byte[] { 0xE0 }),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				//Console.WriteLine(ByteUtil.BytesToHexString(b));
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x47, 0x03, 0x04, 0x05 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitOr()
		{
			Key key = new Key(args.ns, args.set, "opbkey2");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Or(BitPolicy.Default, binName, 17, 6, new byte[] { 0xA8 }),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x57, 0x04, 0x05 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Or(BitPolicy.Default, binName, 17, 6, new byte[] { 0xA8 }),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x57, 0x04, 0x05 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitXor()
		{
			Key key = new Key(args.ns, args.set, "opbkey3");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Xor(BitPolicy.Default, binName, 17, 6, new byte[] { 0xAC }),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x55, 0x04, 0x05 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Xor(BitPolicy.Default, binName, 17, 6, new byte[] { 0xAC }),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x55, 0x04, 0x05 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitAnd()
		{
			Key key = new Key(args.ns, args.set, "opbkey4");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.And(BitPolicy.Default, binName, 23, 9, new byte[] { 0x3C, 0x80 }),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x02, 0x00, 0x05 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.And(BitPolicy.Default, binName, 23, 9, new byte[] { 0x3C, 0x80 }),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x02, 0x00, 0x05 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitNot()
		{
			Key key = new Key(args.ns, args.set, "opbkey5");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Not(BitPolicy.Default, binName, 25, 6),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x03, 0x7A, 0x05 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Not(BitPolicy.Default, binName, 25, 6),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x03, 0x7A, 0x05 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitLshift()
		{
			Key key = new Key(args.ns, args.set, "opbkey6");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Lshift(BitPolicy.Default, binName, 32, 8, 3),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x03, 0x04, 0x28 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Lshift(BitPolicy.Default, binName, 32, 8, 3),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x03, 0x04, 0x28 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitRshift()
		{
			Key key = new Key(args.ns, args.set, "opbkey7");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Rshift(BitPolicy.Default, binName, 0, 9, 1),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x00, 0xC2, 0x03, 0x04, 0x05 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Rshift(BitPolicy.Default, binName, 0, 9, 1),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x00, 0xC2, 0x03, 0x04, 0x05 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitAdd()
		{
			Key key = new Key(args.ns, args.set, "opbkey10");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Add(BitPolicy.Default, binName, 24, 16, 128, false, BitOverflowAction.FAIL),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x03, 0x04, 0x85 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Add(BitPolicy.Default, binName, 24, 16, 128, false, BitOverflowAction.FAIL),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x03, 0x04, 0x85 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitSubtract()
		{
			Key key = new Key(args.ns, args.set, "opbkey11");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Subtract(BitPolicy.Default, binName, 24, 16, 128, false, BitOverflowAction.FAIL),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x03, 0x03, 0x85 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Subtract(BitPolicy.Default, binName, 24, 16, 128, false, BitOverflowAction.FAIL),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x01, 0x42, 0x03, 0x03, 0x85 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitSetInt()
		{
			Key key = new Key(args.ns, args.set, "opbkey12");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.SetInt(BitPolicy.Default, binName, 1, 8, 127),
					Operation.Get(binName)
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x3F, 0xC2, 0x03, 0x04, 0x05 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.SetInt(BitPolicy.Default, binName, 1, 8, 127),
					Operation.Get(binName) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				IList list = record.GetList(binName);

				byte[] b = (byte[])list[1];
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x3F, 0xC2, 0x03, 0x04, 0x05 }, b));
			}
		}

		[TestMethod]
		public async Task OperateBitGet()
		{
			Key key = new Key(args.ns, args.set, "opbkey13");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Get(binName, 9, 5)
					);

				AssertRecordFound(key, record);

				byte[] b = (byte[])record.GetValue(binName);
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x80 }, b));
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Get(binName, 9, 5) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				byte[] b = (byte[])record.GetValue(binName);
				Assert.IsTrue(Util.ByteArrayEquals(new byte[] { 0x80 }, b));

			}
		}

		[TestMethod]
		public async Task OperateBitCount()
		{
			Key key = new Key(args.ns, args.set, "opbkey14");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Count(binName, 20, 4)
					);

				AssertRecordFound(key, record);

				long v = (long)record.GetValue(binName);
				Assert.AreEqual(2, v);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Count(binName, 20, 4) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				long v = (long)record.GetValue(binName);
				Assert.AreEqual(2, v);
			}
		}

		[TestMethod]
		public async Task OperateBitLscan()
		{
			Key key = new Key(args.ns, args.set, "opbkey15");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Lscan(binName, 24, 8, true)
					);

				AssertRecordFound(key, record);

				long v = (long)record.GetValue(binName);
				Assert.AreEqual(5, v);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Lscan(binName, 24, 8, true) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				long v = (long)record.GetValue(binName);
				Assert.AreEqual(5, v);
			}
		}

		[TestMethod]
		public async Task OperateBitRscan()
		{
			Key key = new Key(args.ns, args.set, "opbkey16");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.Rscan(binName, 32, 8, true)
					);

				AssertRecordFound(key, record);

				long v = (long)record.GetValue(binName);
				Assert.AreEqual(7, v);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.Rscan(binName, 32, 8, true) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				long v = (long)record.GetValue(binName);
				Assert.AreEqual(7, v);
			}
		}

		[TestMethod]
		public async Task OperateBitGetInt()
		{
			Key key = new Key(args.ns, args.set, "opbkey17");

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				client.Put(null, key, new Bin(binName, bytes));

				Record record = client.Operate(null, key,
					BitOperation.GetInt(binName, 8, 16, false)
					);

				AssertRecordFound(key, record);

				long v = (long)record.GetValue(binName);
				Assert.AreEqual(16899, v);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				byte[] bytes = new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 };

				await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, bytes) }, CancellationToken.None);

				Record record = await asyncAwaitClient.Operate(null, key,
					new[] { BitOperation.GetInt(binName, 8, 16, false) },
					CancellationToken.None
					);

				AssertRecordFound(key, record);

				long v = (long)record.GetValue(binName);
				Assert.AreEqual(16899, v);
			}
		}
	}
}
