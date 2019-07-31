/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
		public void OperateBitResize()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey1");

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

		[TestMethod]
		public void OperateBitInsert()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey2");

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

		[TestMethod]
		public void OperateBitRemove()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey3");

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

		[TestMethod]
		public void OperateBitSet()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey1");

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

		[TestMethod]
		public void OperateBitOr()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey2");

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

		[TestMethod]
		public void OperateBitXor()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey3");

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

		[TestMethod]
		public void OperateBitAnd()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey4");

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

		[TestMethod]
		public void OperateBitNot()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey5");

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

		[TestMethod]
		public void OperateBitLshift()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey6");

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

		[TestMethod]
		public void OperateBitRshift()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey7");

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

		[TestMethod]
		public void OperateBitAdd()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey10");

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

		[TestMethod]
		public void OperateBitSubtract()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey11");

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

		[TestMethod]
		public void OperateBitSetInt()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey12");

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

		[TestMethod]
		public void OperateBitGet()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey13");

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

		[TestMethod]
		public void OperateBitCount()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey14");

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

		[TestMethod]
		public void OperateBitLscan()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey15");

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

		[TestMethod]
		public void OperateBitRscan()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey16");

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

		[TestMethod]
		public void OperateBitGetInt()
		{
			if (!args.HasBit)
			{
				return;
			}

			Key key = new Key(args.ns, args.set, "opbkey17");

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
	}
}
