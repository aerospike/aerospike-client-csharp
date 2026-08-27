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
using Aerospike.Client;
using System.Collections;

namespace Aerospike.Test
{
	[TestClass]
	public class TestOperateBit : TestSync
	{
		private const string binName = "opbbin";

		[TestMethod]
		public void OperateBitResize()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey1");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Resize(BitPolicy.Default, binName, 4, BitResizeFlags.DEFAULT),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			//Console.WriteLine(ByteUtil.BytesToHexString(b));
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42, 0x00, 0x00], b));
		}

		[TestMethod]
		public void OperateBitInsert()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey2");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Insert(BitPolicy.Default, binName, 1, [0xFF, 0xC7]),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0xFF, 0xC7, 0x42, 0x03, 0x04, 0x05], b));
		}

		[TestMethod]
		public void OperateBitRemove()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey3");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Remove(BitPolicy.Default, binName, 2, 3),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42], b));
		}

		[TestMethod]
		public void OperateBitSet()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey1");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Set(BitPolicy.Default, binName, 13, 3, [0xE0]),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			//Console.WriteLine(ByteUtil.BytesToHexString(b));
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x47, 0x03, 0x04, 0x05], b));
		}

		[TestMethod]
		public void OperateBitOr()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey2");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Or(BitPolicy.Default, binName, 17, 6, [0xA8]),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42, 0x57, 0x04, 0x05], b));
		}

		[TestMethod]
		public void OperateBitXor()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey3");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Xor(BitPolicy.Default, binName, 17, 6, [0xAC]),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42, 0x55, 0x04, 0x05], b));
		}

		[TestMethod]
		public void OperateBitAnd()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey4");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.And(BitPolicy.Default, binName, 23, 9, [0x3C, 0x80]),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42, 0x02, 0x00, 0x05], b));
		}

		[TestMethod]
		public void OperateBitNot()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey5");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Not(BitPolicy.Default, binName, 25, 6),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42, 0x03, 0x7A, 0x05], b));
		}

		[TestMethod]
		public void OperateBitLshift()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey6");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Lshift(BitPolicy.Default, binName, 32, 8, 3),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42, 0x03, 0x04, 0x28], b));
		}

		[TestMethod]
		public void OperateBitRshift()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey7");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Rshift(BitPolicy.Default, binName, 0, 9, 1),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x00, 0xC2, 0x03, 0x04, 0x05], b));
		}

		[TestMethod]
		public void OperateBitAdd()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey10");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Add(BitPolicy.Default, binName, 24, 16, 128, false, BitOverflowAction.FAIL),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42, 0x03, 0x04, 0x85], b));
		}

		[TestMethod]
		public void OperateBitSubtract()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey11");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Subtract(BitPolicy.Default, binName, 24, 16, 128, false, BitOverflowAction.FAIL),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x01, 0x42, 0x03, 0x03, 0x85], b));
		}

		[TestMethod]
		public void OperateBitSetInt()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey12");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.SetInt(BitPolicy.Default, binName, 1, 8, 127),
				Operation.Get(binName)
				);

			AssertRecordFound(key, record);

			IList list = record.GetList(binName);

			byte[] b = (byte[])list[1];
			Assert.IsTrue(Util.ByteArrayEquals([0x3F, 0xC2, 0x03, 0x04, 0x05], b));
		}

		[TestMethod]
		public void OperateBitGet()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey13");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.Get(binName, 9, 5)
				);

			AssertRecordFound(key, record);

			byte[] b = (byte[])record.GetValue(binName);
			Assert.IsTrue(Util.ByteArrayEquals([0x80], b));
		}

		[TestMethod]
		public void OperateBitCount()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey14");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey15");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey16");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

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
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkey17");

			client.Delete(null, key);

			byte[] bytes = [0x01, 0x42, 0x03, 0x04, 0x05];

			client.Put(null, key, new Bin(binName, bytes));

			Record record = client.Operate(null, key,
				BitOperation.GetInt(binName, 8, 16, false)
				);

			AssertRecordFound(key, record);

			long v = (long)record.GetValue(binName);
			Assert.AreEqual(16899, v);
		}

		[TestMethod]
		public void OperateBitB64Encode()
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "bit b64Encode");

			byte[] initial = [0x01, 0x42, 0x03];
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkeyb64");

			client.Delete(null, key);
			client.Put(null, key, new Bin(binName, initial));

			// The span is in bytes, not bits, unlike every other bit read op. With
			// invertSize the size counts back from the end, so 0 means "to the end".
			Record record = client.Operate(null, key,
				BitOperation.B64Encode(binName),
				BitOperation.B64Encode(binName, 0, 2),
				BitOperation.B64Encode(binName, 1, 0, true),
				BitOperation.B64Encode(binName, -1, 1));

			AssertRecordFound(key, record);

			IList results = record.GetList(binName);
			Assert.AreEqual(Convert.ToBase64String(initial), results[0]);
			Assert.AreEqual(Convert.ToBase64String([0x01, 0x42]), results[1]);
			Assert.AreEqual(Convert.ToBase64String([0x42, 0x03]), results[2]);
			Assert.AreEqual(Convert.ToBase64String([0x03]), results[3]);
		}

		[TestMethod]
		public void OperateBitB64EncodeRoundTripsThroughB64Decode()
		{
			CheckServerVersion(Node.SERVER_VERSION_8_1_3, "bit b64Encode");

			byte[] initial = [0xDE, 0xAD, 0xBE, 0xEF];
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "opbkeyb64rt");

			client.Delete(null, key);
			client.Put(null, key, new Bin(binName, initial));

			// b64Encode is the inverse of StringOperation.B64Decode: encode the blob here,
			// write the text to a string bin, then decode it back and compare.
			Record encoded = client.Operate(null, key, BitOperation.B64Encode(binName));
			string text = encoded.GetString(binName);

			client.Put(null, key, new Bin("b64txt", text));
			Record decoded = client.Operate(null, key, StringOperation.B64Decode("b64txt"));
			CollectionAssert.AreEqual(initial, (byte[])decoded.GetValue("b64txt"));
		}
	}
}
