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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestBitExp : TestSync
	{
		private string binA = "A";
		private Policy policy = new Policy();

		[TestMethod]
		public void CallRead() {
			Key key = new Key(args.ns, args.set, 5000);
			client.Delete(null, key);

			Bin bin = new Bin(binA, new byte[] {0x01, 0x42, 0x03, 0x04, 0x05});
			client.Put(null, key, bin);

			Get(key);
			Count(key);
			Lscan(key);
			Rscan(key);
			GetInt(key);
		}

		[TestMethod]
		public void CallModify() {
			Key key = new Key(args.ns, args.set, 5001);
			client.Delete(null, key);

			Bin bin = new Bin(binA, new byte[] {0x01, 0x42, 0x03, 0x04, 0x05});
			client.Put(null, key, bin);

			Resize(key);
			Insert(key);
			Remove(key);
			Set(key);
			Or(key);
			Xor(key);
			And(key);
			Not(key);
			Lshift(key);
			Rshift(key);
			Add(key);
			Subtract(key);
			SetInt(key);
		}

		private void Get(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
					Exp.Val(new byte[] {0x03})));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Count(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Count(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
					BitExp.Count(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Count(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
					BitExp.Count(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Lscan(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Lscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
					Exp.Val(5)));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Lscan(Exp.Val(0), Exp.Val(8), Exp.Val(true),
						BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))),
					Exp.Val(5)));

			r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Lscan(Exp.Val(0), Exp.Val(8), Exp.Val(true),
						BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))),
					Exp.Val(5)));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Lscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
					Exp.Val(5)));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Rscan(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Rscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
					Exp.Val(7)));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Rscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
					Exp.Val(7)));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void GetInt(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.GetInt(Exp.Val(32), Exp.Val(8), true, Exp.BlobBin(binA)),
					Exp.Val(0x05)));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.GetInt(Exp.Val(32), Exp.Val(8), true, Exp.BlobBin(binA)),
					Exp.Val(0x05)));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Resize(Key key) {
			Exp size = Exp.Val(6);

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA)),
					BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA)),
					BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Insert(Key key) {
			byte[] bytes = new byte[] {(byte)0xff};
			int expected = 0xff;

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.GetInt(Exp.Val(8), Exp.Val(8), false,
						BitExp.Insert(BitPolicy.Default, Exp.Val(1), Exp.Val(bytes), Exp.BlobBin(binA))),
					Exp.Val(expected)));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.GetInt(Exp.Val(8), Exp.Val(8), false,
						BitExp.Insert(BitPolicy.Default, Exp.Val(1), Exp.Val(bytes), Exp.BlobBin(binA))),
					Exp.Val(expected)));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Remove(Key key) {
			int expected = 0x42;

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.GetInt(Exp.Val(0), Exp.Val(8), false,
						BitExp.Remove(BitPolicy.Default, Exp.Val(0), Exp.Val(1), Exp.BlobBin(binA))),
					Exp.Val(expected)));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.GetInt(Exp.Val(0), Exp.Val(8), false,
						BitExp.Remove(BitPolicy.Default, Exp.Val(0), Exp.Val(1), Exp.BlobBin(binA))),
					Exp.Val(expected)));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Set(Key key) {
			byte[] bytes = new byte[] {(byte)0x80};

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Set(BitPolicy.Default, Exp.Val(31), Exp.Val(1), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Set(BitPolicy.Default, Exp.Val(31), Exp.Val(1), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Or(Key key) {
			byte[] bytes = new byte[] {(byte)0x01};

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Or(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Or(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Xor(Key key) {
			byte[] bytes = new byte[] {(byte)0x02};

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.Xor(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.Xor(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void And(Key key) {
			byte[] bytes = new byte[] {(byte)0x01};

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.And(BitPolicy.Default, Exp.Val(16), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(0), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.And(BitPolicy.Default, Exp.Val(16), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(0), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Not(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.Not(BitPolicy.Default, Exp.Val(6), Exp.Val(1), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.Not(BitPolicy.Default, Exp.Val(6), Exp.Val(1), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Lshift(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(0), Exp.Val(6),
						BitExp.Lshift(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(2), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(2), Exp.Val(6), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(0), Exp.Val(6),
						BitExp.Lshift(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(2), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(2), Exp.Val(6), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Rshift(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(26), Exp.Val(6),
						BitExp.Rshift(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(2), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(24), Exp.Val(6), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(26), Exp.Val(6),
						BitExp.Rshift(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(2), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(24), Exp.Val(6), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Add(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(16), Exp.Val(8),
						BitExp.Add(BitPolicy.Default, Exp.Val(16), Exp.Val(8), Exp.Val(1), false, BitOverflowAction.FAIL, Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(24), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(16), Exp.Val(8),
						BitExp.Add(BitPolicy.Default, Exp.Val(16), Exp.Val(8), Exp.Val(1), false, BitOverflowAction.FAIL, Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(24), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void Subtract(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Subtract(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(1), false, BitOverflowAction.FAIL, Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Subtract(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(1), false, BitOverflowAction.FAIL, Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}

		private void SetInt(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.SetInt(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(0x42), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(8), Exp.Val(8), Exp.BlobBin(binA))));

			Record r = client.Get(policy, key);
			Assert.AreEqual(null, r);

			policy.filterExp = Exp.Build(
				Exp.EQ(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.SetInt(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(0x42), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(8), Exp.Val(8), Exp.BlobBin(binA))));

			r = client.Get(policy, key);
			AssertRecordFound(key, r);
		}
	}
}
