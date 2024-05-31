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
		public async Task CallRead() {
			Key key = new Key(args.ns, args.set, 5000);

			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				Bin bin = new Bin(binA, new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 });
				client.Put(null, key, bin);

				await Get(key);
				await Count(key);
				await Lscan(key);
				await Rscan(key);
				await GetInt(key);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				Bin bin = new Bin(binA, new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 });
				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);

				await Get(key);
				await Count(key);
				await Lscan(key);
				await Rscan(key);
				await GetInt(key);
			}
		}

		[TestMethod]
		public async Task CallModify() {
			Key key = new Key(args.ns, args.set, 5001);
			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);

				Bin bin = new Bin(binA, new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 });
				client.Put(null, key, bin);

				await Resize(key);
				await Insert(key);
				await Remove(key);
				await Set(key);
				await Or(key);
				await Xor(key);
				await And(key);
				await Not(key);
				await Lshift(key);
				await Rshift(key);
				await Add(key);
				await Subtract(key);
				await SetInt(key);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);

				Bin bin = new Bin(binA, new byte[] { 0x01, 0x42, 0x03, 0x04, 0x05 });
				await asyncAwaitClient.Put(null, key, new[] { bin }, CancellationToken.None);

				await Resize(key);
				await Insert(key);
				await Remove(key);
				await Set(key);
				await Or(key);
				await Xor(key);
				await And(key);
				await Not(key);
				await Lshift(key);
				await Rshift(key);
				await Add(key);
				await Subtract(key);
				await SetInt(key);
			}
		}

		private async Task Get(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
						Exp.Val(new byte[] { 0x03 })));

				r = client.Get(policy, key);
				AssertRecordFound(key, r);
			}
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
						BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
						Exp.Val(new byte[] { 0x03 })));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Count(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Count(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
					BitExp.Count(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
				Record r = client.Get(policy, key);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Count(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
						BitExp.Count(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

				r = client.Get(policy, key);
				AssertRecordFound(key, r);
			}
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Count(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA)),
						BitExp.Count(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Lscan(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Lscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
					Exp.Val(5)));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.NE(
						BitExp.Lscan(Exp.Val(0), Exp.Val(8), Exp.Val(true),
							BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))),
						Exp.Val(5)));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Lscan(Exp.Val(0), Exp.Val(8), Exp.Val(true),
							BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))),
						Exp.Val(5)));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Lscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
						Exp.Val(5)));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Rscan(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Rscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
					Exp.Val(7)));

			if (!args.testAsyncAwait)
			{
				Record r = client.Get(policy, key);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Rscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
						Exp.Val(7)));

				r = client.Get(policy, key);
				AssertRecordFound(key, r);
			}
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Rscan(Exp.Val(32), Exp.Val(8), Exp.Val(true), Exp.BlobBin(binA)),
						Exp.Val(7)));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task GetInt(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.GetInt(Exp.Val(32), Exp.Val(8), true, Exp.BlobBin(binA)),
					Exp.Val(0x05)));

			if (!args.testAsyncAwait)
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.GetInt(Exp.Val(32), Exp.Val(8), true, Exp.BlobBin(binA)),
						Exp.Val(0x05)));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Resize(Key key) {
			Exp size = Exp.Val(6);

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA)),
					BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
				Record r = client.Get(policy, key);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA)),
						BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA))));

				r = client.Get(policy, key);
				AssertRecordFound(key, r);
			}
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA)),
						BitExp.Resize(BitPolicy.Default, size, 0, Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Insert(Key key) {
			byte[] bytes = new byte[] {(byte)0xff};
			int expected = 0xff;

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.GetInt(Exp.Val(8), Exp.Val(8), false,
						BitExp.Insert(BitPolicy.Default, Exp.Val(1), Exp.Val(bytes), Exp.BlobBin(binA))),
					Exp.Val(expected)));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.GetInt(Exp.Val(8), Exp.Val(8), false,
							BitExp.Insert(BitPolicy.Default, Exp.Val(1), Exp.Val(bytes), Exp.BlobBin(binA))),
						Exp.Val(expected)));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Remove(Key key) {
			int expected = 0x42;

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.GetInt(Exp.Val(0), Exp.Val(8), false,
						BitExp.Remove(BitPolicy.Default, Exp.Val(0), Exp.Val(1), Exp.BlobBin(binA))),
					Exp.Val(expected)));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.GetInt(Exp.Val(0), Exp.Val(8), false,
							BitExp.Remove(BitPolicy.Default, Exp.Val(0), Exp.Val(1), Exp.BlobBin(binA))),
						Exp.Val(expected)));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Set(Key key) {
			byte[] bytes = new byte[] {(byte)0x80};

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Set(BitPolicy.Default, Exp.Val(31), Exp.Val(1), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(24), Exp.Val(8),
							BitExp.Set(BitPolicy.Default, Exp.Val(31), Exp.Val(1), Exp.Val(bytes), Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Or(Key key) {
			byte[] bytes = new byte[] {(byte)0x01};

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Or(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));
			if (!args.testAsyncAwait)
			{

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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(24), Exp.Val(8),
							BitExp.Or(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(32), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Xor(Key key) {
			byte[] bytes = new byte[] {(byte)0x02};

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.Xor(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(0), Exp.Val(8),
							BitExp.Xor(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task And(Key key) {
			byte[] bytes = new byte[] {(byte)0x01};

			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.And(BitPolicy.Default, Exp.Val(16), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(0), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(0), Exp.Val(8),
							BitExp.And(BitPolicy.Default, Exp.Val(16), Exp.Val(8), Exp.Val(bytes), Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(0), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Not(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(0), Exp.Val(8),
						BitExp.Not(BitPolicy.Default, Exp.Val(6), Exp.Val(1), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(0), Exp.Val(8),
							BitExp.Not(BitPolicy.Default, Exp.Val(6), Exp.Val(1), Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Lshift(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(0), Exp.Val(6),
						BitExp.Lshift(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(2), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(2), Exp.Val(6), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(0), Exp.Val(6),
							BitExp.Lshift(BitPolicy.Default, Exp.Val(0), Exp.Val(8), Exp.Val(2), Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(2), Exp.Val(6), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Rshift(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(26), Exp.Val(6),
						BitExp.Rshift(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(2), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(24), Exp.Val(6), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(26), Exp.Val(6),
							BitExp.Rshift(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(2), Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(24), Exp.Val(6), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Add(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(16), Exp.Val(8),
						BitExp.Add(BitPolicy.Default, Exp.Val(16), Exp.Val(8), Exp.Val(1), false, BitOverflowAction.FAIL, Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(24), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(16), Exp.Val(8),
							BitExp.Add(BitPolicy.Default, Exp.Val(16), Exp.Val(8), Exp.Val(1), false, BitOverflowAction.FAIL, Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(24), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task Subtract(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.Subtract(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(1), false, BitOverflowAction.FAIL, Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(24), Exp.Val(8),
							BitExp.Subtract(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(1), false, BitOverflowAction.FAIL, Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(16), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}

		private async Task SetInt(Key key) {
			policy.filterExp = Exp.Build(
				Exp.NE(
					BitExp.Get(Exp.Val(24), Exp.Val(8),
						BitExp.SetInt(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(0x42), Exp.BlobBin(binA))),
					BitExp.Get(Exp.Val(8), Exp.Val(8), Exp.BlobBin(binA))));

			if (!args.testAsyncAwait)
			{
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
			else
			{
				Record r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				Assert.AreEqual(null, r);

				policy.filterExp = Exp.Build(
					Exp.EQ(
						BitExp.Get(Exp.Val(24), Exp.Val(8),
							BitExp.SetInt(BitPolicy.Default, Exp.Val(24), Exp.Val(8), Exp.Val(0x42), Exp.BlobBin(binA))),
						BitExp.Get(Exp.Val(8), Exp.Val(8), Exp.BlobBin(binA))));

				r = await asyncAwaitClient.Get(policy, key, CancellationToken.None);
				AssertRecordFound(key, r);
			}
		}
	}
}
