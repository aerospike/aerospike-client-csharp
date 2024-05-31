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
	public class TestOperateHLL : TestSync
	{
		private const string binName = "ophbin";
		private static readonly Key key = new Key(args.ns, args.set, "ophkey");
		private static readonly Key[] keys = new Key[] {
				new Key(args.ns, args.set, "ophkey0"),
				new Key(args.ns, args.set, "ophkey1"),
				new Key(args.ns, args.set, "ophkey2")};
		private const int n_entries = 1 << 18;

		private const int minIndexBits = 4;
		private const int maxIndexBits = 16;
		private const int minMinhashBits = 4;
		private const int maxMinhashBits = 51;

		private static readonly List<Value> entries = new List<Value>();
		private static readonly List<int> legalIndexBits = new List<int>();
		private static readonly List<List<int>> legalDescriptions = new List<List<int>>();
		private static readonly List<List<int>> illegalDescriptions = new List<List<int>>();

		[ClassInitialize()]
		public static void CreateData(TestContext testContext)
		{
			for (int i = 0; i < n_entries; i++)
			{
				entries.Add(new Value.StringValue("key " + i));
			}

			for (int index_bits = minIndexBits; index_bits <= maxIndexBits; index_bits += 4)
			{
				int combined_bits = maxMinhashBits + index_bits;
				int max_allowed_minhash_bits = maxMinhashBits;

				if (combined_bits > 64)
				{
					max_allowed_minhash_bits -= combined_bits - 64;
				}

				int mid_minhash_bits = (max_allowed_minhash_bits + index_bits) / 2;
				List<int> legal_zero = new List<int>();
				List<int> legal_min = new List<int>();
				List<int> legal_mid = new List<int>();
				List<int> legal_max = new List<int>();

				legalIndexBits.Add(index_bits);
				legal_zero.Add(index_bits);
				legal_min.Add(index_bits);
				legal_mid.Add(index_bits);
				legal_max.Add(index_bits);

				legal_zero.Add(0);
				legal_min.Add(minMinhashBits);
				legal_mid.Add(mid_minhash_bits);
				legal_max.Add(max_allowed_minhash_bits);

				legalDescriptions.Add(legal_zero);
				legalDescriptions.Add(legal_min);
				legalDescriptions.Add(legal_mid);
				legalDescriptions.Add(legal_max);
			}

			for (int index_bits = minIndexBits - 1; index_bits <= maxIndexBits + 5; index_bits += 4)
			{
				if (index_bits < minIndexBits || index_bits > maxIndexBits)
				{
					List<int> illegal_zero = new List<int>();
					List<int> illegal_min = new List<int>();
					List<int> illegal_max = new List<int>();

					illegal_zero.Add(index_bits);
					illegal_min.Add(index_bits);
					illegal_max.Add(index_bits);

					illegal_zero.Add(0);
					illegal_min.Add(minMinhashBits - 1);
					illegal_max.Add(maxMinhashBits);

					illegalDescriptions.Add(illegal_zero);
					illegalDescriptions.Add(illegal_min);
					illegalDescriptions.Add(illegal_max);
				}
				else
				{
					List<int> illegal_min = new List<int>();
					List<int> illegal_max = new List<int>();
					List<int> illegal_max1 = new List<int>();

					illegal_min.Add(index_bits);
					illegal_max.Add(index_bits);

					illegal_min.Add(minMinhashBits - 1);
					illegal_max.Add(maxMinhashBits + 1);

					illegalDescriptions.Add(illegal_min);
					illegalDescriptions.Add(illegal_max);

					if (index_bits + maxMinhashBits > 64)
					{
						illegal_max1.Add(index_bits);
						illegal_max1.Add(1 + maxMinhashBits - (64 - (index_bits + maxMinhashBits)));
						illegalDescriptions.Add(illegal_max1);
					}
				}
			}
		}

		public async Task AssertThrows(string msg, Key key, Type eclass, int eresult, params Operation[] ops)
		{
			try
			{
				if (!args.testAsyncAwait)
				{
					client.Operate(null, key, ops);
				}
				else
				{
					await asyncAwaitClient.Operate(null, key, ops, CancellationToken.None);
				}
				Assert.IsTrue(false, msg + " succeeded?");
			}
			catch (AerospikeException e)
			{
				if (eclass != e.GetType() || eresult != e.Result)
				{
					Assert.AreEqual(eclass, e.GetType(), msg + " " + e.GetType() + " " + e);
					Assert.AreEqual(eresult, e.Result, msg + " " + e.Result + " " + e);
				}
			}
		}

		public async Task<Record> AssertSuccess(string msg, Key key, params Operation[] ops)
		{
			Record record;

			try
			{
				if (!args.testAsyncAwait)
				{
					record = client.Operate(null, key, ops);
				}
				else
				{
					record = await asyncAwaitClient.Operate(null, key, ops, CancellationToken.None);
				}
			}
			catch (Exception e)
			{
				Assert.AreEqual(null, e, msg + " " + e);
				return null;
			}
			AssertRecordFound(key, record);
			return record;
		}

		public bool CheckBits(int index_bits, int minhash_bits)
		{
			return !(index_bits < minIndexBits || index_bits > maxIndexBits ||
					(minhash_bits != 0 && minhash_bits < minMinhashBits) ||
					minhash_bits > maxMinhashBits || index_bits + minhash_bits > 64);
		}

		public double RelativeCountError(int n_index_bits)
		{
			return 1.04 / Math.Sqrt(Math.Pow(2, n_index_bits));
		}

		public bool IsWithinRelativeError(long expected, long estimate, double relative_error)
		{
			return expected * (1 - relative_error) <= estimate || estimate <= expected * (1 + relative_error);
		}

		public void AssertHLLCount(String msg, int index_bits, long hll_count, long expected)
		{
			double countErr6sigma = RelativeCountError(index_bits) * 6;

			Assert.IsTrue(countErr6sigma > Math.Abs(1 - (hll_count / (double)expected)), 
				msg + " - err " + countErr6sigma + " count " + hll_count + " expected " + expected + " index_bits " + index_bits
				);
		}

		public void AssertDescription(string msg, IList description, int index_bits, int minhash_bits)
		{
			Assert.AreEqual(index_bits, (long)description[0], msg);
			Assert.AreEqual(minhash_bits, (long)description[1], msg);
		}

		public async Task AssertInit(int index_bits, int minhash_bits, bool should_pass)
		{
			string msg = "Fail - index_bits " + index_bits + " minhash_bits " + minhash_bits;
			HLLPolicy p = HLLPolicy.Default;
			Operation[] ops = new Operation[] {
					HLLOperation.Init(p, binName, index_bits, minhash_bits),
					HLLOperation.GetCount(binName),
					HLLOperation.RefreshCount(binName),
					HLLOperation.Describe(binName)};

			if (! should_pass)
			{
				await AssertThrows(msg, key, typeof(AerospikeException), ResultCode.PARAMETER_ERROR, ops);
				return;
			}

			Record record = await AssertSuccess(msg, key, ops);
			IList result_list = record.GetList(binName);
			long count = (long)result_list[1];
			long count1 = (long)result_list[2];
			IList description = (IList)result_list[3];

			AssertDescription(msg, description, index_bits, minhash_bits);
			Assert.AreEqual(0, count);
			Assert.AreEqual(0, count1);
		}

		[TestMethod]
		public async Task OperateHLLInit()
		{
			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);
			}

			foreach (List<int> desc in legalDescriptions)
			{
				await AssertInit(desc[0], desc[1], true);
			}

			foreach (List<int> desc in illegalDescriptions)
			{
				await AssertInit(desc[0], desc[1], false);
			}
		}

		[TestMethod]
		public async Task OperateHLLFlags()
		{
			int index_bits = 4;

			// Keep record around win binName is removed.
			await AssertSuccess("other bin", key,
				Operation.Delete(),
				HLLOperation.Init(HLLPolicy.Default, binName + "other", index_bits));

			// create_only
			HLLPolicy c = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);

			await AssertSuccess("create_only", key, HLLOperation.Init(c, binName, index_bits));
			await AssertThrows("create_only - error", key, typeof(AerospikeException), ResultCode.BIN_EXISTS_ERROR,
				HLLOperation.Init(c, binName, index_bits));

			// update_only
			HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

			await AssertSuccess("update_only", key, HLLOperation.Init(u, binName, index_bits));
			await AssertSuccess("remove bin", key, Operation.Put(Bin.AsNull(binName)));
			await AssertThrows("update_only - error", key, typeof(AerospikeException), ResultCode.BIN_NOT_FOUND,
				HLLOperation.Init(u, binName, index_bits));

			// create_only no_fail
			HLLPolicy cn = new HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL);

			await AssertSuccess("create_only nofail", key, HLLOperation.Init(cn, binName, index_bits));
			await AssertSuccess("create_only nofail - no error", key, HLLOperation.Init(cn, binName, index_bits));

			// update_only no_fail
			HLLPolicy un = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY | HLLWriteFlags.NO_FAIL);

			await AssertSuccess("update_only nofail", key, HLLOperation.Init(un, binName, index_bits));
			await AssertSuccess("remove bin", key, Operation.Put(Bin.AsNull(binName)));
			await AssertSuccess("update_only nofail - no error", key, HLLOperation.Init(un, binName, index_bits));

			// fold
			await AssertSuccess("create_only", key, HLLOperation.Init(c, binName, index_bits));

			HLLPolicy f = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);

			await AssertThrows("fold", key, typeof(AerospikeException), ResultCode.PARAMETER_ERROR,
				HLLOperation.Init(f, binName, index_bits));
		}

		public async Task AssertAddInit(int index_bits, int minhash_bits)
		{
			if (!args.testAsyncAwait)
			{
				client.Delete(null, key);
			}
			else
			{
				await asyncAwaitClient.Delete(null, key, CancellationToken.None);
			}

			string msg = "Fail - index_bits " + index_bits + " minhash_bits " + minhash_bits;
			HLLPolicy p = HLLPolicy.Default;
			Operation[] ops = new Operation[] {
				HLLOperation.Add(p, binName, entries, index_bits, minhash_bits),
				HLLOperation.GetCount(binName),
				HLLOperation.RefreshCount(binName),
				HLLOperation.Describe(binName),
				HLLOperation.Add(p, binName, entries)};

			if (!CheckBits(index_bits, minhash_bits))
			{
				await AssertThrows(msg, key, typeof(AerospikeException), ResultCode.PARAMETER_ERROR, ops);
				return;
			}

			Record record = await AssertSuccess(msg, key, ops);
			IList result_list = record.GetList(binName);
			long count = (long) result_list[1];
			long count1 = (long) result_list[2];
			IList description = (IList) result_list[3];
			long n_added = (long) result_list[4];

			AssertDescription(msg, description, index_bits, minhash_bits);
			AssertHLLCount(msg, index_bits, count, entries.Count);
			Assert.AreEqual(count, count1);
			Assert.AreEqual(n_added, 0);
		}

		[TestMethod]
		public async Task OperateHLLAddInit()
		{
			foreach (List<int> desc in legalDescriptions)
			{
				await AssertAddInit(desc[0], desc[1]);
			}
		}

		[TestMethod]
		public async Task OperateAddFlags()
		{
			int index_bits = 4;

			// Keep record around win binName is removed.
			await AssertSuccess("other bin", key, Operation.Delete(), HLLOperation.Init(HLLPolicy.Default, binName + "other", index_bits));

			// create_only
			HLLPolicy c = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);

			await AssertSuccess("create_only", key, HLLOperation.Add(c, binName, entries, index_bits));
			await AssertThrows("create_only - error", key, typeof(AerospikeException), ResultCode.BIN_EXISTS_ERROR,
				HLLOperation.Add(c, binName, entries, index_bits));

			// update_only
			HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

			await AssertThrows("update_only - error", key, typeof(AerospikeException), ResultCode.PARAMETER_ERROR,
				HLLOperation.Add(u, binName, entries, index_bits));

			// create_only no_fail
			HLLPolicy cn = new HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL);

			await AssertSuccess("create_only nofail", key, HLLOperation.Add(cn, binName, entries, index_bits));
			await AssertSuccess("create_only nofail - no error", key, HLLOperation.Add(cn, binName, entries, index_bits));

			// fold
			await AssertSuccess("init", key, HLLOperation.Init(HLLPolicy.Default, binName, index_bits));

			HLLPolicy f = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);

			await AssertThrows("fold", key, typeof(AerospikeException), ResultCode.PARAMETER_ERROR,
				HLLOperation.Add(f, binName, entries, index_bits));
		}

		public async Task AssertFold(IList vals0, IList vals1, int index_bits)
		{
			string msg = "Fail - index_bits " + index_bits;
			HLLPolicy p = HLLPolicy.Default;

			for (int ix = minIndexBits; ix <= index_bits; ix++)
			{
				if (!CheckBits(index_bits, 0) || !CheckBits(ix, 0))
				{
					Assert.IsTrue(false, "Expected valid inputs: " + msg);
				}

				Record recorda = await AssertSuccess(msg, key, 
					Operation.Delete(), 
					HLLOperation.Add(p, binName, vals0, index_bits), 
					HLLOperation.GetCount(binName), 
					HLLOperation.RefreshCount(binName),
					HLLOperation.Describe(binName));

				IList resulta_list = recorda.GetList(binName);
				long counta = (long) resulta_list[1];
				long counta1 = (long) resulta_list[2];
				IList descriptiona = (IList) resulta_list[3];

				AssertDescription(msg, descriptiona, index_bits, 0);
				AssertHLLCount(msg, index_bits, counta, vals0.Count);
				Assert.AreEqual(counta, counta1);

				Record recordb = await AssertSuccess(msg, key,
					HLLOperation.Fold(binName, ix),
					HLLOperation.GetCount(binName),
					HLLOperation.Add(p, binName, vals0),
					HLLOperation.Add(p, binName, vals1),
					HLLOperation.GetCount(binName),
					HLLOperation.Describe(binName)
					);

				IList resultb_list = recordb.GetList(binName);
				long countb = (long) resultb_list[1];
				long n_added0 = (long) resultb_list[2];
				long countb1 = (long) resultb_list[4];
				IList descriptionb = (IList) resultb_list[5];

				Assert.AreEqual(0, n_added0);
				AssertDescription(msg, descriptionb, ix, 0);
				AssertHLLCount(msg, ix, countb, vals0.Count);
				AssertHLLCount(msg, ix, countb1, vals0.Count + vals1.Count);
			}
		}

		[TestMethod]
		public async Task OperateFold()
		{
			IList vals0 = new List<Value>();
			IList vals1 = new List<Value>();

			for (int i = 0; i < n_entries / 2; i++)
			{
				vals0.Add(new Value.StringValue("key " + i));
			}

			for (int i = n_entries / 2; i < n_entries; i++)
			{
				vals1.Add(new Value.StringValue("key " + i));
			}

			for (int index_bits = 4; index_bits < maxIndexBits; index_bits++)
			{
				await AssertFold(vals0, vals1, index_bits);
			}
		}

		[TestMethod]
		public async Task OperateFoldExists()
		{
			int index_bits = 10;
			int fold_down = 4;
			int fold_up = 16;

			// Keep record around win binName is removed.
			await AssertSuccess("other bin", key, Operation.Delete(), HLLOperation.Init(HLLPolicy.Default, binName + "other", index_bits), HLLOperation.Init(HLLPolicy.Default, binName, index_bits));

			// Exists.
			await AssertSuccess("exists fold down", key, HLLOperation.Fold(binName, fold_down));
			await AssertThrows("exists fold up", key, typeof(AerospikeException), ResultCode.OP_NOT_APPLICABLE, HLLOperation.Fold(binName, fold_up));

			// Does not exist.
			await AssertSuccess("remove bin", key, Operation.Put(Bin.AsNull(binName)));

			await AssertThrows("create_only - error", key, typeof(AerospikeException), ResultCode.BIN_NOT_FOUND, HLLOperation.Fold(binName, fold_down));
		}

		public async Task AssertSetUnion(IList<IList> vals, int index_bits, bool folding, bool allow_folding)
		{
			string msg = "Fail - index_bits " + index_bits;
			HLLPolicy p = HLLPolicy.Default;
			HLLPolicy u = HLLPolicy.Default;

			if (allow_folding)
			{
				u = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);
			}

			long union_expected = 0;
			bool folded = false;

			for (int i = 0; i < keys.Length; i++)
			{
				int ix = index_bits;

				if (folding)
				{
					ix -= i;

					if (ix < minIndexBits)
					{
						ix = minIndexBits;
					}

					if (ix < index_bits)
					{
						folded = true;
					}
				}

				IList sub_vals = vals[i];

				union_expected += sub_vals.Count;

				Record record = await AssertSuccess(msg, keys[i], Operation.Delete(),
				HLLOperation.Add(p, binName, sub_vals, ix), HLLOperation.GetCount(binName));
				IList result_list = record.GetList(binName);
				long count = (long) result_list[1];

				AssertHLLCount(msg, ix, count, sub_vals.Count);
			}

			List<Value.HLLValue> hlls = new List<Value.HLLValue>();

			for (int i = 0; i < keys.Length; i++)
			{
				Record record = await AssertSuccess(msg, keys[i], Operation.Get(binName), HLLOperation.GetCount(binName));
				IList result_list = record.GetList(binName);
				Value.HLLValue hll = (Value.HLLValue)result_list[0];

				Assert.AreNotEqual(null, hll);
				hlls.Add(hll);
			}

			Operation[] ops = new Operation[] {
				Operation.Delete(),
				HLLOperation.Init(p, binName, index_bits),
				HLLOperation.SetUnion(u, binName, hlls),
				HLLOperation.GetCount(binName),
				Operation.Delete(),
				HLLOperation.SetUnion(p, binName, hlls),
				HLLOperation.GetCount(binName)
			};

			if (folded && !allow_folding)
			{
				await AssertThrows(msg, key, typeof(AerospikeException), ResultCode.OP_NOT_APPLICABLE, ops);
				return;
			}

			Record record_union = await AssertSuccess(msg, key, ops);
			IList union_result_list = record_union.GetList(binName);
			long union_count = (long) union_result_list[2];
			long union_count2 = (long) union_result_list[4];

			AssertHLLCount(msg, index_bits, union_count, union_expected);
			Assert.AreEqual(union_count, union_count2, msg);

			for (int i = 0; i < keys.Length; i++)
			{
				IList sub_vals = vals[i];
				Record record = await AssertSuccess(msg, key,
					HLLOperation.Add(p, binName, sub_vals, index_bits),
					HLLOperation.GetCount(binName));

				IList result_list = record.GetList(binName);
				long n_added = (long) result_list[0];
				long count = (long) result_list[1];

				Assert.AreEqual(0, n_added, msg);
				Assert.AreEqual(union_count, count, msg);
				AssertHLLCount(msg, index_bits, count, union_expected);
			}
		}

		[TestMethod]
		public async Task OperateSetUnion()
		{
			List<IList> vals = new List<IList>();

			for (int i = 0; i < keys.Length; i++)
			{
				List<Value> sub_vals = new List<Value>();

				for (int j = 0; j < n_entries / 3; j++)
				{
					sub_vals.Add(new Value.StringValue("key" + i + " " + j));
				}

				vals.Add(sub_vals);
			}

			for (int index_bits = 4; index_bits <= maxIndexBits; index_bits++)
			{
				await AssertSetUnion(vals, index_bits, false, false);
				await AssertSetUnion(vals, index_bits, false, true);
				await AssertSetUnion(vals, index_bits, true, false);
				await AssertSetUnion(vals, index_bits, true, true);
			}
		}

		[TestMethod]
		public async Task OperateSetUnionFlags()
		{
			int index_bits = 6;
			int low_n_bits = 4;
			int high_n_bits = 8;
			string otherName = binName + "o";

			// Keep record around win binName is removed.
			List<Value.HLLValue> hlls = new List<Value.HLLValue>();
			Record record = await AssertSuccess("other bin", key,
				Operation.Delete(),
				HLLOperation.Add(HLLPolicy.Default, otherName, entries, index_bits),
				Operation.Get(otherName)
				);
			IList result_list = record.GetList(otherName);
			Value.HLLValue hll = (Value.HLLValue) result_list[1];

			hlls.Add(hll);

			// create_only
			HLLPolicy c = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);

			await AssertSuccess("create_only", key, HLLOperation.SetUnion(c, binName, hlls));
			await AssertThrows("create_only - error", key, typeof(AerospikeException), ResultCode.BIN_EXISTS_ERROR, HLLOperation.SetUnion(c, binName, hlls));

			// update_only
			HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

			await AssertSuccess("update_only", key, HLLOperation.SetUnion(u, binName, hlls));
			await AssertSuccess("remove bin", key, Operation.Put(Bin.AsNull(binName)));
			await AssertThrows("update_only - error", key, typeof(AerospikeException), ResultCode.BIN_NOT_FOUND, HLLOperation.SetUnion(u, binName, hlls));

			// create_only no_fail
			HLLPolicy cn = new HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL);

			await AssertSuccess("create_only nofail", key, HLLOperation.SetUnion(cn, binName, hlls));
			await AssertSuccess("create_only nofail - no error", key, HLLOperation.SetUnion(cn, binName, hlls));

			// update_only no_fail
			HLLPolicy un = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY | HLLWriteFlags.NO_FAIL);

			await AssertSuccess("update_only nofail", key, HLLOperation.SetUnion(un, binName, hlls));
			await AssertSuccess("remove bin", key, Operation.Put(Bin.AsNull(binName)));
			await AssertSuccess("update_only nofail - no error", key, HLLOperation.SetUnion(un, binName, hlls));

			// fold
			HLLPolicy f = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);

			// fold down
			await AssertSuccess("size up", key, HLLOperation.Init(HLLPolicy.Default, binName, high_n_bits));
			await AssertSuccess("fold down to index_bits", key, HLLOperation.SetUnion(f, binName, hlls));

			// fold up
			await AssertSuccess("size down", key, HLLOperation.Init(HLLPolicy.Default, binName, low_n_bits));
			await AssertSuccess("fold down to low_n_bits", key, HLLOperation.SetUnion(f, binName, hlls));
		}

		[TestMethod]
		public async Task OperateRefreshCount()
		{
			int index_bits = 6;

			// Keep record around win binName is removed.
			await AssertSuccess("other bin", key, Operation.Delete(), HLLOperation.Init(HLLPolicy.Default, binName + "other", index_bits), HLLOperation.Init(HLLPolicy.Default, binName, index_bits));

			// Exists.
			await AssertSuccess("refresh zero count", key, HLLOperation.RefreshCount(binName), HLLOperation.RefreshCount(binName));
			await AssertSuccess("add items", key, HLLOperation.Add(HLLPolicy.Default, binName, entries));
			await AssertSuccess("refresh count", key, HLLOperation.RefreshCount(binName), HLLOperation.RefreshCount(binName));

			// Does not exist.
			await AssertSuccess("remove bin", key, Operation.Put(Bin.AsNull(binName)));
			await AssertThrows("refresh nonexistant count", key, typeof(AerospikeException), ResultCode.BIN_NOT_FOUND, HLLOperation.RefreshCount(binName));
		}

		[TestMethod]
		public async Task OperateGetCount()
		{
			int index_bits = 6;

			// Keep record around win binName is removed.
			await AssertSuccess("other bin", key, Operation.Delete(), HLLOperation.Init(HLLPolicy.Default, binName + "other", index_bits),
				HLLOperation.Add(HLLPolicy.Default, binName, entries, index_bits));

			// Exists.
			Record record = await AssertSuccess("exists count", key, HLLOperation.GetCount(binName));
			long count = record.GetLong(binName);
			AssertHLLCount("check count", index_bits, count, entries.Count);

			// Does not exist.
			await AssertSuccess("remove bin", key, Operation.Put(Bin.AsNull(binName)));
			record = await AssertSuccess("exists count", key, HLLOperation.GetCount(binName));
			Assert.AreEqual(null, record.GetValue(binName));
		}

		[TestMethod]
		public async Task OperateGetUnion()
		{
			int index_bits = 14;
			long expected_union_count = 0;
			List<IList> vals = new List<IList>();
			IList<Value.HLLValue> hlls = new List<Value.HLLValue>();

			for (int i = 0; i < keys.Length; i++)
			{
				List<Value> sub_vals = new List<Value>();

				for (int j = 0; j < n_entries / 3; j++)
				{
					sub_vals.Add(new Value.StringValue("key" + i + " " + j));
				}

				Record record = await AssertSuccess("init other keys", keys[i], Operation.Delete(), HLLOperation.Add(HLLPolicy.Default, binName, sub_vals, index_bits), Operation.Get(binName));

				IList result_list = record.GetList(binName);
				hlls.Add((Value.HLLValue)result_list[1]);
				expected_union_count += sub_vals.Count;
				vals.Add(sub_vals);
			}

			// Keep record around win binName is removed.
			await AssertSuccess("other bin", key, 
				Operation.Delete(),
				HLLOperation.Init(HLLPolicy.Default, binName + "other", index_bits), 
				HLLOperation.Add(HLLPolicy.Default, binName, vals[0], index_bits)
				);

			Record r = await AssertSuccess("union and unionCount", key, HLLOperation.GetUnion(binName, hlls), HLLOperation.GetUnionCount(binName, hlls));
			IList rlist = r.GetList(binName);
			long union_count = (long)rlist[1];

			AssertHLLCount("verify union count", index_bits, union_count, expected_union_count);

			Value.HLLValue union_hll = (Value.HLLValue)rlist[0];

			r = await AssertSuccess("", key, Operation.Put(new Bin(binName, union_hll)), HLLOperation.GetCount(binName));
			rlist = r.GetList(binName);
			long union_count_2 = (long)rlist[1];

			Assert.AreEqual(union_count, union_count_2, "unions equal");
		}

		[TestMethod]
		public async Task GetPut()
		{
			foreach (List<int> desc in legalDescriptions)
			{
				int index_bits = desc[0];
				int minhash_bits = desc[1];

				await AssertSuccess("init record", key, Operation.Delete(), HLLOperation.Init(HLLPolicy.Default, binName, index_bits, minhash_bits));

				if (!args.testAsyncAwait)
				{
					Record record = client.Get(null, key);
					Value.HLLValue hll = record.GetHLLValue(binName);

					client.Delete(null, key);
					client.Put(null, key, new Bin(binName, hll));

					record = await AssertSuccess("describe", key, HLLOperation.GetCount(binName), HLLOperation.Describe(binName));

					IList result_list = record.GetList(binName);
					long count = (long)result_list[0];
					IList description = (IList)result_list[1];

					Assert.AreEqual(0, count);
					AssertDescription("Check description", description, index_bits, minhash_bits);
				}
				else
				{
					Record record = await asyncAwaitClient.Get(null, key, CancellationToken.None);
					Value.HLLValue hll = record.GetHLLValue(binName);

					await asyncAwaitClient.Delete(null, key, CancellationToken.None);
					await asyncAwaitClient.Put(null, key, new[] { new Bin(binName, hll) }, CancellationToken.None);

					record = await AssertSuccess("describe", key, HLLOperation.GetCount(binName), HLLOperation.Describe(binName));

					IList result_list = record.GetList(binName);
					long count = (long)result_list[0];
					IList description = (IList)result_list[1];

					Assert.AreEqual(0, count);
					AssertDescription("Check description", description, index_bits, minhash_bits);
				}
			}
		}

		public double AbsoluteSimilarityError(int index_bits, int minhash_bits, double expected_similarity)
		{
			double min_err_index = 1 / Math.Sqrt(1 << index_bits);
			double min_err_minhash = 6 * Math.Pow(Math.E, minhash_bits * -1) / expected_similarity;
			return Math.Max(min_err_index, min_err_minhash);
		}

		public void AssertHMHSimilarity(string msg, int index_bits, int minhash_bits, double similarity, double expected_similarity, long intersect_count, long expected_intersect_count)
		{
			double sim_err_6sigma = 0;

			if (minhash_bits != 0)
			{
				sim_err_6sigma = 6 * AbsoluteSimilarityError(index_bits, minhash_bits, expected_similarity);
			}

			msg = msg + " - err " + sim_err_6sigma + " index_bits " + index_bits + " minhash_bits " + minhash_bits + "\n\t- similarity " + similarity + " expected_similarity " + expected_similarity + "\n\t- intersect_count " + intersect_count + " expected_intersect_count " + expected_intersect_count;

			if (minhash_bits == 0)
			{
				return;
			}

			Assert.IsTrue(sim_err_6sigma > Math.Abs(expected_similarity - similarity), msg);
			Assert.IsTrue(IsWithinRelativeError(expected_intersect_count, intersect_count, sim_err_6sigma), msg);
		}

		public async Task AssertSimilarityOp(double overlap, IList common, IList<IList> vals, int index_bits, int minhash_bits)
		{
			IList<Value.HLLValue> hlls = new List<Value.HLLValue>();

			for (int i = 0; i < keys.Length; i++)
			{
				Record record = await AssertSuccess("init other keys", keys[i],
					Operation.Delete(),
					HLLOperation.Add(HLLPolicy.Default, binName, vals[i], index_bits, minhash_bits),
					HLLOperation.Add(HLLPolicy.Default, binName, common, index_bits, minhash_bits),
					Operation.Get(binName)
					);

				IList result_list = record.GetList(binName);
				hlls.Add((Value.HLLValue)result_list[2]);
			}

			// Keep record around win binName is removed.
			Record r = await AssertSuccess("other bin", key,
				Operation.Delete(),
				HLLOperation.Init(HLLPolicy.Default, binName + "other", index_bits, minhash_bits),
				HLLOperation.SetUnion(HLLPolicy.Default, binName, hlls),
				HLLOperation.Describe(binName)
				);

			IList rlist = r.GetList(binName);
			IList description = (IList)rlist[1];

			AssertDescription("check desc", description, index_bits, minhash_bits);

			r = await AssertSuccess("similarity and intersect_count", key,
				HLLOperation.GetSimilarity(binName, hlls),
				HLLOperation.GetIntersectCount(binName, hlls)
				);

			rlist = r.GetList(binName);
			double sim = (double)rlist[0];
			long intersect_count = (long)rlist[1];
			double expected_similarity = overlap;
			long expected_intersect_count = common.Count;

			AssertHMHSimilarity("check sim", index_bits, minhash_bits, sim, expected_similarity, intersect_count, expected_intersect_count);
		}

		[TestMethod]
		public async Task OperateSimilarity()
		{
			double[] overlaps = new double[] { 0.0001, 0.001, 0.01, 0.1, 0.5 };

			foreach (double overlap in overlaps)
			{
				long expected_intersect_count = (long)(n_entries * overlap);
				IList common = new List<Value>();

				for (int i = 0; i < expected_intersect_count; i++)
				{
					common.Add(new Value.StringValue("common" + i));
				}

				List<IList> vals = new List<IList>();
				long unique_entries_per_node = (n_entries - expected_intersect_count) / 3;

				for (int i = 0; i < keys.Length; i++)
				{
					List<Value> sub_vals = new List<Value>();

					for (int j = 0; j < unique_entries_per_node; j++)
					{
						sub_vals.Add(new Value.StringValue("key" + i + " " + j));
					}

					vals.Add(sub_vals);
				}

				foreach (List<int> desc in legalDescriptions)
				{
					int index_bits = desc[0];
					int minhash_bits = desc[1];

					if (minhash_bits == 0)
					{
						continue;
					}

					await AssertSimilarityOp(overlap, common, vals, index_bits, minhash_bits);
				}
			}
		}
	}
}
