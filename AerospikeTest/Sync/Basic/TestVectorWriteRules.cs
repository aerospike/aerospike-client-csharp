/*
 * Copyright 2012-2026 Aerospike, Inc.
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
using System.Collections;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// AV/AW/AS write-rule and edge-path coverage that needs a live 8.2.1+ cluster.
	/// </summary>
	[TestClass]
	public class TestVectorWriteRules : TestSync
	{
		private const string BinName = "vwr";
		private const string Set = "vecWriteRules";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			RequireVectorSupport();
		}

		/// <summary>AV-02: live bit-identical round-trip for extremes / -0 / subnormals / float16 specials.</summary>
		[TestMethod]
		public void LiveBitIdenticalExtremesRoundTrip()
		{
			AssertLiveBits(Vector.OfFloat32([-0f, 0f, float.Epsilon, float.MinValue, float.MaxValue]));
			AssertLiveBits(Vector.OfFloat64([-0.0, 0.0, double.Epsilon, double.MinValue, double.MaxValue]));
			AssertLiveBits(Vector.OfInt32([int.MinValue, int.MaxValue, 0, -1]));
			// Server rejects non-finite float16; keep -0 / subnormal / finite patterns for live AV-02.
			AssertLiveBits(Vector.OfFloat16(new short[] {
				0x0000, unchecked((short)0x8000), 0x0001, 0x3c00, unchecked((short)0xbc00), 0x4000
			}));
		}

		/// <summary>AV-11: plain arrays/lists/bytes are not particle type 16; real vectors are.</summary>
		[TestMethod]
		public void PlainCollectionsAreNotVectors()
		{
			Key key = Key("av11");
			client.Delete(null, key);

			client.Put(null, key,
				new Bin("arr", new List<object> { 1.0, 2.0, 3.0 }),
				new Bin("bytes", new byte[] { 1, 2, 3, 4 }),
				new Bin("vec", Vector.OfFloat32([1f, 2f, 3f])));

			try
			{
				Record record = client.Get(null, key);
				Assert.IsInstanceOfType(record.GetValue("arr"), typeof(System.Collections.IList));
				Assert.IsInstanceOfType(record.GetValue("bytes"), typeof(byte[]));
				Assert.IsNull(record.GetVector("arr"));
				Assert.IsNull(record.GetVector("bytes"));
				Assert.IsInstanceOfType(record.GetVector("vec"), typeof(Vector));
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AW-02: large vectors survive Get and scan (compression when EE allows).</summary>
		[TestMethod]
		public void LargeVectorGetAndScan()
		{
			// Full max float32 (65,536) is ~256 KiB; use max float64 for a full-cap payload.
			Vector large = Vector.OfFloat64(new double[Vector.MaxDimensionsFloat64]);
			Key key = Key("aw02");
			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, large), new Bin("mark", 202));

			try
			{
				Assert.AreEqual(large, client.Get(null, key).GetVector(BinName));

				Policy getCompressed = new() { compress = true };
				try
				{
					Assert.AreEqual(large, client.Get(getCompressed, key).GetVector(BinName));
				}
				catch (AerospikeException ae) when (ae.Result == ResultCode.UNSUPPORTED_FEATURE)
				{
					// Compression is EE-only.
				}

				int hits = 0;
				ScanPolicy scan = new();
				client.ScanAll(scan, SuiteHelpers.ns, Set, (k, record) =>
				{
					if (record.GetLong("mark") == 202)
					{
						Assert.AreEqual(large, record.GetVector(BinName));
						hits++;
					}
				}, BinName, "mark");
				Assert.AreEqual(1, hits);
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AW-05: write-back via put, generation policy, and batch replace+sendKey.</summary>
		[TestMethod]
		public void WriteBackWithGenerationAndBatchReplace()
		{
			Key key = Key("aw05");
			Vector original = Vector.OfFloat32([1f, 2f, 3f]);
			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, original));

			try
			{
				Record first = client.Get(null, key);
				Vector roundTrip = first.GetVector(BinName);
				Assert.AreEqual(original, roundTrip);

				WritePolicy gen = new()
				{
					generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL,
					generation = first.generation
				};
				Vector next = Vector.OfFloat32([4f, 5f, 6f]);
				client.Put(gen, key, new Bin(BinName, next));
				Assert.AreEqual(next, client.Get(null, key).GetVector(BinName));

				BatchWritePolicy bwp = new()
				{
					sendKey = true,
					recordExistsAction = RecordExistsAction.REPLACE
				};
				Vector replaced = Vector.OfInt32([7, 8, 9]);
				List<BatchRecord> batch = [new BatchWrite(bwp, key, [Operation.Put(new Bin(BinName, replaced))])];
				Assert.IsTrue(client.Operate(null, batch));
				Assert.AreEqual(ResultCode.OK, batch[0].resultCode);
				Assert.AreEqual(replaced, client.Get(null, key).GetVector(BinName));
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AW-07: scan with last-update-time filter keeps vectors intact.</summary>
		[TestMethod]
		public void ScanWithLastUpdateFilter()
		{
			Key key = Key("aw07");
			Vector v = Vector.OfFloat32([3f, 2f, 1f]);
			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, v), new Bin("mark", 707));

			try
			{
				ScanPolicy policy = new()
				{
					filterExp = Exp.Build(Exp.GT(Exp.LastUpdate(), Exp.Val(new DateTime(2000, 1, 1))))
				};
				int hits = 0;
				client.ScanAll(policy, SuiteHelpers.ns, Set, (k, record) =>
				{
					if (record.GetLong("mark") == 707)
					{
						Assert.AreEqual(v, record.GetVector(BinName));
						hits++;
					}
				}, BinName, "mark");
				Assert.AreEqual(1, hits);
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AW-08: replace type/size/scalar, null bin, delete — generation/TTL follow normal rules.</summary>
		[TestMethod]
		public void ReplaceNullAndDeleteFollowNormalRules()
		{
			Key key = Key("aw08");
			client.Delete(null, key);

			WritePolicy ttlPolicy = new() { expiration = 120 };
			client.Put(ttlPolicy, key, new Bin(BinName, Vector.OfFloat32([1f, 2f])));
			Record r1 = client.Get(null, key);
			int gen1 = r1.generation;

			client.Put(null, key, new Bin(BinName, Vector.OfInt32([1, 2, 3, 4])));
			Record r2 = client.Get(null, key);
			Assert.AreEqual(Vector.OfInt32([1, 2, 3, 4]), r2.GetVector(BinName));
			Assert.IsTrue(r2.generation > gen1);

			client.Put(null, key, new Bin(BinName, 99));
			Record r3 = client.Get(null, key);
			Assert.AreEqual(99, r3.GetLong(BinName));
			Assert.IsNull(r3.GetVector(BinName));
			Assert.IsTrue(r3.generation > r2.generation);

			client.Put(null, key, Bin.AsNull(BinName), new Bin("keep", 1));
			Record r4 = client.Get(null, key);
			Assert.IsNull(r4.GetValue(BinName));
			Assert.AreEqual(1, r4.GetLong("keep"));

			Assert.IsTrue(client.Delete(null, key));
			Assert.IsNull(client.Get(null, key));
		}

		/// <summary>AW-09: several vector bins coexist with scalar bins.</summary>
		[TestMethod]
		public void MultipleVectorBinsIntact()
		{
			Key key = Key("aw09");
			Vector a = Vector.OfFloat16(new Half[] { (Half)1f, (Half)2f });
			Vector b = Vector.OfFloat32([3f, 4f, 5f]);
			Vector c = Vector.OfFloat64([6.0, 7.0]);
			client.Delete(null, key);
			client.Put(null, key, new Bin("va", a), new Bin("vb", b), new Bin("vc", c), new Bin("s", "ok"));

			try
			{
				Record record = client.Get(null, key);
				Assert.AreEqual(a, record.GetVector("va"));
				Assert.AreEqual(b, record.GetVector("vb"));
				Assert.AreEqual(c, record.GetVector("vc"));
				Assert.AreEqual("ok", record.GetString("s"));
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AW-10: durable-delete of last vector bin — CE reports unsupported / EE tombstones.</summary>
		[TestMethod]
		public void DurableDeleteLastVectorBin()
		{
			Key key = Key("aw10");
			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, Vector.OfFloat32([1f])));

			WritePolicy wp = new() { durableDelete = true };
			try
			{
				client.Put(wp, key, Bin.AsNull(BinName));
				Assert.IsNull(client.Get(null, key));
			}
			catch (AerospikeException ae) when (ae.Result == ResultCode.UNSUPPORTED_FEATURE)
			{
				// CE: durable-delete flag reaches the server as unsupported (rc 25).
				Assert.AreEqual(ResultCode.UNSUPPORTED_FEATURE, ae.Result);
			}
			finally
			{
				try { client.Delete(null, key); } catch (AerospikeException) { }
			}
		}

		/// <summary>AW-12: batch with one invalid raw vector — per-record codes; valids succeed.</summary>
		[TestMethod]
		public void BatchWriteInvalidVectorIsPerRecord()
		{
			Key good = Key("aw12g");
			Key bad = Key("aw12b");
			client.Delete(null, good);
			client.Delete(null, bad);

			byte[] malformed = new byte[Vector.HEADER_SIZE];
			malformed[0] = 2; // version 2
			malformed[1] = (byte)Vector.ElementType.FLOAT32;
			ByteUtil.IntToLittleBytes(1, malformed, 2);

			Vector ok = Vector.OfFloat32([1f, 2f]);
			List<BatchRecord> records =
			[
				new BatchWrite(good, [Operation.Put(new Bin(BinName, ok))]),
				new BatchWrite(bad, [Operation.Put(new Bin(BinName, new Value.BytesValue(malformed, ParticleType.VECTOR)))])
			];

			bool status = client.Operate(null, records);
			Assert.IsFalse(status);
			Assert.AreEqual(ResultCode.OK, records[0].resultCode);
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, records[1].resultCode);
			Assert.AreEqual(ok, client.Get(null, good).GetVector(BinName));
			Assert.IsNull(client.Get(null, bad));

			client.Delete(null, good);
		}

		/// <summary>AS-02: append/prepend/increment against a vector bin fail on the server.</summary>
		[TestMethod]
		public void ScalarOpsOnVectorBinRejected()
		{
			Key key = Key("as02");
			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, Vector.OfFloat32([1f, 2f])));

			try
			{
				Assert.Throws<AerospikeException>(() =>
					client.Operate(null, key, StringOperation.Append(StringPolicy.Default, BinName, "x")));
				Assert.Throws<AerospikeException>(() =>
					client.Operate(null, key, StringOperation.Prepend(StringPolicy.Default, BinName, "x")));
				Assert.Throws<AerospikeException>(() => client.Add(null, key, new Bin(BinName, 1)));
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AS-03: raw type-16 with version 2 or element type 5 → PARAMETER_ERROR.</summary>
		[TestMethod]
		public void MalformedRawVectorRejected()
		{
			AssertRawRejected(version: 2, elementType: (byte)Vector.ElementType.FLOAT32, dims: 1);
			AssertRawRejected(version: Vector.VERSION, elementType: 5, dims: 1);
		}

		private static void AssertRawRejected(byte version, byte elementType, int dims)
		{
			Key key = Key("as03_" + version + "_" + elementType);
			client.Delete(null, key);

			byte[] raw = new byte[Vector.HEADER_SIZE + Math.Max(dims, 0) * 4];
			raw[0] = version;
			raw[1] = elementType;
			ByteUtil.IntToLittleBytes((uint)dims, raw, 2);

			AerospikeException ae = Assert.Throws<AerospikeException>(
				() => client.Put(null, key, new Bin(BinName, new Value.BytesValue(raw, ParticleType.VECTOR))));
			Assert.AreEqual(ResultCode.PARAMETER_ERROR, ae.Result);
		}

		private static void AssertLiveBits(Vector v)
		{
			Key key = Key("bits_" + Guid.NewGuid().ToString("N"));
			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, v));
			try
			{
				Assert.AreEqual(v, client.Get(null, key).GetVector(BinName));
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		private static Key Key(string userKey) => new(SuiteHelpers.ns, Set, userKey);
	}
}