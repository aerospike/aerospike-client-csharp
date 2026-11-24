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
	/// Server-round-trip tests for the vector particle type.
	/// Requires server version 8.2.1+.
	/// </summary>
	[TestClass]
	public class TestVectorIO : TestSync
	{
		private const string BinName = "vecbin";

		[ClassInitialize]
		public static void ServerVersionCheck(TestContext testContext)
		{
			RequireVectorSupport();
		}

		[TestMethod]
		public void PutGetFloat16()
		{
			PutGetRoundTrip("veckey16", Vector.OfFloat16(new short[] { 0x3c00, unchecked((short)0xbc00), 0x4000 }));
		}

		[TestMethod]
		public void PutGetInt32()
		{
			PutGetRoundTrip("veckey32i", Vector.OfInt32(new int[] { -5, 0, 7, 12345 }));
		}

		[TestMethod]
		public void PutGetFloat32()
		{
			PutGetRoundTrip("veckey32f", Vector.OfFloat32(new float[] { 1.5f, -2.25f, 3.14159f }));
		}

		[TestMethod]
		public void PutGetFloat64()
		{
			PutGetRoundTrip("veckey64", Vector.OfFloat64(new double[] { 1.5, -2.25, 3.14159 }));
		}

		private static void PutGetRoundTrip(string userKey, Vector v)
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, userKey);
			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, v));

			Record record = client.Get(null, key);
			AssertRecordFound(key, record);
			Assert.AreEqual(v, record.GetVector(BinName));
		}

		[TestMethod]
		public void OverwriteWithDifferentDimensions()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "vecoverwrite");
			client.Delete(null, key);

			client.Put(null, key, new Bin(BinName, Vector.OfFloat32(new float[] { 1.0f, 2.0f })));
			Vector second = Vector.OfFloat32(new float[] { 9.0f, 8.0f, 7.0f, 6.0f });
			client.Put(null, key, new Bin(BinName, second));

			Record record = client.Get(null, key);
			AssertRecordFound(key, record);
			Assert.AreEqual(second, record.GetVector(BinName));
		}

		[TestMethod]
		public void EmptyVectorRejectedByClient()
		{
			// Of* rejects empty arrays before any round trip.
			AerospikeException e = Assert.Throws<AerospikeException>(
				() => Vector.OfFloat32(Array.Empty<float>()));
			Assert.AreEqual(ResultCode.PARSE_ERROR, e.Result);
		}

		[TestMethod]
		public void EmptyVectorWriteRejectedByServer()
		{
			// Bypass Vector.Of* and send a raw zero-dimension VECTOR particle so the
			// server still rejects malformed wire data.
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "vecempty");
			client.Delete(null, key);

			byte[] empty = new byte[Vector.HEADER_SIZE];
			empty[0] = Vector.VERSION;
			empty[1] = (byte)Vector.ElementType.FLOAT32;
			ByteUtil.IntToLittleBytes(0, empty, 2);

			try
			{
				client.Put(null, key, new Bin(BinName, new Value.BytesValue(empty, ParticleType.VECTOR)));
				Assert.Fail("Expected PARAMETER_ERROR for zero-dimension vector");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void VectorInListBinRejected()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "veclistbin");
			client.Delete(null, key);

			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			AerospikeException e = Assert.Throws<AerospikeException>(
				() => client.Put(null, key, new Bin("listbin", new List<object> { v })));
			Assert.AreEqual(ResultCode.PARSE_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains("list/map cannot hold a vector"));
		}

		[TestMethod]
		public void VectorInMapBinRejected()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "vecmapbin");
			client.Delete(null, key);

			Vector v = Vector.OfFloat32(new float[] { 1.5f, 2.5f });
			AerospikeException e = Assert.Throws<AerospikeException>(
				() => client.Put(null, key, new Bin("mapbin", new Dictionary<object, object> { { "k", v } })));
			Assert.AreEqual(ResultCode.PARSE_ERROR, e.Result);
			Assert.IsTrue(e.Message.Contains("list/map cannot hold a vector"));
		}

		[TestMethod]
		public void OperateVectorRoundTrip()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "vecoperate");
			client.Delete(null, key);

			Vector v = Vector.OfFloat64(new double[] { 1.1, 2.2, 3.3 });
			Record record = client.Operate(null, key,
				Operation.Put(new Bin(BinName, v)),
				Operation.Get(BinName));

			AssertRecordFound(key, record);
			Assert.AreEqual(v, record.GetVector(BinName));
		}

		[TestMethod]
		public void BatchWriteReadVectors()
		{
			int count = 5;
			Key[] keys = new Key[count];
			Vector[] vectors = new Vector[count];

			for (int i = 0; i < count; i++)
			{
				keys[i] = new Key(SuiteHelpers.ns, SuiteHelpers.set, "vecbatch" + i);
				client.Delete(null, keys[i]);
				vectors[i] = Vector.OfFloat32(new float[] { i, i + 0.5f, i + 1.0f });
				client.Put(null, keys[i], new Bin(BinName, vectors[i]));
			}

			Record[] records = client.Get(null, keys);
			Assert.AreEqual(count, records.Length);

			for (int i = 0; i < count; i++)
			{
				AssertRecordFound(keys[i], records[i]);
				Assert.AreEqual(vectors[i], records[i].GetVector(BinName));
			}
		}

		[TestMethod]
		public void WriteAllElementTypesPreservesBytes()
		{
			Vector v = Vector.OfInt32(new int[] { int.MinValue, 0, int.MaxValue });
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "vecbytes");
			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, v));

			Record record = client.Get(null, key);
			AssertRecordFound(key, record);
			CollectionAssert.AreEqual(v.GetInt32Data(), record.GetVector(BinName).GetInt32Data());
		}

		/// <summary>AW-03: typed vector reads across Get / selected bins / operate / exp / batch / query / scan / txn.</summary>
		[TestMethod]
		public void ReadPathsReturnVectorNotBytes()
		{
			const string set = "vecReadPaths";
			const string marker = "vecpath";
			Key key = new(SuiteHelpers.ns, set, "read1");
			Vector v = Vector.OfFloat32([9f, 8f, 7f]);

			client.Delete(null, key);
			client.Put(null, key, new Bin(BinName, v), new Bin(marker, 1));

			try
			{
				AssertVector(client.Get(null, key).GetVector(BinName), v);
				AssertVector(client.Get(null, key, BinName).GetVector(BinName), v);

				Record operate = client.Operate(null, key, Operation.Get(BinName));
				AssertVector(operate.GetVector(BinName), v);

				Record expRead = client.Operate(null, key,
					ExpOperation.Read("copy", Exp.Build(Exp.VectorBin(BinName)), ExpReadFlags.DEFAULT));
				AssertVector(expRead.GetVector("copy"), v);

				Record[] batch = client.Get(null, [key]);
				AssertVector(batch[0].GetVector(BinName), v);

				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(set);
				stmt.SetBinNames(BinName);
				QueryPolicy qp = new()
				{
					filterExp = Exp.Build(Exp.EQ(Exp.IntBin(marker), Exp.Val(1)))
				};
				using (RecordSet rs = client.Query(qp, stmt))
				{
					Assert.IsTrue(rs.Next());
					AssertVector(rs.Record.GetVector(BinName), v);
					Assert.IsFalse(rs.Next());
				}

				int scanned = 0;
				client.ScanAll(null, SuiteHelpers.ns, set, (k, record) =>
				{
					if (record.GetValue(BinName) != null)
					{
						AssertVector(record.GetVector(BinName), v);
						scanned++;
					}
				}, BinName);
				Assert.IsTrue(scanned >= 1);

				using Txn txn = new();
				Policy rp = client.ReadPolicyDefault.Clone();
				rp.Txn = txn;
				AssertVector(client.Get(rp, key).GetVector(BinName), v);
				client.Abort(txn);
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AW-04: vector writes via put / operate / batch / background query / expression write.</summary>
		[TestMethod]
		public void WritePathsStoreVectorParticle()
		{
			const string set = "vecWritePaths";
			Vector v = Vector.OfFloat32([1f, 0f, -1f]);

			Key putKey = new(SuiteHelpers.ns, set, "wput");
			Key opKey = new(SuiteHelpers.ns, set, "wop");
			Key batchKey = new(SuiteHelpers.ns, set, "wbatch");
			Key expKey = new(SuiteHelpers.ns, set, "wexp");
			Key bgKey = new(SuiteHelpers.ns, set, "wbg");

			foreach (Key k in new[] { putKey, opKey, batchKey, expKey, bgKey })
			{
				client.Delete(null, k);
			}

			try
			{
				client.Put(null, putKey, new Bin(BinName, v));
				AssertVector(client.Get(null, putKey).GetVector(BinName), v);

				client.Operate(null, opKey, Operation.Put(new Bin(BinName, v)));
				AssertVector(client.Get(null, opKey).GetVector(BinName), v);

				List<BatchRecord> batchRecords = [new BatchWrite(batchKey, [Operation.Put(new Bin(BinName, v))])];
				Assert.IsTrue(client.Operate(null, batchRecords));
				AssertVector(client.Get(null, batchKey).GetVector(BinName), v);

				// Expression write of a vector bin (copy). Constant Exp.Val(Vector) is not a
				// server-supported write rtype in 8.2.1; bin-typed VECTOR expressions are.
				client.Put(null, expKey, new Bin("src", v));
				client.Operate(null, expKey,
					ExpOperation.Write(BinName, Exp.Build(Exp.VectorBin("src")), ExpWriteFlags.DEFAULT));
				AssertVector(client.Get(null, expKey).GetVector(BinName), v);

				client.Put(null, bgKey, new Bin("id", 42));
				Statement bg = new();
				bg.SetNamespace(SuiteHelpers.ns);
				bg.SetSetName(set);
				WritePolicy bgPolicy = new()
				{
					filterExp = Exp.Build(Exp.EQ(Exp.IntBin("id"), Exp.Val(42)))
				};
				ExecuteTask task = client.Execute(bgPolicy, bg, Operation.Put(new Bin(BinName, v)));
				task.Wait(3000, 3000);
				AssertVector(client.Get(null, bgKey).GetVector(BinName), v);
			}
			finally
			{
				foreach (Key k in new[] { putKey, opKey, batchKey, expKey, bgKey })
				{
					client.Delete(null, k);
				}
			}
		}

		/// <summary>AW-06: projection, partition page, and metadata-only reads keep vectors intact / decode-safe.</summary>
		[TestMethod]
		public void ProjectionPagingAndMetadataOnly()
		{
			const string set = "vecProjPage";
			const int count = 8;
			Key[] keys = new Key[count];
			Vector[] vectors = new Vector[count];

			for (int i = 0; i < count; i++)
			{
				keys[i] = new Key(SuiteHelpers.ns, set, "p" + i);
				vectors[i] = Vector.OfFloat32([i, i + 0.25f]);
				client.Delete(null, keys[i]);
				client.Put(null, keys[i], new Bin(BinName, vectors[i]), new Bin("n", i));
			}

			try
			{
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(set);
				stmt.SetBinNames(BinName);
				QueryPolicy qp = new()
				{
					filterExp = Exp.Build(Exp.BinExists("n"))
				};

				HashSet<long> seen = new();
				using (RecordSet rs = client.Query(qp, stmt))
				{
					while (rs.Next())
					{
						Vector got = rs.Record.GetVector(BinName);
						Assert.IsNotNull(got);
						Assert.IsInstanceOfType(got, typeof(Vector));
						seen.Add((long)got.GetFloat32Data()[0]);
					}
				}
				Assert.AreEqual(count, seen.Count);

				Statement pageStmt = new();
				pageStmt.SetNamespace(SuiteHelpers.ns);
				pageStmt.SetSetName(set);
				pageStmt.SetBinNames(BinName);
#pragma warning disable 0618
				pageStmt.maxRecords = 3;
#pragma warning restore 0618
				PartitionFilter filter = PartitionFilter.All();
				int paged = 0;
				for (int page = 0; page < 8 && !filter.Done; page++)
				{
					using RecordSet rs = client.QueryPartitions(null, pageStmt, filter);
					while (rs.Next())
					{
						Assert.IsInstanceOfType(rs.Record.GetVector(BinName), typeof(Vector));
						paged++;
					}
				}
				Assert.AreEqual(count, paged);

				ScanPolicy meta = new() { includeBinData = false };
				int metaCount = 0;
				client.ScanAll(meta, SuiteHelpers.ns, set, (k, record) =>
				{
					Assert.IsNotNull(record);
					metaCount++;
				});
				Assert.IsTrue(metaCount >= count);
			}
			finally
			{
				foreach (Key key in keys)
				{
					client.Delete(null, key);
				}
			}
		}

		private static void AssertVector(Vector actual, Vector expected)
		{
			Assert.IsNotNull(actual);
			Assert.IsInstanceOfType(actual, typeof(Vector));
			Assert.AreEqual(expected, actual);
		}
	}
}
