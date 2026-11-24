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
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// AS-04: secondary indexes on vector bins — blob index may be created but holds no vector entries.
	/// </summary>
	[TestClass]
	public class TestVectorIndex : TestSync
	{
		private const string SetName = "vecIdxSet";
		private const string VecBin = "embedding";
		private const string BlobIndex = "vec_blob_idx";
		private const string IntIndex = "vec_int_idx";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			RequireVectorSupport();
		}

		[ClassCleanup]
		public static void Destroy()
		{
			TryDrop(BlobIndex);
			TryDrop(IntIndex);
		}

		/// <summary>
		/// A BLOB index on a vector bin name can be created, but vector particles are not indexed entries.
		/// </summary>
		[TestMethod]
		public void BlobIndexOnVectorBinHoldsNoVectorEntries()
		{
			Policy indexPolicy = new() { totalTimeout = 0 };
			TryDrop(BlobIndex);

			try
			{
				client.CreateIndex(indexPolicy, SuiteHelpers.ns, SetName, BlobIndex, VecBin, IndexType.BLOB).Wait();
			}
			catch (AerospikeException ae) when (ae.Result == ResultCode.INDEX_ALREADY_EXISTS)
			{
			}

			Key key = new(SuiteHelpers.ns, SetName, "vecidx1");
			Vector v = Vector.OfFloat32([1f, 2f, 3f]);
			client.Delete(null, key);
			client.Put(null, key, new Bin(VecBin, v));

			try
			{
				byte[] wire = new byte[v.GetWireSize()];
				v.WriteTo(wire, 0);

				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(SetName);
				stmt.SetBinNames(VecBin);
				stmt.SetFilter(Filter.Equal(VecBin, wire));

				int count = 0;
				using RecordSet rs = client.Query(null, stmt);

				while (rs.Next())
				{
					count++;
				}

				Assert.AreEqual(0, count, "BLOB SI must not index vector particles");
				Assert.AreEqual(v, client.Get(null, key).GetVector(VecBin));
			}
			finally
			{
				client.Delete(null, key);
				TryDrop(BlobIndex);
			}
		}

		/// <summary>
		/// Creating a numeric index on a vector bin is allowed; the vector record is not returned by SI filter.
		/// </summary>
		[TestMethod]
		public void NumericIndexOnVectorBinDoesNotMatchVectorRecords()
		{
			Policy indexPolicy = new() { totalTimeout = 0 };
			TryDrop(IntIndex);

			try
			{
				client.CreateIndex(indexPolicy, SuiteHelpers.ns, SetName, IntIndex, VecBin, IndexType.NUMERIC).Wait();
			}
			catch (AerospikeException ae) when (ae.Result == ResultCode.INDEX_ALREADY_EXISTS)
			{
			}

			Key key = new(SuiteHelpers.ns, SetName, "vecidx2");
			client.Delete(null, key);
			client.Put(null, key, new Bin(VecBin, Vector.OfInt32([1, 2, 3])));

			try
			{
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(SetName);
				stmt.SetFilter(Filter.Equal(VecBin, 1));

				int count = 0;
				using RecordSet rs = client.Query(null, stmt);

				while (rs.Next())
				{
					count++;
				}

				Assert.AreEqual(0, count);
			}
			finally
			{
				client.Delete(null, key);
				TryDrop(IntIndex);
			}
		}

		private static void TryDrop(string indexName)
		{
			try
			{
				client.DropIndex(null, SuiteHelpers.ns, SetName, indexName);
			}
			catch (AerospikeException)
			{
			}
		}
	}
}