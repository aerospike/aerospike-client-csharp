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
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// AU-01 / AU-02 / AU-03 / AU-04: record and stream UDF vector behaviour.
	/// </summary>
	[TestClass]
	public class TestVectorUdf : TestSync
	{
		private const string BinName = "udfvec";
		private const string StreamSet = "udfVecStream";
		private const string StreamIndex = "udf_vec_stream_idx";
		private const string IdBin = "id";

		[ClassInitialize]
		public static void Prepare(TestContext testContext)
		{
			RequireVectorSupport();

			Assembly assembly = Assembly.GetExecutingAssembly();
			client.Register(null, assembly, "Aerospike.Test.LuaResources.record_example.lua",
				"record_example.lua", Language.LUA).Wait();
			client.Register(null, assembly, "Aerospike.Test.LuaResources.vector_stream_example.lua",
				"vector_stream_example.lua", Language.LUA).Wait();

			Policy indexPolicy = new() { totalTimeout = 0 };
			try
			{
				client.CreateIndex(indexPolicy, SuiteHelpers.ns, StreamSet, StreamIndex, IdBin, IndexType.INTEGER).Wait();
			}
			catch (AerospikeException ae) when (ae.Result == ResultCode.INDEX_ALREADY_EXISTS)
			{
			}
		}

		[ClassCleanup]
		public static void Destroy()
		{
			try
			{
				client.DropIndex(null, SuiteHelpers.ns, StreamSet, StreamIndex);
			}
			catch (AerospikeException)
			{
			}
		}

		[TestMethod]
		public void RecordUdfStoresAndReturnsVector()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "udfvec1");
			client.Delete(null, key);

			Vector v = Vector.OfFloat32([1.5f, -2.25f, 3.125f]);

			client.Execute(null, key, "record_example", "writeBin", Value.Get(BinName), Value.Get(v));

			Record record = client.Get(null, key, BinName);
			AssertRecordFound(key, record);
			Assert.AreEqual(v, record.GetVector(BinName));

			object returned = client.Execute(null, key, "record_example", "readBin", Value.Get(BinName));
			Assert.IsInstanceOfType(returned, typeof(Vector));
			Assert.AreEqual(v, (Vector)returned);
		}

		/// <summary>AU-02: UDF write of a malformed vector fails with PARAMETER_ERROR or UDF error.</summary>
		[TestMethod]
		public void UdfInvalidVectorRejected()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "udfvecBad");
			client.Delete(null, key);

			byte[] malformed = new byte[Vector.HEADER_SIZE];
			malformed[0] = 2;
			malformed[1] = (byte)Vector.ElementType.FLOAT32;
			ByteUtil.IntToLittleBytes(1, malformed, 2);

			try
			{
				client.Execute(null, key, "record_example", "writeBin",
					Value.Get(BinName), new Value.BytesValue(malformed, ParticleType.VECTOR));
				Assert.Fail("expected UDF/parameter rejection for malformed vector");
			}
			catch (AerospikeException ae)
			{
				Assert.IsTrue(
					ae.Result == ResultCode.PARAMETER_ERROR || ae.Result == ResultCode.UDF_BAD_RESPONSE,
					"unexpected rc " + ae.Result);
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>
		/// AU-03: stream (aggregate) UDF delivers a stored vector as bytes, like HLL.
		/// </summary>
		[TestMethod]
		public void StreamUdfReturnsVectorAsBytes()
		{
			Key key = new(SuiteHelpers.ns, StreamSet, "stream1");
			Vector v = Vector.OfFloat32([9f, 8f, 7f]);
			client.Delete(null, key);
			client.Put(null, key, new Bin(IdBin, 1), new Bin(BinName, v));

			try
			{
				Statement stmt = new();
				stmt.SetNamespace(SuiteHelpers.ns);
				stmt.SetSetName(StreamSet);
				stmt.SetFilter(Filter.Equal(IdBin, 1));
				stmt.SetAggregateFunction(
					Assembly.GetExecutingAssembly(),
					"Aerospike.Test.LuaResources.vector_stream_example.lua",
					"vector_stream_example",
					"return_vector_bin",
					Value.Get(BinName));

				int count = 0;
				using ResultSet rs = client.QueryAggregate(null, stmt);
				while (rs.Next())
				{
					count++;
					object obj = rs.Object;
					Assert.IsInstanceOfType(obj, typeof(byte[]),
						"stream UDF should surface vector as bytes, got " + obj?.GetType().Name);
					byte[] bytes = (byte[])obj;
					Assert.AreEqual(v, Vector.From(bytes, 0, bytes.Length));
				}
				Assert.AreEqual(1, count);
			}
			finally
			{
				client.Delete(null, key);
			}
		}

		/// <summary>AU-04: batch UDF with vector arguments — per-record results.</summary>
		[TestMethod]
		public void BatchUdfWithVectorArgs()
		{
			Key[] keys =
			[
				new Key(SuiteHelpers.ns, SuiteHelpers.set, "udfvecB0"),
				new Key(SuiteHelpers.ns, SuiteHelpers.set, "udfvecB1")
			];
			foreach (Key key in keys)
			{
				client.Delete(null, key);
			}

			Vector v0 = Vector.OfFloat32([1f, 2f]);
			Vector v1 = Vector.OfFloat32([3f, 4f]);

			try
			{
				BatchResults results = client.Execute(null, null, keys, "record_example", "writeBin",
					Value.Get(BinName), Value.Get(v0));
				Assert.IsNotNull(results);
				client.Execute(null, keys[1], "record_example", "writeBin", Value.Get(BinName), Value.Get(v1));

				Assert.AreEqual(v0, client.Get(null, keys[0]).GetVector(BinName));
				Assert.AreEqual(v1, client.Get(null, keys[1]).GetVector(BinName));
			}
			finally
			{
				foreach (Key key in keys)
				{
					client.Delete(null, key);
				}
			}
		}
	}
}
