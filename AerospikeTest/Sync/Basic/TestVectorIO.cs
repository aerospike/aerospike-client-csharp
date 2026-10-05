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
	/// </summary>
	[TestClass]
	public class TestVectorIO : TestSync
	{
		private const string BinName = "vecbin";

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
		public void EmptyVectorWriteRejectedByServer()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "vecempty");
			client.Delete(null, key);

			try
			{
				client.Put(null, key, new Bin(BinName, Vector.OfFloat32(Array.Empty<float>())));
				Assert.Fail("Expected PARAMETER_ERROR for zero-dimension vector");
			}
			catch (AerospikeException e)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, e.Result);
			}
		}

		[TestMethod]
		public void VectorInListBin()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "veclistbin");
			client.Delete(null, key);

			Vector v = Vector.OfInt32(new int[] { 1, 2, 3 });
			client.Put(null, key, new Bin("listbin", new List<object> { v }));

			Record record = client.Get(null, key);
			AssertRecordFound(key, record);

			IList list = record.GetList("listbin");
			Assert.AreEqual(1, list.Count);
			Assert.AreEqual(v, list[0]);
		}

		[TestMethod]
		public void VectorInMapBin()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "vecmapbin");
			client.Delete(null, key);

			Vector v = Vector.OfFloat32(new float[] { 1.5f, 2.5f });
			client.Put(null, key, new Bin("mapbin", new Dictionary<object, object> { { "k", v } }));

			Record record = client.Get(null, key);
			AssertRecordFound(key, record);

			IDictionary map = record.GetMap("mapbin");
			Assert.AreEqual(v, map["k"]);
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
	}
}
