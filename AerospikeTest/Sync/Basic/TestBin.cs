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
	[TestClass]
	public class TestBin
	{
		[TestMethod]
		public void BinConstructorsWrapValues()
		{
			Bin fromValue = new("name", Value.Get(42));
			Bin fromString = new("text", "hello");
			Bin fromBytes = new("blob", new byte[] { 1, 2, 3 });

			Assert.AreEqual("name", fromValue.name);
			Assert.AreEqual(42, fromValue.value.Object);
			Assert.AreEqual("hello", fromString.value.Object);
			CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, (byte[])fromBytes.value.Object);
		}

		[TestMethod]
		public void BinToStringIncludesNameAndValue()
		{
			Bin bin = new("score", 10);

			Assert.IsTrue(bin.ToString().StartsWith("score:"));
			Assert.IsTrue(bin.ToString().Contains("10"));
		}

		[TestMethod]
		public void BinAsNullAndGeoJsonHelpers()
		{
			Bin nil = Bin.AsNull("empty");
			Bin geo = Bin.AsGeoJSON("loc", "{ \"type\": \"Point\", \"coordinates\": [0, 0] }");

			Assert.AreEqual(Value.AsNull, nil.value);
			Assert.IsInstanceOfType(geo.value, typeof(Value.GeoJSONValue));
		}
	}
}
