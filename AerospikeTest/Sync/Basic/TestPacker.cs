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
	public class TestPacker
	{
		[TestMethod]
		public void PackNumberUsesSignedIntegerBoundaries()
		{
			AssertPacked(-32L, 0xe0);
			AssertPacked(-33L, 0xd0, 0xdf);
			AssertPacked(sbyte.MinValue, 0xd0, 0x80);
			AssertPacked(-129L, 0xd1, 0xff, 0x7f);
			AssertPacked(short.MinValue, 0xd1, 0x80, 0x00);
		}

		private static void AssertPacked(long value, params byte[] expected)
		{
			Packer packer = new();
			packer.PackNumber(value);

			CollectionAssert.AreEqual(expected, packer.ToByteArray(), $"Unexpected MessagePack encoding for {value}.");
		}
	}
}
