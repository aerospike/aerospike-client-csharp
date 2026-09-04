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
using System.Collections;

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

		[TestMethod]
		public void SortMapsProducesCanonicalKeyOrder()
		{
			Hashtable scrambled = new()
			{
				["z"] = 3,
				["a"] = 1,
				["m"] = 2
			};
			Hashtable ordered = new()
			{
				["a"] = 1,
				["m"] = 2,
				["z"] = 3
			};

			byte[] scrambledPacked = PackCanonicalMap(scrambled);
			byte[] orderedPacked = PackCanonicalMap(ordered);

			CollectionAssert.AreEqual(orderedPacked, scrambledPacked);
		}

		[TestMethod]
		public void SortMapsOrdersMixedTypesByCanonicalRules()
		{
			Hashtable map = new()
			{
				[10] = "int-key",
				[-5] = "neg-key",
				["text"] = "string-key"
			};

			byte[] packed = PackCanonicalMap(map);

			Assert.IsTrue(packed.Length > 0);
			// Negative integers sort before non-negative integers, which sort before strings.
			int negOffset = IndexOf(packed, PackNumberBytes(-5));
			int intOffset = IndexOf(packed, PackNumberBytes(10));
			int stringOffset = IndexOf(packed, PackParticleStringBytes("text"));
			Assert.IsTrue(negOffset >= 0);
			Assert.IsTrue(intOffset >= 0);
			Assert.IsTrue(stringOffset >= 0);
			Assert.IsTrue(negOffset < intOffset);
			Assert.IsTrue(intOffset < stringOffset);
		}

		[TestMethod]
		public void SortMapsRejectsDuplicatePackedKeys()
		{
			DuplicatePackedKeyMap map = new();

			try
			{
				PackCanonicalMap(map);
				Assert.Fail("Expected AerospikeException for duplicate packed keys");
			}
			catch (AerospikeException ex)
			{
				Assert.AreEqual(ResultCode.PARAMETER_ERROR, ex.Result);
				Assert.IsTrue(ex.Message.Contains("duplicate msgpack keys"));
			}
		}

		private static byte[] PackCanonicalMap(IDictionary map)
		{
			Packer packer = new();
			packer.SortMaps(true);
			packer.PackMap(map);
			return packer.ToByteArray();
		}

		private static byte[] PackNumberBytes(long value)
		{
			Packer packer = new();
			packer.PackNumber(value);
			return packer.ToByteArray();
		}

		private static byte[] PackParticleStringBytes(string value)
		{
			Packer packer = new();
			packer.PackParticleString(value);
			return packer.ToByteArray();
		}

		private static int IndexOf(byte[] haystack, byte[] needle)
		{
			for (int i = 0; i <= haystack.Length - needle.Length; i++)
			{
				bool match = true;
				for (int j = 0; j < needle.Length; j++)
				{
					if (haystack[i + j] != needle[j])
					{
						match = false;
						break;
					}
				}

				if (match)
				{
					return i;
				}
			}
			return -1;
		}

		private sealed class DuplicatePackedKeyMap : IDictionary
		{
			public int Count => 2;
			public bool IsFixedSize => true;
			public bool IsReadOnly => true;
			public bool IsSynchronized => false;
			public object SyncRoot => this;

			public ICollection Keys => throw new NotSupportedException();
			public ICollection Values => throw new NotSupportedException();

			public object this[object key]
			{
				get => throw new NotSupportedException();
				set => throw new NotSupportedException();
			}

			public void Add(object key, object value) => throw new NotSupportedException();
			public void Clear() => throw new NotSupportedException();
			public bool Contains(object key) => throw new NotSupportedException();
			public void CopyTo(Array array, int index) => throw new NotSupportedException();
			public IDictionaryEnumerator GetEnumerator() => new DuplicateEnumerator();
			public void Remove(object key) => throw new NotSupportedException();
			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

			private sealed class DuplicateEnumerator : IDictionaryEnumerator
			{
				private int index = -1;

				public object Current => Entry;
				public DictionaryEntry Entry => index switch
				{
					0 => new DictionaryEntry((short)1, "first"),
					1 => new DictionaryEntry((int)1, "second"),
					_ => throw new InvalidOperationException()
				};
				public object Key => Entry.Key;
				public object Value => Entry.Value;

				public bool MoveNext()
				{
					index++;
					return index < 2;
				}

				public void Reset() => index = -1;
			}
		}

		private static void AssertPacked(long value, params byte[] expected)
		{
			Packer packer = new();
			packer.PackNumber(value);

			CollectionAssert.AreEqual(expected, packer.ToByteArray(), $"Unexpected MessagePack encoding for {value}.");
		}
	}
}
