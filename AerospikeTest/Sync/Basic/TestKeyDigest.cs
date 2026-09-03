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
	public class TestKeyDigest : TestSync
	{
		private const string BinName = "kdbin";

		[TestMethod]
		public void DigestStability()
		{
			Key key1 = new(SuiteHelpers.ns, SuiteHelpers.set, "digest-stable");
			Key key2 = new(SuiteHelpers.ns, SuiteHelpers.set, "digest-stable");

			CollectionAssert.AreEqual(key1.digest, key2.digest);
		}

		[TestMethod]
		public void DigestDiffersBySet()
		{
			Key key1 = new(SuiteHelpers.ns, SuiteHelpers.set, "digest-set");
			Key key2 = new(SuiteHelpers.ns, "other-set", "digest-set");

			Assert.IsFalse(key1.Equals(key2));
		}

		[TestMethod]
		public void DigestDiffersByUserKey()
		{
			Key key1 = new(SuiteHelpers.ns, SuiteHelpers.set, "digest-a");
			Key key2 = new(SuiteHelpers.ns, SuiteHelpers.set, "digest-b");

			Assert.IsFalse(key1.Equals(key2));
		}

		[TestMethod]
		public void DigestDiffersByNamespace()
		{
			Key key1 = new(SuiteHelpers.ns, SuiteHelpers.set, "digest-ns");
			Key key2 = new("other-ns", SuiteHelpers.set, "digest-ns");

			Assert.IsFalse(key1.Equals(key2));
		}

		[TestMethod]
		public void ComputeDigestMatchesKey()
		{
			Key key = new(SuiteHelpers.ns, SuiteHelpers.set, 4242L);
			byte[] digest = Key.ComputeDigest(SuiteHelpers.set, key.userKey);

			CollectionAssert.AreEqual(key.digest, digest);
		}

		[TestMethod]
		public void KeyTypesProduceDigest()
		{
			Key stringKey = new(SuiteHelpers.ns, SuiteHelpers.set, "string-key");
			Key longKey = new(SuiteHelpers.ns, SuiteHelpers.set, 99L);
			Key bytesKey = new(SuiteHelpers.ns, SuiteHelpers.set, [1, 2, 3]);

			Assert.AreEqual(20, stringKey.digest.Length);
			Assert.AreEqual(20, longKey.digest.Length);
			Assert.AreEqual(20, bytesKey.digest.Length);
			Assert.IsFalse(stringKey.Equals(longKey));
		}

		[TestMethod]
		public void DigestKeyReadWrite()
		{
			Key userKey = new(SuiteHelpers.ns, SuiteHelpers.set, "digest-read");
			Bin bin = new(BinName, "digest-value");

			client.Put(null, userKey, bin);

			Key digestKey = new(SuiteHelpers.ns, userKey.digest, SuiteHelpers.set, null);
			Record record = client.Get(null, digestKey, BinName);

			AssertRecordFound(digestKey, record);
			Assert.AreEqual("digest-value", record.GetString(BinName));
		}

		[TestMethod]
		public void KeyEqualityAndHashCode()
		{
			Key key1 = new(SuiteHelpers.ns, SuiteHelpers.set, "eq-key");
			Key key2 = new(SuiteHelpers.ns, key1.digest, SuiteHelpers.set, null);

			Assert.IsTrue(key1.Equals(key2));
			Assert.AreEqual(key1.GetHashCode(), key2.GetHashCode());
		}
	}
}
