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
using System.Text.RegularExpressions;
using Aerospike.Client;
using Driver = Aerospike.L5Driver.L5Driver;

namespace Aerospike.Test
{
	/// <summary>
	/// The L5 driver's <c>large</c> command against a stub store. Needs no server.
	/// </summary>
	[TestClass]
	public class TestL5DriverLarge
	{
		private const int Bytes = 1 << 20;

		[TestMethod]
		public void WritesAndReadsRecordsOfTheGivenSize()
		{
			Dictionary<string, string> stored = new Dictionary<string, string>(StringComparer.Ordinal);
			Driver.Large large = Driver.Large.Of("mtu", Bytes.ToString(), "3");
			Assert.IsNotNull(large);

			large.Run("test", "l5",
				(key, value) => stored[(string)key.userKey.Object] = value,
				key => stored.TryGetValue((string)key.userKey.Object, out string v) ? v : null);

			Assert.AreEqual("mtu-0,mtu-1,mtu-2", string.Join(",", stored.Keys));

			foreach (KeyValuePair<string, string> entry in stored)
			{
				Assert.AreEqual(Bytes, entry.Value.Length);
				Assert.IsTrue(entry.Value.StartsWith(entry.Key, StringComparison.Ordinal));
			}

			Assert.IsTrue(Regex.IsMatch(large.Reply(7),
				"\\{\"type\":\"large\",\"t\":7,\"step\":\"mtu\",\"bytes\":1048576,\"keys\":3," +
				"\"written\":3,\"read\":3,\"mismatched\":0,\"failures\":0,\"firstFailure\":null," +
				"\"maxMs\":\\d+,\"elapsedMs\":\\d+\\}"));
		}

		[TestMethod]
		public void CountsMismatchesAndFailures()
		{
			Driver.Large large = Driver.Large.Of("s", "16", "3");
			Assert.IsNotNull(large);

			large.Run("test", "l5",
				(key, value) =>
				{
					if ("s-1".Equals(key.userKey.Object))
					{
						throw new AerospikeException("put refused");
					}
				},
				key => "s-0".Equals(key.userKey.Object) ? Driver.Large.Value("s-0", 16) : "other");

			Assert.IsTrue(Regex.IsMatch(large.Reply(9),
				"\\{\"type\":\"large\",\"t\":9,\"step\":\"s\",\"bytes\":16,\"keys\":3," +
				"\"written\":2,\"read\":1,\"mismatched\":1,\"failures\":1," +
				"\"firstFailure\":\"s-1 put: .*put refused\"," +
				"\"maxMs\":\\d+,\"elapsedMs\":\\d+\\}"));
		}

		[TestMethod]
		public void MalformedArgumentsAreRefused()
		{
			Assert.IsNull(Driver.Large.Of("s", "x", "1"));
			Assert.IsNull(Driver.Large.Of("s", "16", "0"));
			Assert.IsNull(Driver.Large.Of("s", "-1", "1"));
			Assert.IsNull(Driver.Large.Of("step", "5", "10"));
			Assert.IsNotNull(Driver.Large.Of("step", "6", "10"));
			Assert.AreEqual("step-9", Driver.Large.Value("step-9", 6));
		}
	}
}
