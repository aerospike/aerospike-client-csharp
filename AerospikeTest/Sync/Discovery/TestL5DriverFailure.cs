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
using Driver = Aerospike.L5Driver.L5Driver;

namespace Aerospike.Test
{
	/// <summary>
	/// The L5 driver's failure and ready lines. Needs no server.
	/// </summary>
	[TestClass]
	public class TestL5DriverFailure
	{
		private static readonly Regex Failure = new Regex(
			"^\\{\"type\":\"failure\",\"t\":(\\d+),\"startedAt\":(\\d+),\"message\":\"(.*)\"\\}$",
			RegexOptions.CultureInvariant);

		[TestMethod]
		public void FailureHasTypeTimesAndMessage()
		{
			long startedAt = Driver.Now();
			long t = Driver.Now();
			string line = Driver.FailureLine(t, startedAt, "Error -11,1,0,0,0,0: Max retries exceeded: 0");
			Match m = Failure.Match(line);

			Assert.IsTrue(m.Success, line);
			Assert.AreEqual(t, long.Parse(m.Groups[1].Value));
			Assert.AreEqual(startedAt, long.Parse(m.Groups[2].Value));
			Assert.IsTrue(long.Parse(m.Groups[2].Value) <= long.Parse(m.Groups[1].Value));
			Assert.AreEqual("Error -11,1,0,0,0,0: Max retries exceeded: 0", m.Groups[3].Value);
		}

		[TestMethod]
		public void FailureOnlyAfterReady()
		{
			using StringWriter writer = new StringWriter();
			Driver.Output output = new Driver.Output(writer);
			string ready = Driver.ReadyLine(5, 1700000000000L, "8.5.1", "port-map");

			output.WriteIfOpen(Driver.FailureLine(3, 1, "before"));
			Assert.AreEqual("", writer.ToString());

			output.Open(ready);
			output.WriteIfOpen(Driver.FailureLine(9, 6, "after"));
			Assert.AreEqual(ready + "\n" + Driver.FailureLine(9, 6, "after") + "\n", writer.ToString());
		}

		[TestMethod]
		public void ReadyReportsContract3()
		{
			Assert.AreEqual(
				"{\"type\":\"ready\",\"t\":5,\"contract\":3,\"epochMs\":1700000000000,\"client\":\"csharp\"," +
				"\"clientVersion\":\"8.5.1\",\"shape\":\"port-map\"}",
				Driver.ReadyLine(5, 1700000000000L, "8.5.1", "port-map"));
		}
	}
}
