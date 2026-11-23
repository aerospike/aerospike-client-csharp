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
using System.Diagnostics;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Query run-time guardrails and order-by job cap.
	/// Live against a server that exposes the dynamic knobs; otherwise inconclusive.
	/// </summary>
	[TestClass]
	public class TestQueryGuardrails : TestSync
	{
		private const string SetPrefix = "qg";
		private const string ScoreBin = "score";
		private const int NRecords = 400;
		private const int TopK = 5;

		// A node applies the rate per query thread (default 4), so 400 records at
		// RPS 2 outlast the limits and timeouts below.
		private const int Rps = 2;

		private const string ServerRunTimeLimitName = "query-max-run-time-ms";
		private const string ServerTimeoutStat = "pi_query_long_basic_timeout";
		private const string ServerRunTimeLimitMs = "500";
		private const string ServerBlockerRunTimeLimitMs = "3000";
		private const string ServerSocketTimeoutRunTimeLimitMs = "1000";

		private const int TotalTimeoutMs = 800;
		private const int SocketTimeoutMs = 500;
		private const int SocketTimeoutMaxElapsedMs = 1500;
		private const int RefusedMaxElapsedMs = 1000;

		// Server builds have used both names for the Top-K concurrency cap.
		private static readonly string[] ServerJobsCapNames =
			["order-by-jobs-limit", "top-k-max-jobs"];

		private static void RequireCapableCluster() => RequireQueryOrderBySupport();

		//---------------------------------
		// Run-time limit (212)
		//---------------------------------

		/// <summary>
		/// A long plain query stopped by <c>query-max-run-time-ms</c> returns
		/// <see cref="ResultCode.QUERY_TIMEOUT"/> and is not retried (one timeout
		/// per node in the server statistic, not one per retry).
		/// </summary>
		[TestMethod]
		public void PlainQueryHitsQueryMaxRunTimeReturnsQueryTimeout()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerRunTimeLimitName, ServerRunTimeLimitMs, out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerRunTimeLimitName);
			}

			string set = SetPrefix + "Plain212";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				Statement stmt = Plain(set);
				ulong timeoutsBefore = SumNamespaceStat(ServerTimeoutStat);

				AerospikeException ae = Assert.Throws<AerospikeException>(() => DrainQuery(stmt));
				Assert.AreEqual(ResultCode.QUERY_TIMEOUT, ae.Result);

				Thread.Sleep(500);
				ulong timeouts = SumNamespaceStat(ServerTimeoutStat) - timeoutsBefore;
				Assert.IsTrue(timeouts >= 1, "expected at least one server long-query timeout");
				Assert.IsTrue(timeouts <= (ulong)client.Nodes.Length,
					"timeouts=" + timeouts + " exceeds cluster size (client retried?)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		/// <summary>
		/// AE-07: a Top-K query stopped by <c>query-max-run-time-ms</c> returns 212
		/// with no partial records, and is not retried.
		/// </summary>
		[TestMethod]
		public void TopKHitsQueryMaxRunTimeReturnsQueryTimeout()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerRunTimeLimitName, ServerRunTimeLimitMs, out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerRunTimeLimitName);
			}

			string set = SetPrefix + "TopK212";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				Statement stmt = Ordered(set, TopK);
				ulong timeoutsBefore = SumNamespaceStat(ServerTimeoutStat);

				AerospikeException ae = Assert.Throws<AerospikeException>(() => DrainQuery(stmt));
				Assert.AreEqual(ResultCode.QUERY_TIMEOUT, ae.Result,
					"guardrail deadline must surface as QUERY_TIMEOUT (212), not TIMEOUT (9)");

				Thread.Sleep(500);
				ulong timeouts = SumNamespaceStat(ServerTimeoutStat) - timeoutsBefore;
				Assert.IsTrue(timeouts >= 1, "expected at least one server long-query timeout");
				Assert.IsTrue(timeouts <= (ulong)client.Nodes.Length,
					"timeouts=" + timeouts + " exceeds cluster size (client retried?)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		/// <summary>
		/// A scan stopped by <c>query-max-run-time-ms</c> returns 212 and is not retried.
		/// </summary>
		[TestMethod]
		public void ScanHitsQueryMaxRunTimeReturnsQueryTimeout()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerRunTimeLimitName, ServerRunTimeLimitMs, out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerRunTimeLimitName);
			}

			string set = SetPrefix + "Scan212";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				ulong timeoutsBefore = SumNamespaceStat(ServerTimeoutStat);

				AerospikeException ae = Assert.Throws<AerospikeException>(() => DrainScan(set));
				Assert.AreEqual(ResultCode.QUERY_TIMEOUT, ae.Result);

				Thread.Sleep(500);
				ulong timeouts = SumNamespaceStat(ServerTimeoutStat) - timeoutsBefore;
				Assert.IsTrue(timeouts >= 1, "expected at least one server long-query timeout");
				Assert.IsTrue(timeouts <= (ulong)client.Nodes.Length,
					"timeouts=" + timeouts + " exceeds cluster size (client retried?)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		//---------------------------------
		// Total timeout
		//---------------------------------

		/// <summary>
		/// A long plain query that passes its total timeout returns a timeout
		/// without running on. No server run-time limit is set.
		/// </summary>
		[TestMethod]
		public void PlainQueryTotalTimeoutReturnsTimeout()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerRunTimeLimitName, "0", out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerRunTimeLimitName);
			}

			string set = SetPrefix + "PlainTT";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				QueryPolicy policy = new()
				{
					totalTimeout = TotalTimeoutMs
				};
				Statement stmt = Plain(set);

				Stopwatch sw = Stopwatch.StartNew();
				AerospikeException ae = Assert.Throws<AerospikeException>(() => DrainQuery(stmt, policy));
				sw.Stop();

				Assert.IsTrue(
					ae.Result == ResultCode.TIMEOUT || ae.Result == ResultCode.QUERY_TIMEOUT,
					"expected TIMEOUT or QUERY_TIMEOUT, got " + ae.Result);
				Assert.IsTrue(sw.ElapsedMilliseconds < TotalTimeoutMs * 3,
					"elapsed " + sw.ElapsedMilliseconds + " ms exceeds 3× total timeout");
			}
			finally
			{
				TrySetNamespaceConfig(ServerRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		/// <summary>
		/// A Top-K query that passes its total timeout returns a timeout with no
		/// records. Server may surface 9 (client) or 212 (server deadline from
		/// the client's total timeout).
		/// </summary>
		[TestMethod]
		public void TopKTotalTimeoutReturnsTimeout()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerRunTimeLimitName, "0", out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerRunTimeLimitName);
			}

			string set = SetPrefix + "TopKTT";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				QueryPolicy policy = new()
				{
					totalTimeout = TotalTimeoutMs
				};
				Statement stmt = Ordered(set, TopK);

				Stopwatch sw = Stopwatch.StartNew();
				AerospikeException ae = Assert.Throws<AerospikeException>(() => DrainQuery(stmt, policy));
				sw.Stop();

				Assert.IsTrue(
					ae.Result == ResultCode.TIMEOUT || ae.Result == ResultCode.QUERY_TIMEOUT,
					"expected TIMEOUT or QUERY_TIMEOUT, got " + ae.Result);
				Assert.IsTrue(sw.ElapsedMilliseconds < TotalTimeoutMs * 3,
					"elapsed " + sw.ElapsedMilliseconds + " ms exceeds 3× total timeout");
			}
			finally
			{
				TrySetNamespaceConfig(ServerRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		//---------------------------------
		// Top-K socket timeout (no retry)
		//---------------------------------

		/// <summary>
		/// A Top-K query that exceeds its socket timeout returns
		/// <see cref="ResultCode.TIMEOUT"/> and is not retried. A large
		/// <c>sleepBetweenRetries</c> would make any retry exceed the elapsed bound.
		/// </summary>
		[TestMethod]
		public void TopKSocketTimeoutNotRetried()
		{
			RequireCapableCluster();
			// Keep a server deadline above the socket timeout so the client gives
			// up first; without it a hung-up Top-K job can hold a cap slot for seconds.
			if (!TrySetNamespaceConfig(ServerRunTimeLimitName, ServerSocketTimeoutRunTimeLimitMs,
				out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerRunTimeLimitName);
			}

			string set = SetPrefix + "SockTO";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				QueryPolicy policy = new()
				{
					socketTimeout = SocketTimeoutMs,
					// Any partition-tracker retry would sleep this long first.
					sleepBetweenRetries = SocketTimeoutMs * 10,
					maxRetries = 5
				};
				Statement stmt = Ordered(set, TopK);

				Stopwatch sw = Stopwatch.StartNew();
				AerospikeException ae = Assert.Throws<AerospikeException>(() => DrainQuery(stmt, policy));
				sw.Stop();

				Assert.AreEqual(ResultCode.TIMEOUT, ae.Result);
				Assert.IsTrue(sw.ElapsedMilliseconds < SocketTimeoutMaxElapsedMs,
					"elapsed " + sw.ElapsedMilliseconds +
					" ms suggests a retry (expected under " + SocketTimeoutMaxElapsedMs + " ms)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		//---------------------------------
		// Top-K job cap (22)
		//---------------------------------

		/// <summary>
		/// AE-08: a Top-K query refused by the job cap returns
		/// <see cref="ResultCode.FAIL_FORBIDDEN"/> with the server's reason and
		/// is not retried.
		/// </summary>
		[TestMethod]
		public void ConcurrentTopKHitsOrderByJobsLimitReturnsForbidden()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerRunTimeLimitName, ServerBlockerRunTimeLimitMs,
				out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerRunTimeLimitName);
			}

			if (!TrySetJobsCap("1", out string jobsCapName, out string restoreCap))
			{
				TrySetNamespaceConfig(ServerRunTimeLimitName, restoreMs, out _);
				Assert.Inconclusive("Server does not expose a Top-K job cap setting");
			}

			string set = SetPrefix + "JobsCap";
			Key[] keys = SeedScores(set, NRecords);
			Exception blockerError = null;
			try
			{
				// Let any prior Top-K job finish before taking the only slot.
				Thread.Sleep(1000);

				Statement blockerStmt = Ordered(set, TopK);
				Thread blocker = new(() =>
				{
					try
					{
						DrainQuery(blockerStmt);
					}
					catch (Exception e)
					{
						blockerError = e;
					}
				});
				blocker.Start();
				Thread.Sleep(500);

				QueryPolicy refusedPolicy = new()
				{
					errorDetailVerbosity = 2,
					sleepBetweenRetries = 500,
					maxRetries = 5
				};
				Statement refusedStmt = Ordered(set, TopK);

				Stopwatch sw = Stopwatch.StartNew();
				AerospikeException ae = Assert.Throws<AerospikeException>(
					() => DrainQuery(refusedStmt, refusedPolicy));
				sw.Stop();

				blocker.Join(TimeSpan.FromMinutes(2));

				Assert.AreEqual(ResultCode.FAIL_FORBIDDEN, ae.Result,
					"expected FAIL_FORBIDDEN (22) from " + jobsCapName + "; blocker=" +
					Describe(blockerError));
				Assert.IsTrue(
					ae.BaseMessage.Contains("retry later", StringComparison.OrdinalIgnoreCase) ||
					ae.Message.Contains("retry later", StringComparison.OrdinalIgnoreCase),
					"expected server 'retry later' detail; got: " + ae.BaseMessage);
				Assert.IsTrue(sw.ElapsedMilliseconds < RefusedMaxElapsedMs,
					"elapsed " + sw.ElapsedMilliseconds +
					" ms suggests a retry (expected under " + RefusedMaxElapsedMs + " ms)");
			}
			finally
			{
				TrySetServiceConfig(jobsCapName, restoreCap, out _);
				TrySetNamespaceConfig(ServerRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		//---------------------------------
		// Helpers
		//---------------------------------

		private static Statement Plain(string set)
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(set);
			stmt.SetBinNames(ScoreBin);
			stmt.SetRecordsPerSecond(Rps);
			return stmt;
		}

		private static Statement Ordered(string set, int k)
		{
			Statement stmt = Plain(set);
			stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(k);
			return stmt;
		}

		private static int DrainQuery(Statement stmt, QueryPolicy policy = null)
		{
			int count = 0;
			using RecordSet rs = client.Query(policy, stmt);
			while (rs.Next())
			{
				count++;
			}
			return count;
		}

		private static void DrainScan(string set)
		{
			ScanPolicy policy = new()
			{
				recordsPerSecond = Rps
			};
			client.ScanAll(policy, SuiteHelpers.ns, set, (_, _) => { });
		}

		private static Key[] SeedScores(string set, int n)
		{
			Key[] keys = new Key[n];
			for (int i = 0; i < n; i++)
			{
				keys[i] = new Key(SuiteHelpers.ns, set, "k" + i);
				client.Put(null, keys[i], new Bin(ScoreBin, i));
			}
			return keys;
		}

		private static void DeleteAll(Key[] keys)
		{
			foreach (Key key in keys)
			{
				try { client.Delete(null, key); }
				catch (AerospikeException) { }
			}
		}

		private static string Describe(Exception e)
		{
			if (e is AerospikeException ae)
			{
				return ae.Result + " " + ae.BaseMessage;
			}
			return e == null ? "(none)" : e.GetType().Name + ": " + e.Message;
		}

		/// <summary>
		/// Sum a namespace statistic over every node (best-effort; missing stats count as 0).
		/// </summary>
		private static ulong SumNamespaceStat(string name)
		{
			ulong sum = 0;
			string req = "namespace/" + SuiteHelpers.ns;
			foreach (Node node in client.Nodes)
			{
				try
				{
					string res = Info.Request(null, node, req);
					if (res == null)
					{
						continue;
					}
					foreach (string part in res.Split(';'))
					{
						string[] kv = part.Split('=', 2);
						if (kv.Length == 2 && kv[0] == name &&
							ulong.TryParse(kv[1], out ulong value))
						{
							sum += value;
							break;
						}
					}
				}
				catch (AerospikeException)
				{
					// ignore node failures when summing
				}
			}
			return sum;
		}

		/// <summary>
		/// Set the Top-K / order-by jobs cap, trying each known config name.
		/// </summary>
		private static bool TrySetJobsCap(string value, out string nameUsed, out string previous)
		{
			nameUsed = null;
			previous = "0";
			foreach (string name in ServerJobsCapNames)
			{
				if (TrySetServiceConfig(name, value, out previous))
				{
					nameUsed = name;
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Set a service-context dynamic config on every node. Returns false when the
		/// knob is unknown (older servers). <paramref name="previous"/> is best-effort
		/// ("0" when unread) for restore in finally.
		/// </summary>
		private static bool TrySetServiceConfig(string name, string value, out string previous)
		{
			previous = "0";
			try
			{
				string get = Info.Request(null, client.Nodes[0], "get-config:context=service");
				if (!TryReadConfigValue(get, name, out previous))
				{
					return false;
				}

				string cmd = "set-config:context=service;" + name + "=" + value;
				return ApplyConfigOnAllNodes(cmd);
			}
			catch (AerospikeException)
			{
				return false;
			}
		}

		private static bool TrySetNamespaceConfig(string name, string value, out string previous)
		{
			previous = "0";
			string ns = SuiteHelpers.ns;
			try
			{
				string get = Info.Request(null, client.Nodes[0],
					"get-config:context=namespace;id=" + ns);
				if (!TryReadConfigValue(get, name, out previous))
				{
					return false;
				}

				string cmd = "set-config:context=namespace;id=" + ns + ";" + name + "=" + value;
				return ApplyConfigOnAllNodes(cmd);
			}
			catch (AerospikeException)
			{
				return false;
			}
		}

		private static bool TryReadConfigValue(string getConfigResponse, string name, out string previous)
		{
			previous = "0";
			if (getConfigResponse == null)
			{
				return false;
			}

			bool found = false;
			foreach (string part in getConfigResponse.Split(';'))
			{
				string[] kv = part.Split('=', 2);
				if (kv.Length == 2 && kv[0] == name)
				{
					previous = kv[1];
					found = true;
					break;
				}
			}
			return found;
		}

		private static bool ApplyConfigOnAllNodes(string command)
		{
			foreach (Node node in client.Nodes)
			{
				string response = Info.Request(null, node, command);
				if (response == null ||
					response.Contains("error", StringComparison.OrdinalIgnoreCase) ||
					response.Contains("fail", StringComparison.OrdinalIgnoreCase))
				{
					return false;
				}
			}
			return true;
		}
	}
}
