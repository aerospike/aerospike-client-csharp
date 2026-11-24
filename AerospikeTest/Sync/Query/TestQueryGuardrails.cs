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

		// The rate keeps a query running past the limits and timeouts below. With
		// 400 records, about 5 s on one node and about 2 s per node on three.
		private const int Rps = 2;

		private const string ServerQueryRunTimeLimitName = "query-max-run-time-ms";
		private const string ServerTimeoutStat = "pi_query_long_basic_timeout";
		private const string ServerTopKJobsCapName = "order-by-jobs-limit";
		private const string ServerRefusedStat = "order_by_jobs_refused";

		private const string ServerRunTimeLimitMs = "500";
		private const string ServerBlockerRunTimeLimitMs = "3000";
		// Above the socket timeout so the client gives up first; without a
		// deadline a hung-up Top-K job can hold a cap slot for seconds.
		private const string ServerAbandonedJobRunTimeLimitMs = "1000";

		private const int TotalTimeoutMs = 800;
		// A retry that restarts the total timeout runs another full timeout, so
		// the query must end less than half of that past the first one.
		private const int TotalTimeoutMaxElapsedMs = TotalTimeoutMs * 3 / 2;

		private const int SocketTimeoutMs = 500;
		// A retry waits for another socket timeout, so the query must end less
		// than half of that past the first one.
		private const int SocketTimeoutMaxElapsedMs = SocketTimeoutMs * 3 / 2;

		private static void RequireCapableCluster() => RequireQueryOrderBySupport();

		//---------------------------------
		// Run-time limit (212)
		//---------------------------------

		/// <summary>
		/// A long plain query stopped by <c>query-max-run-time-ms</c> returns
		/// <see cref="ResultCode.QUERY_TIMEOUT"/> and is not retried. Each node
		/// that runs the query counts one timeout.
		/// </summary>
		[TestMethod]
		public void PlainQueryHitsQueryMaxRunTimeReturnsQueryTimeout()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerQueryRunTimeLimitName, ServerRunTimeLimitMs,
				out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerQueryRunTimeLimitName);
			}

			string set = SetPrefix + "Plain212";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				ulong timeouts = RunAndCountServerTimeouts(() => DrainQuery(Plain(set)));
				Assert.AreEqual((ulong)client.Nodes.Length, timeouts,
					"expected one server timeout per node (no retry)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerQueryRunTimeLimitName, restoreMs, out _);
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
			if (!TrySetNamespaceConfig(ServerQueryRunTimeLimitName, ServerRunTimeLimitMs,
				out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerQueryRunTimeLimitName);
			}

			string set = SetPrefix + "TopK212";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				ulong timeouts = RunAndCountServerTimeouts(() => DrainQuery(Ordered(set, TopK)));
				Assert.AreEqual((ulong)client.Nodes.Length, timeouts,
					"expected one server timeout per node (no retry)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerQueryRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		/// <summary>
		/// A scan stopped by <c>query-max-run-time-ms</c> returns 212 and is not
		/// retried. A scan runs one node at a time and stops at the first timeout,
		/// so the servers count one.
		/// </summary>
		[TestMethod]
		public void ScanHitsQueryMaxRunTimeReturnsQueryTimeout()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerQueryRunTimeLimitName, ServerRunTimeLimitMs,
				out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerQueryRunTimeLimitName);
			}

			string set = SetPrefix + "Scan212";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				ulong timeouts = RunAndCountServerTimeouts(() => DrainScan(set));
				Assert.AreEqual(1UL, timeouts, "expected one server timeout for a scan (no retry)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerQueryRunTimeLimitName, restoreMs, out _);
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
			if (!TrySetNamespaceConfig(ServerQueryRunTimeLimitName, "0", out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerQueryRunTimeLimitName);
			}

			string set = SetPrefix + "PlainTT";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				QueryPolicy policy = new() { totalTimeout = TotalTimeoutMs };
				Stopwatch sw = Stopwatch.StartNew();
				AerospikeException ae = Assert.Throws<AerospikeException>(
					() => DrainQuery(Plain(set), policy));
				sw.Stop();

				Assert.IsTrue(
					ae.Result == ResultCode.TIMEOUT || ae.Result == ResultCode.QUERY_TIMEOUT,
					"expected TIMEOUT or QUERY_TIMEOUT, got " + ae.Result);
				Assert.IsTrue(sw.ElapsedMilliseconds < TotalTimeoutMaxElapsedMs,
					"elapsed " + sw.ElapsedMilliseconds +
					" ms suggests a retry (expected under " + TotalTimeoutMaxElapsedMs + " ms)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerQueryRunTimeLimitName, restoreMs, out _);
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
			if (!TrySetNamespaceConfig(ServerQueryRunTimeLimitName, "0", out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerQueryRunTimeLimitName);
			}

			string set = SetPrefix + "TopKTT";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				QueryPolicy policy = new() { totalTimeout = TotalTimeoutMs };
				Stopwatch sw = Stopwatch.StartNew();
				AerospikeException ae = Assert.Throws<AerospikeException>(
					() => DrainQuery(Ordered(set, TopK), policy));
				sw.Stop();

				Assert.IsTrue(
					ae.Result == ResultCode.TIMEOUT || ae.Result == ResultCode.QUERY_TIMEOUT,
					"expected TIMEOUT or QUERY_TIMEOUT, got " + ae.Result);
				Assert.IsTrue(sw.ElapsedMilliseconds < TotalTimeoutMaxElapsedMs,
					"elapsed " + sw.ElapsedMilliseconds +
					" ms suggests a retry (expected under " + TotalTimeoutMaxElapsedMs + " ms)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerQueryRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		//---------------------------------
		// Top-K socket timeout (no retry)
		//---------------------------------

		/// <summary>
		/// A Top-K query that exceeds its socket timeout returns
		/// <see cref="ResultCode.TIMEOUT"/> and is not retried. A retry waits for
		/// another socket timeout, so the elapsed bound is 1.5× the first one.
		/// </summary>
		[TestMethod]
		public void TopKSocketTimeoutNotRetried()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerQueryRunTimeLimitName, ServerAbandonedJobRunTimeLimitMs,
				out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerQueryRunTimeLimitName);
			}

			string set = SetPrefix + "SockTO";
			Key[] keys = SeedScores(set, NRecords);
			try
			{
				QueryPolicy policy = new() { socketTimeout = SocketTimeoutMs };
				Stopwatch sw = Stopwatch.StartNew();
				AerospikeException ae = Assert.Throws<AerospikeException>(
					() => DrainQuery(Ordered(set, TopK), policy));
				sw.Stop();

				Assert.AreEqual(ResultCode.TIMEOUT, ae.Result);
				Assert.IsTrue(sw.ElapsedMilliseconds < SocketTimeoutMaxElapsedMs,
					"elapsed " + sw.ElapsedMilliseconds +
					" ms suggests a retry (expected under " + SocketTimeoutMaxElapsedMs + " ms)");
			}
			finally
			{
				TrySetNamespaceConfig(ServerQueryRunTimeLimitName, restoreMs, out _);
				DeleteAll(keys);
			}
		}

		//---------------------------------
		// Top-K job cap (22)
		//---------------------------------

		/// <summary>
		/// AE-08: a Top-K query refused by the job cap returns
		/// <see cref="ResultCode.FAIL_FORBIDDEN"/> and is not retried. Each node
		/// that refuses counts one in <c>order_by_jobs_refused</c>; a retry
		/// raises the count.
		/// </summary>
		[TestMethod]
		public void ConcurrentTopKHitsOrderByJobsLimitReturnsForbidden()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig(ServerQueryRunTimeLimitName, ServerBlockerRunTimeLimitMs,
				out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose " + ServerQueryRunTimeLimitName);
			}

			if (!TrySetServiceConfig(ServerTopKJobsCapName, "1", out string restoreCap))
			{
				TrySetNamespaceConfig(ServerQueryRunTimeLimitName, restoreMs, out _);
				Assert.Fail("Server exposes " + ServerQueryRunTimeLimitName +
					" but not " + ServerTopKJobsCapName);
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

				ulong refusedBefore = SumServiceStat(ServerRefusedStat);
				AerospikeException ae = Assert.Throws<AerospikeException>(
					() => DrainQuery(Ordered(set, TopK)));
				ulong nRefused = SumServiceStat(ServerRefusedStat) - refusedBefore;

				blocker.Join(TimeSpan.FromMinutes(2));

				Assert.AreEqual(ResultCode.FAIL_FORBIDDEN, ae.Result,
					"expected FAIL_FORBIDDEN (22); blocker=" + Describe(blockerError));
				Assert.AreEqual((ulong)client.Nodes.Length, nRefused,
					"expected one refusal per node (no retry); blocker=" + Describe(blockerError));
			}
			finally
			{
				TrySetServiceConfig(ServerTopKJobsCapName, restoreCap, out _);
				TrySetNamespaceConfig(ServerQueryRunTimeLimitName, restoreMs, out _);
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

		/// <summary>
		/// Run an action expected to throw QUERY_TIMEOUT and return how many
		/// long-query timeouts the servers counted for it.
		/// </summary>
		private static ulong RunAndCountServerTimeouts(Action action)
		{
			ulong timeoutsBefore = SumNamespaceStat(ServerTimeoutStat);
			AerospikeException ae = Assert.Throws<AerospikeException>(action);
			Assert.AreEqual(ResultCode.QUERY_TIMEOUT, ae.Result,
				"guardrail deadline must surface as QUERY_TIMEOUT (212), not TIMEOUT (9)");

			// A node may count the timeout after it has sent the error.
			Thread.Sleep(500);
			return SumNamespaceStat(ServerTimeoutStat) - timeoutsBefore;
		}

		private static void DrainQuery(Statement stmt, QueryPolicy policy = null)
		{
			using RecordSet rs = client.Query(policy, stmt);
			while (rs.Next())
			{
				// consume
			}
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

		private static ulong SumNamespaceStat(string name) =>
			SumStat("namespace/" + SuiteHelpers.ns, name);

		private static ulong SumServiceStat(string name) =>
			SumStat("statistics", name);

		/// <summary>
		/// Sum a statistic over every node (best-effort; missing stats count as 0).
		/// </summary>
		private static ulong SumStat(string command, string name)
		{
			ulong sum = 0;
			foreach (Node node in client.Nodes)
			{
				try
				{
					string res = Info.Request(null, node, command);
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
		/// Set a service-context dynamic config on every node. Returns false when the
		/// knob is unknown (older servers).
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

				return ApplyConfigOnAllNodes("set-config:context=service;" + name + "=" + value);
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

				return ApplyConfigOnAllNodes(
					"set-config:context=namespace;id=" + ns + ";" + name + "=" + value);
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

			foreach (string part in getConfigResponse.Split(';'))
			{
				string[] kv = part.Split('=', 2);
				if (kv.Length == 2 && kv[0] == name)
				{
					previous = kv[1];
					return true;
				}
			}
			return false;
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
