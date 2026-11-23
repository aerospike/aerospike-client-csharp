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
using System.Collections.Concurrent;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// CLIENT-5996 / AE-07 / AE-08: query-run-time guardrails and order-by job cap.
	/// Live against a server that exposes the dynamic knobs; otherwise inconclusive.
	/// </summary>
	[TestClass]
	public class TestQueryGuardrails : TestSync
	{
		private const string Set = "qgTopK";
		private const string ScoreBin = "score";

		private static void RequireCapableCluster() => RequireQueryOrderBySupport();

		/// <summary>
		/// AE-07: with <c>query-max-run-time-ms</c> set low, a Top-K query fails with
		/// <see cref="ResultCode.QUERY_TIMEOUT"/> (212) and no partial records. The
		/// partition tracker does not retry 212 (see <see cref="TestTopKClientValidation"/>).
		/// </summary>
		[TestMethod]
		public void TopKHitsQueryMaxRunTimeReturnsQueryTimeout()
		{
			RequireCapableCluster();
			if (!TrySetNamespaceConfig("query-max-run-time-ms", "1", out string restoreMs))
			{
				Assert.Inconclusive("Server does not expose query-max-run-time-ms (SERVER-2095)");
			}

			string set = Set + "Ae07";
			Key[] keys = SeedScores(set, 400);
			try
			{
				Statement stmt = Ordered(set, 50);
				AerospikeException ae = Assert.Throws<AerospikeException>(
					() => Drain(stmt));
				Assert.AreEqual(ResultCode.QUERY_TIMEOUT, ae.Result,
					"guardrail deadline must surface as QUERY_TIMEOUT (212), not TIMEOUT (9)");
			}
			finally
			{
				TrySetNamespaceConfig("query-max-run-time-ms", restoreMs, out _);
				DeleteAll(keys);
			}
		}

		/// <summary>
		/// AE-08: with <c>order-by-jobs-limit</c> = 1, a second concurrent Top-K is refused
		/// with <see cref="ResultCode.FAIL_FORBIDDEN"/> (22). The client does not backoff-retry.
		/// </summary>
		[TestMethod]
		public void ConcurrentTopKHitsOrderByJobsLimitReturnsForbidden()
		{
			RequireCapableCluster();
			if (!TrySetServiceConfig("order-by-jobs-limit", "1", out string restoreLimit))
			{
				Assert.Inconclusive("Server does not expose order-by-jobs-limit (SERVER-2095)");
			}

			string set = Set + "Ae08";
			Key[] keys = SeedScores(set, 800);
			ConcurrentBag<AerospikeException> errors = new();
			Barrier start = new(2);
			try
			{
				Thread t1 = new(() => RunTopK(Ordered(set, 100), start, errors));
				Thread t2 = new(() => RunTopK(Ordered(set, 100), start, errors));
				t1.Start();
				t2.Start();
				t1.Join(TimeSpan.FromMinutes(2));
				t2.Join(TimeSpan.FromMinutes(2));

				Assert.IsTrue(errors.Any(e => e.Result == ResultCode.FAIL_FORBIDDEN),
					"expected at least one FAIL_FORBIDDEN (22) from order-by-jobs-limit; got: " +
					string.Join("; ", errors.Select(e => e.Result + " " + e.BaseMessage)));
			}
			finally
			{
				TrySetServiceConfig("order-by-jobs-limit", restoreLimit, out _);
				DeleteAll(keys);
			}
		}

		private static void RunTopK(
			Statement stmt, Barrier start, ConcurrentBag<AerospikeException> errors)
		{
			start.SignalAndWait();
			try
			{
				Drain(stmt);
			}
			catch (AerospikeException ae)
			{
				errors.Add(ae);
			}
		}

		private static void Drain(Statement stmt)
		{
			using RecordSet rs = client.Query(null, stmt);
			while (rs.Next())
			{
				// consume
			}
		}

		private static Statement Ordered(string set, int k)
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(set);
			stmt.SetBinNames(ScoreBin);
			stmt.SetOrderBy(ScoreBin, BinDataType.INTEGER, Order.DESC);
			stmt.SetTopK(k);
			return stmt;
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
				if (get != null)
				{
					foreach (string part in get.Split(';'))
					{
						string[] kv = part.Split('=', 2);
						if (kv.Length == 2 && kv[0] == name)
						{
							previous = kv[1];
							break;
						}
					}
				}

				string cmd = "set-config:context=service;" + name + "=" + value;
				foreach (Node node in client.Nodes)
				{
					string response = Info.Request(null, node, cmd);
					if (response == null ||
						response.Contains("error", StringComparison.OrdinalIgnoreCase) ||
						response.Contains("fail", StringComparison.OrdinalIgnoreCase))
					{
						return false;
					}
				}
				return true;
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
				string get = Info.Request(null, client.Nodes[0], "get-config:context=namespace;id=" + ns);
				if (get != null)
				{
					foreach (string part in get.Split(';'))
					{
						string[] kv = part.Split('=', 2);
						if (kv.Length == 2 && kv[0] == name)
						{
							previous = kv[1];
							break;
						}
					}
				}

				string cmd = "set-config:context=namespace;id=" + ns + ";" + name + "=" + value;
				foreach (Node node in client.Nodes)
				{
					string response = Info.Request(null, node, cmd);
					if (response == null ||
						response.Contains("error", StringComparison.OrdinalIgnoreCase) ||
						response.Contains("fail", StringComparison.OrdinalIgnoreCase))
					{
						return false;
					}
				}
				return true;
			}
			catch (AerospikeException)
			{
				return false;
			}
		}
	}
}
