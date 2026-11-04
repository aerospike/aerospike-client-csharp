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
	/// Server certificate name matching against a TLS-enabled server whose certificate shape is
	/// chosen per run by AerospikeTest/tls/run_tls_matching.sh. Case parameters arrive as
	/// environment variables: TLS_CASE, TLS_NAME, TLS_EXPECT, TLS_WARNING, TLS_REVOKE,
	/// TLS_CA, TLS_HOST, TLS_PORT, TLS_NAMESPACE.
	/// </summary>
	[TestClass]
	public class TestTlsMatching
	{
		private const int TimeoutMs = 5000;

		private static readonly ConcurrentQueue<string> Warnings = new ConcurrentQueue<string>();

		private static string caseName;
		private static string tlsName;
		private static bool expectAccept;
		private static bool expectWarning;
		private static string revokeHex;
		private static string hostName;
		private static int port;
		private static string ns;

		[ClassInitialize]
		public static void ClassInit(TestContext context)
		{
			caseName = Required("TLS_CASE");
			tlsName = Required("TLS_NAME");
			expectAccept = Required("TLS_EXPECT").Equals("accept", StringComparison.Ordinal);
			expectWarning = bool.Parse(Required("TLS_WARNING"));
			revokeHex = Environment.GetEnvironmentVariable("TLS_REVOKE") ?? "";
			hostName = Environment.GetEnvironmentVariable("TLS_HOST") ?? "127.0.0.1";
			port = int.Parse(Required("TLS_PORT"));
			ns = Environment.GetEnvironmentVariable("TLS_NAMESPACE") ?? "test";

			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN)
				{
					Warnings.Enqueue(message);
				}
				Console.WriteLine("CLIENT-LOG " + level + ' ' + message);
			});
		}

		[ClassCleanup]
		public static void ClassCleanup()
		{
			Log.SetCallback(null);
		}

		[TestMethod]
		public void Matching()
		{
			Outcome outcome = RunSync();
			long warnCount = WarningCount();

			Console.WriteLine("TLS-RESULT case=" + caseName + " tlsName=" + tlsName +
				" expected=" + (expectAccept ? "accept" : "reject") +
				" sync=" + outcome +
				" warningExpected=" + expectWarning +
				" warningsAfterClient=" + warnCount);

			if (expectAccept)
			{
				Assert.IsTrue(outcome.Accepted, "expected accept: " + outcome);
			}
			else
			{
				// SslStream surfaces name/serial rejects as AuthenticationException (often wrapped as
				// AerospikeException.Connection). Match L2: require a failed handshake, not Java's
				// exact "Invalid TLS name:" / non-retryable shape.
				Assert.IsFalse(outcome.Accepted, "expected reject: " + outcome);
				Assert.IsNotNull(outcome.Error, "expected handshake exception: " + outcome);
			}

			Assert.AreEqual(expectWarning ? 1L : 0L, warnCount, "legacy-match warnings");
		}

		private static Outcome RunSync()
		{
			Outcome outcome = new Outcome();
			TlsPolicy tp = new TlsPolicy();

			if (!string.IsNullOrEmpty(revokeHex))
			{
				tp.revokeCertificates = Util.HexStringToByteArrays(revokeHex);
			}

			ClientPolicy cp = new ClientPolicy
			{
				tlsPolicy = tp,
				timeout = TimeoutMs,
				failIfNotConnected = true
			};

			try
			{
				using (AerospikeClient client = new AerospikeClient(cp, new Host(hostName, tlsName, port)))
				{
					Key key = new Key(ns, "tls", caseName);
					client.Put(null, key, new Bin("v", caseName));
					Record rec = client.Get(null, key);
					outcome.Accepted = rec != null && caseName.Equals(rec.GetString("v"));
				}
			}
			catch (Exception e)
			{
				outcome.Error = e;
			}

			return outcome;
		}

		private static long WarningCount()
		{
			string quoted = "'" + tlsName + "'";
			long n = 0;

			foreach (string m in Warnings)
			{
				if (m.Contains(quoted))
				{
					n++;
				}
			}

			return n;
		}

		private static string Required(string name)
		{
			string value = Environment.GetEnvironmentVariable(name);

			if (string.IsNullOrEmpty(value))
			{
				Assert.Inconclusive(name + " is not set. Run AerospikeTest/tls/run_tls_matching.sh");
			}

			return value;
		}

		private sealed class Outcome
		{
			public bool Accepted;
			public Exception Error;

			public override string ToString()
			{
				return "accepted=" + Accepted + " error=" + Error;
			}
		}
	}
}
