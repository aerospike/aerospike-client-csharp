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
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Address translator Close() is called once after tend stops or when construction fails.
	/// </summary>
	[TestClass]
	public class TestAddressTranslatorClose
	{
		private static readonly Host DeadSeed = new Host("127.0.0.1", 1);

		[TestMethod]
		public void CloseCalledOnceAfterTendStops()
		{
			CloseCountingTranslator translator = new CloseCountingTranslator();
			ClientPolicy policy = Policy(translator);
			AerospikeClient client = new AerospikeClient(policy, DeadSeed);

			try
			{
				Assert.AreEqual(0, translator.Closes);
				Thread tend = TendThread(client.Cluster);
				Assert.IsNotNull(tend);
				client.Close();
				Await(() => translator.Closes == 1);
				Assert.AreEqual(1, translator.Closes);
				tend.Join(TimeSpan.FromSeconds(10));
				Assert.IsFalse(tend.IsAlive);
				client.Close();
				Thread.Sleep(200);
				Assert.AreEqual(1, translator.Closes);
			}
			finally
			{
				try { client.Close(); } catch { }
			}
		}

		[TestMethod]
		public void CloseCalledOnceWhenConstructionFails()
		{
			CloseCountingTranslator translator = new CloseCountingTranslator();
			ClientPolicy policy = Policy(translator);
			policy.failIfNotConnected = true;

			Assert.Throws<AerospikeException>(() => new AerospikeClient(policy, DeadSeed));
			Assert.AreEqual(1, translator.Closes);

			translator = new CloseCountingTranslator();
			policy = Policy(translator);
			policy.forceSingleNode = true;

			Assert.Throws<AerospikeException>(() => new AerospikeClient(policy, DeadSeed));
			Assert.AreEqual(1, translator.Closes);
		}

		[TestMethod]
		public void CloseExceptionDoesNotStopShutdown()
		{
			ConcurrentBag<string> warnings = new ConcurrentBag<string>();
			Log.SetLevel(Log.Level.WARN);
			Log.SetCallback((level, message) =>
			{
				if (level == Log.Level.WARN)
				{
					warnings.Add(message);
				}
			});

			try
			{
				CloseCountingTranslator translator = new CloseCountingTranslator
				{
					CloseError = new InvalidOperationException("close boom")
				};
				ClientPolicy policy = Policy(translator);
				AerospikeClient client = new AerospikeClient(policy, DeadSeed);
				Thread tend = TendThread(client.Cluster);
				client.Close();
				Await(() => translator.Closes == 1);
				tend?.Join(TimeSpan.FromSeconds(10));
				Assert.IsTrue(warnings.Any(m =>
					m.Contains("close failed") && m.Contains("close boom")),
					string.Join("; ", warnings));
			}
			finally
			{
				Log.SetCallback(null);
				Log.SetLevel(Log.Level.INFO);
			}
		}

		private static ClientPolicy Policy(IAddressTranslator translator)
		{
			return new ClientPolicy
			{
				failIfNotConnected = false,
				timeout = 100,
				tendInterval = 250,
				addressTranslator = translator
			};
		}

		private static Thread TendThread(Cluster cluster)
		{
			FieldInfo field = typeof(Cluster).GetField("tendThread",
				BindingFlags.Instance | BindingFlags.NonPublic);
			return (Thread)field.GetValue(cluster);
		}

		private static void Await(Func<bool> condition)
		{
			long deadline = Environment.TickCount64 + 10000;

			while (!condition())
			{
				Assert.IsTrue(Environment.TickCount64 < deadline, "condition not met in time");
				Thread.Sleep(5);
			}
		}

		private sealed class CloseCountingTranslator : IAddressTranslator
		{
			public int Closes;
			public Exception CloseError;

			public Host Translate(Endpoint advertised) => null;

			public void Close()
			{
				Interlocked.Increment(ref Closes);

				if (CloseError != null)
				{
					throw CloseError;
				}
			}
		}
	}
}
