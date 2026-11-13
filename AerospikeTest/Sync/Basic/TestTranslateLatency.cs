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
	/// U-8 timing half: <c>translate()</c> p99 stays under 1 ms (NFR-7).
	/// Each translator is timed over <see cref="Rounds"/> rounds after a warm-up; the median of the
	/// round p99s must stay under 1 ms so a GC pause that spoils a few rounds cannot fail the test,
	/// while a translator that is really slow fails every round.
	/// </summary>
	[TestClass]
	public class TestTranslateLatency
	{
		private const int Warmup = 20_000;
		private const int Rounds = 7;
		private const int CallsPerRound = 20_000;

		private static readonly Endpoint[] Endpoints =
		[
			new Endpoint("BB9A", null, "10.0.0.1", 3000, Endpoint.SourceList.STANDARD),
			new Endpoint("BB9B", "tls1", "10.0.0.2", 3000, Endpoint.SourceList.STANDARD),
			new Endpoint("BB9C", null, "10.0.0.3", 3000, Endpoint.SourceList.ALTERNATE),
			new Endpoint("BB9D", null, "2001:db8::1", 3000, Endpoint.SourceList.STANDARD)
		];

		[TestMethod]
		public void StaticMapP99UnderTarget()
		{
			Measure("static-map", new StaticMapAddressTranslator(new Dictionary<string, string>
			{
				["10.0.0.1"] = "192.168.1.1",
				["10.0.0.2"] = "192.168.1.2"
			}), 2);
		}

		[TestMethod]
		public void PortMapP99UnderTarget()
		{
			Measure("port-map", new PortMapAddressTranslator(new Dictionary<string, string>
			{
				["10.0.0.1:3000"] = "lb.example.com:30001",
				["[2001:db8::1]:3000"] = "lb.example.com:30002",
				["BB9B"] = "lb.example.com:30003"
			}), 3);
		}

		[TestMethod]
		public void PassThroughP99UnderTarget()
		{
			Measure("pass-through", PassThroughAddressTranslator.Instance, 4);
		}

		[TestMethod]
		public void SpinningTranslatorFailsTarget()
		{
			IAddressTranslator spinning = new SpinningTranslator(TimeSpan.FromMilliseconds(2));
			long[][] rounds = new long[Rounds][];

			for (int r = 0; r < Rounds; r++)
			{
				rounds[r] = new long[100];
				Time(spinning, rounds[r]);
			}

			AssertFailedException failure = Assert.Throws<AssertFailedException>(() =>
				AssertP99UnderTarget("spin-2ms", rounds));
			StringAssert.Contains(failure.Message, "spin-2ms median round p99 ");
		}

		private static void Measure(string name, IAddressTranslator translator, int hitsPerRound)
		{
			int hits = 0;

			for (int i = 0; i < Warmup; i++)
			{
				if (translator.Translate(Endpoints[i % Endpoints.Length]) != null)
				{
					hits++;
				}
			}

			long[][] rounds = new long[Rounds][];

			for (int r = 0; r < Rounds; r++)
			{
				rounds[r] = new long[CallsPerRound];
				hits += Time(translator, rounds[r]);
			}

			Assert.AreEqual((Warmup + Rounds * CallsPerRound) / Endpoints.Length * hitsPerRound, hits);
			AssertP99UnderTarget(name, rounds);
		}

		private static int Time(IAddressTranslator translator, long[] ticks)
		{
			int hits = 0;

			for (int i = 0; i < ticks.Length; i++)
			{
				Endpoint advertised = Endpoints[i % Endpoints.Length];
				long begin = Stopwatch.GetTimestamp();
				bool hit = translator.Translate(advertised) != null;
				ticks[i] = Stopwatch.GetTimestamp() - begin;

				if (hit)
				{
					hits++;
				}
			}

			return hits;
		}

		private static void AssertP99UnderTarget(string name, long[][] rounds)
		{
			long[] p50s = new long[rounds.Length];
			long[] p99s = new long[rounds.Length];
			int calls = 0;

			for (int r = 0; r < rounds.Length; r++)
			{
				long[] nanos = (long[])rounds[r].Clone();
				Array.Sort(nanos);
				p50s[r] = TicksToNs(nanos[nanos.Length / 2]);
				p99s[r] = TicksToNs(nanos[nanos.Length * 99 / 100]);
				calls += rounds[r].Length;
			}

			string roundP99s = '[' + string.Join(", ", p99s) + ']';
			Array.Sort(p50s);
			Array.Sort(p99s);
			long p50 = p50s[rounds.Length / 2];
			long p99 = p99s[rounds.Length / 2];
			Console.WriteLine("translate() " + name + ": p50 " + p50 + " ns, p99 " + p99 + " ns (median of " +
				rounds.Length + " rounds, round p99s " + roundP99s + " ns) over " + calls + " calls");

			Assert.IsTrue(p99 < 1_000_000L, name + " median round p99 " + p99 + " ns, round p99s " + roundP99s + " ns");
		}

		private static long TicksToNs(long ticks)
		{
			return ticks * 1_000_000_000L / Stopwatch.Frequency;
		}

		private sealed class SpinningTranslator : IAddressTranslator
		{
			private readonly long spinTicks;

			public SpinningTranslator(TimeSpan spin)
			{
				spinTicks = (long)(spin.TotalSeconds * Stopwatch.Frequency);
			}

			public Host Translate(Endpoint advertised)
			{
				long deadline = Stopwatch.GetTimestamp() + spinTicks;

				while (Stopwatch.GetTimestamp() < deadline)
				{
				}

				return new Host(advertised.host, advertised.port);
			}
		}
	}
}
