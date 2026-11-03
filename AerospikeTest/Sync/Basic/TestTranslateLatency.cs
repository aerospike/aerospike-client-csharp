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
using System.Collections.Generic;
using System.Diagnostics;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// U-8 timing half: translate() p99 stays under a 10 ms CI ceiling (NFR-7 target is 1 ms).
	/// </summary>
	[TestClass]
	public class TestTranslateLatency
	{
		private const int Warmup = 50_000;
		private const int Iterations = 50_000;
		private static readonly long CeilingTicks = Stopwatch.Frequency / 100; // 10 ms

		private static readonly Endpoint[] Endpoints =
		[
			new Endpoint("BB9A", null, "10.0.0.1", 3000, Endpoint.SourceList.STANDARD),
			new Endpoint("BB9B", "tls1", "10.0.0.2", 3000, Endpoint.SourceList.STANDARD),
			new Endpoint("BB9C", null, "10.0.0.3", 3000, Endpoint.SourceList.ALTERNATE),
			new Endpoint("BB9D", null, "2001:db8::1", 3000, Endpoint.SourceList.STANDARD)
		];

		[TestMethod]
		public void StaticMapP99UnderCeiling()
		{
			Measure("static-map", new StaticMapAddressTranslator(new Dictionary<string, string>
			{
				["10.0.0.1"] = "192.168.1.1",
				["10.0.0.2"] = "192.168.1.2"
			}), 2);
		}

		[TestMethod]
		public void PortMapP99UnderCeiling()
		{
			Measure("port-map", new PortMapAddressTranslator(new Dictionary<string, string>
			{
				["10.0.0.1:3000"] = "lb.example.com:30001",
				["[2001:db8::1]:3000"] = "lb.example.com:30002",
				["BB9B"] = "lb.example.com:30003"
			}), 3);
		}

		[TestMethod]
		public void PassThroughP99UnderCeiling()
		{
			Measure("pass-through", PassThroughAddressTranslator.Instance, 4);
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

			long[] ticks = new long[Iterations];

			for (int i = 0; i < Iterations; i++)
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

			Array.Sort(ticks);
			long p50 = ticks[Iterations / 2];
			long p99 = ticks[Iterations * 99 / 100];
			Console.WriteLine("translate() " + name + ": p50 " + TicksToNs(p50) + " ns, p99 " +
				TicksToNs(p99) + " ns over " + Iterations + " calls");

			Assert.AreEqual((Warmup + Iterations) / Endpoints.Length * hitsPerRound, hits);
			Assert.IsTrue(p99 < CeilingTicks, name + " p99 " + TicksToNs(p99) + " ns");
		}

		private static long TicksToNs(long ticks)
		{
			return ticks * 1_000_000_000L / Stopwatch.Frequency;
		}
	}
}
