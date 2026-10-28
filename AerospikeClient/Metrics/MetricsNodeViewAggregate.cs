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

using System.Collections.ObjectModel;
using static Aerospike.Client.Latency;

namespace Aerospike.Client
{
	/// <summary>
	/// Sums immutable node views for snapshot construction. Unlike
	/// <see cref="MetricsNodeCollector"/>, it owns no store state and never
	/// participates in shape cutovers.
	/// </summary>
	internal sealed class MetricsNodeViewAggregate
	{
		private sealed class NamespaceTotals
		{
			internal long Errors;
			internal long Timeouts;
			internal long ClientTimeouts;
			internal long ServerTimeouts;
			internal long KeyBusy;
			internal long RecordTooBig;
			internal long DeviceOverload;
			internal long BytesIn;
			internal long BytesOut;
			internal readonly long[][] Latency;

			internal NamespaceTotals(int columns)
			{
				Latency = new long[Aerospike.Client.Latency.GetMax()][];

				for (int i = 0; i < Latency.Length; i++)
				{
					Latency[i] = new long[columns];
				}
			}
		}

		private readonly int columns;
		private readonly Dictionary<string, NamespaceTotals> namespaces =
			new(StringComparer.Ordinal);
		private readonly long[] connectionLatency;
		private long opened;
		private long closed;
		private long poolExhausted;
		private long openFailures;
		private long tlsHandshakeFailures;
		private long authFailures;

		internal MetricsNodeViewAggregate(MetricsHistogramShape shape)
		{
			columns = shape.Columns;
			connectionLatency = new long[columns];
		}

		internal void Add(MetricsNodeView source)
		{
			opened += source.Opened;
			closed += source.Closed;
			poolExhausted += source.PoolExhausted;
			openFailures += source.OpenFailures;
			tlsHandshakeFailures += source.TlsHandshakeFailures;
			authFailures += source.AuthFailures;
			AddBuckets(connectionLatency, source.ConnectionLatency);

			foreach (KeyValuePair<string, MetricsNamespaceView> entry in source.Namespaces)
			{
				string name = entry.Key ?? string.Empty;

				if (!namespaces.TryGetValue(name, out NamespaceTotals totals))
				{
					totals = new NamespaceTotals(columns);
					namespaces.Add(name, totals);
				}

				MetricsNamespaceView ns = entry.Value;
				totals.Errors += ns.Errors;
				totals.Timeouts += ns.Timeouts;
				totals.ClientTimeouts += ns.ClientTimeouts;
				totals.ServerTimeouts += ns.ServerTimeouts;
				totals.KeyBusy += ns.KeyBusy;
				totals.RecordTooBig += ns.RecordTooBig;
				totals.DeviceOverload += ns.DeviceOverload;
				totals.BytesIn += ns.BytesIn;
				totals.BytesOut += ns.BytesOut;

				foreach (KeyValuePair<LatencyType, IReadOnlyList<long>> latency in ns.Latency)
				{
					AddBuckets(totals.Latency[(int)latency.Key], latency.Value);
				}
			}
		}

		internal MetricsNodeView ToView(string name)
		{
			Dictionary<string, MetricsNamespaceView> namespaceViews = new();

			foreach (KeyValuePair<string, NamespaceTotals> entry in namespaces)
			{
				NamespaceTotals totals = entry.Value;
				Dictionary<LatencyType, IReadOnlyList<long>> latency =
					new(totals.Latency.Length);

				for (int i = 0; i < totals.Latency.Length; i++)
				{
					latency[(LatencyType)i] =
						Array.AsReadOnly((long[])totals.Latency[i].Clone());
				}

				namespaceViews[entry.Key] = new MetricsNamespaceView(
					entry.Key,
					totals.Errors,
					totals.Timeouts,
					totals.ClientTimeouts,
					totals.ServerTimeouts,
					totals.KeyBusy,
					totals.RecordTooBig,
					totals.DeviceOverload,
					totals.BytesIn,
					totals.BytesOut,
					new ReadOnlyDictionary<LatencyType, IReadOnlyList<long>>(latency));
			}

			return new MetricsNodeView(
				name,
				opened,
				closed,
				poolExhausted,
				openFailures,
				tlsHandshakeFailures,
				authFailures,
				Array.AsReadOnly((long[])connectionLatency.Clone()),
				new ReadOnlyDictionary<string, MetricsNamespaceView>(namespaceViews));
		}

		private static void AddBuckets(long[] target, IReadOnlyList<long> source)
		{
			int count = Math.Min(target.Length, source.Count);

			for (int i = 0; i < count; i++)
			{
				target[i] += source[i];
			}
		}
	}
}
