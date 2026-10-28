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
using Aerospike.Client;
using System.Collections.Concurrent;
using System.Reflection;

namespace Aerospike.Test
{
	/// <summary>
	/// Reflection-based inspection of store-owned histogram state, used to prove
	/// that policy application reshapes eagerly and snapshot reads only copy.
	/// </summary>
	internal static class MetricsTestState
	{
		private const BindingFlags Private =
			BindingFlags.Instance | BindingFlags.NonPublic;

		internal static MetricsNamespaceCollector CallCollector(MetricsStore store) =>
			Get<MetricsNamespaceCollector>(store, "callCollector");

		internal static MetricsNodeCollector RemovedNodeCollector(MetricsStore store) =>
			Get<MetricsNodeCollector>(store, "removedNodeCollector");

		internal static MetricsStore.HistogramState Tend(MetricsStore store) =>
			Get<MetricsStore.HistogramState>(store, "tendDuration");

		internal static MetricsNamespaceCollector.HistogramBank Bank(
			MetricsNamespaceCollector collector) =>
			Get<MetricsNamespaceCollector.HistogramBank>(collector, "histogramBank");

		internal static MetricsNodeCollector.ConnectionHistogram Connection(
			MetricsNodeCollector collector) =>
			Get<MetricsNodeCollector.ConnectionHistogram>(collector, "connection");

		internal static IReadOnlyDictionary<string, MetricsNamespaceCollector> Namespaces(
			MetricsNodeCollector collector) =>
			Get<ConcurrentDictionary<string, MetricsNamespaceCollector>>(
				collector,
				"namespaces");

		internal static IReadOnlyList<MetricsNodeGeneration> Generations(
			MetricsStore store) =>
			Get<ConcurrentDictionary<Node, MetricsNodeGeneration>>(store, "nodes")
				.Values
				.ToArray();

		/// <summary>
		/// Every store-owned histogram reference, in a stable order.
		/// </summary>
		internal static IReadOnlyList<object> Identities(MetricsStore store)
		{
			List<object> result = new()
			{
				store.RuntimeConfig,
				Bank(CallCollector(store)),
				Tend(store),
				Connection(RemovedNodeCollector(store))
			};

			foreach (MetricsNodeGeneration generation in Generations(store)
				.OrderBy(item => item.Name, StringComparer.Ordinal))
			{
				result.Add(generation.Collector);
				result.Add(Connection(generation.Collector));

				foreach (KeyValuePair<string, MetricsNamespaceCollector> entry in
					Namespaces(generation.Collector)
						.OrderBy(item => item.Key, StringComparer.Ordinal))
				{
					result.Add(Bank(entry.Value));
				}
			}
			return result;
		}

		internal static void AssertSameIdentities(
			IReadOnlyList<object> expected,
			IReadOnlyList<object> actual)
		{
			Assert.AreEqual(expected.Count, actual.Count);

			for (int i = 0; i < expected.Count; i++)
			{
				Assert.AreSame(expected[i], actual[i], $"Histogram state {i} changed.");
			}
		}

		/// <summary>
		/// Assert every store-owned histogram is published at exactly
		/// <paramref name="shape"/>.
		/// </summary>
		internal static void AssertShape(
			MetricsStore store,
			MetricsHistogramShape shape)
		{
			Assert.AreSame(shape, store.RuntimeConfig.Shape);
			Assert.AreSame(shape, Bank(CallCollector(store)).Shape);
			Assert.AreSame(shape, Tend(store).Shape);
			Assert.AreSame(shape, RemovedNodeCollector(store).CurrentShape);
			Assert.AreSame(shape, Connection(RemovedNodeCollector(store)).Shape);

			foreach (MetricsNodeGeneration generation in Generations(store))
			{
				Assert.AreSame(shape, generation.Collector.CurrentShape);

				foreach (MetricsNamespaceCollector ns in
					Namespaces(generation.Collector).Values)
				{
					Assert.AreSame(shape, Bank(ns).Shape);
				}
			}
		}

		private static T Get<T>(object target, string name)
		{
			FieldInfo field = target.GetType().GetField(name, Private);
			Assert.IsNotNull(field, $"Missing field {name}.");
			return (T)field.GetValue(target);
		}
	}
}
