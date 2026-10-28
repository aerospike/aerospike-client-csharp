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

namespace Aerospike.Client
{
	internal sealed class MetricsPublication
	{
		internal readonly MetricsStore Store;
		internal readonly MetricsRuntimeConfig Config;
		internal readonly IReadOnlyDictionary<string, string> Labels;
		internal readonly string AppId;
		internal readonly string CompatibilityAppId;

		internal MetricsPublication(
			MetricsStore store,
			MetricsRuntimeConfig config,
			IReadOnlyDictionary<string, string> labels,
			string appId,
			string compatibilityAppId)
		{
			Store = store;
			Config = config;
			Labels = labels;
			AppId = appId;
			CompatibilityAppId = compatibilityAppId;
		}
	}

	/// <summary>
	/// Owns the cluster-lifetime metrics store, runtime samplers, delivery, and
	/// transactional enable, disable, and reconfiguration lifecycle.
	/// </summary>
	internal sealed partial class MetricsManager
	{
		private const int FailureThreshold = 3;
		private const int SuspendedCycles = 3;

		private volatile MetricsStore store;
		private volatile MetricsPublication publication;
		private volatile MetricsCompatibilitySnapshot
			frozenCompatibilitySnapshot;
		private readonly object exportLock = new();
		internal readonly object LifecycleLock = new();
		internal MetricsReporter Reporter;
		internal MetricsWriter FileWriter;
		internal Action<MetricsReporter> ReporterStarter =
			static reporter => reporter.Start();
		internal Func<string, long, int, int, MetricsWriter> WriterFactory =
			static (dir, limit, columns, shift) =>
				new MetricsWriter(dir, limit, columns, shift);
		private IReadOnlyList<IMetricsExporter> exporters =
			Array.Empty<IMetricsExporter>();
		internal double FrozenCpuPercent;
		internal long FrozenMemoryBytes;
		internal double FrozenCompatibilityCpuPercent;
		internal long FrozenCompatibilityMemoryBytes;

		internal MetricsProcessSampler PullProcessSampler { get; } = new();

		internal MetricsProcessSampler ReporterProcessSampler { get; } = new();

		internal MetricsProcessSampler CompatibilityProcessSampler { get; } =
			new(true);

		internal MetricsExporterFailureRegistry ExporterFailures { get; } = new();

		internal MetricsStore Store
		{
			get => store;
			set => store = value;
		}

		internal MetricsPublication Publication
		{
			get => publication;
			set => publication = value;
		}

		internal MetricsStore CreateStore(
			MetricsPolicy policy,
			bool active = true)
		{
			return new MetricsStore(
				policy,
				active: active);
		}

		internal void Enable(Cluster cluster, MetricsPolicy policy)
		{
			RejectLifecycleFromExporter();

			lock (LifecycleLock)
			{
				if (cluster.AllowExplicitMetricsEnable())
				{
					lock (cluster.metricsLock)
					{
						EnableInternal(
							cluster,
							cluster.MergeMetricsPolicyWithConfig(policy));
						cluster.SetMetricsPolicyBase(policy);
						ExporterFailures.Clear();
					}
				}
			}
		}

		internal void Disable(Cluster cluster)
		{
			RejectLifecycleFromExporter();

			lock (LifecycleLock)
			{
				if (cluster.AllowExplicitMetricsDisable())
				{
					lock (cluster.metricsLock)
					{
						DisableInternal(cluster);
					}
				}
			}
		}

		internal void EnableConfigured(Cluster cluster, MetricsPolicy policy)
		{
			EnableInternal(cluster, policy);
		}

		internal void DisableForClose(Cluster cluster)
		{
			DisableInternal(cluster);
		}

		internal MetricsCallContext BeginCall(
			Cluster cluster,
			ApiMetricsOperation operation,
			ApiExecutionMode mode)
		{
			if (operation == ApiMetricsOperation.Uninstrumented)
			{
				return default;
			}

			MetricsStore current = store;
			bool sampled = false;
			if (cluster.MetricsEnabled)
			{
				current?.BeginApiCall(
				operation,
				mode,
				out sampled);
			}
			return new MetricsCallContext(
				cluster,
				current,
				operation,
				mode,
				sampled,
				sampled);
		}
	}
}
