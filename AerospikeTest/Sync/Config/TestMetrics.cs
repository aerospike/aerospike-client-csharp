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

namespace Aerospike.Test
{
	[TestClass]
	public class TestMetrics : TestSync
	{
		[TestMethod]
		public void MetricsListenerCallbacks()
		{
			CapturingMetricsListener listener = new();
			MetricsPolicy policy = new()
			{
				Listener = listener,
				Interval = 1
			};

			client.EnableMetrics(policy);
			try
			{
				Assert.IsTrue(client.Cluster.MetricsEnabled);
				Assert.IsTrue(listener.EnableCount >= 1);

				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-listener-key");
				client.Put(null, key, new Bin("m", 1));
				client.Get(null, key);

				WaitForSnapshots(listener, minCount: 1);
			}
			finally
			{
				client.DisableMetrics();
			}

			Assert.IsTrue(listener.DisableCount >= 1);
			Assert.IsFalse(client.Cluster.MetricsEnabled);
		}

		[TestMethod]
		public void MetricsWriterWritesFile()
		{
			string reportDir = Path.Combine(Path.GetTempPath(), $"as-metrics-writer-{Guid.NewGuid():N}");
			Directory.CreateDirectory(reportDir);

			try
			{
				MetricsPolicy policy = new()
				{
					ReportDir = reportDir,
					Interval = 1
				};

				client.EnableMetrics(policy);
				try
				{
					Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-writer-key");
					client.Put(null, key, new Bin("m", 1));
					client.Get(null, key);

					Util.Sleep(2500);
				}
				finally
				{
					client.DisableMetrics();
				}

				string[] files = Directory.GetFiles(reportDir, "metrics-*.log");
				Assert.IsTrue(files.Length > 0, "Expected at least one metrics log file");

				string content = File.ReadAllText(files[0]);
				Assert.IsTrue(content.Contains("header(2)"), "Metrics file should contain header");
				Assert.IsTrue(content.Contains("cluster["), "Metrics file should contain cluster snapshot");
			}
			finally
			{
				if (Directory.Exists(reportDir))
				{
					Directory.Delete(reportDir, true);
				}
			}
		}

		[TestMethod]
		public void ConfigMergesMetricsPolicyWithoutEnableField()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.GoodConfigYaml);
			AerospikeClient configClient = scope.Client;

			CapturingMetricsListener listener = new();
			MetricsPolicy policy = new()
			{
				Listener = listener,
				Interval = 1
			};

			configClient.EnableMetrics(policy);
			try
			{
				Assert.IsTrue(configClient.Cluster.MetricsEnabled);
				Assert.AreEqual(2, configClient.Cluster.MetricsPolicy.LatencyShift);
				Assert.AreEqual(7, configClient.Cluster.MetricsPolicy.LatencyColumns);
				Assert.IsNotNull(configClient.Cluster.MetricsPolicy.labels);
				Assert.AreEqual("us-west", configClient.Cluster.MetricsPolicy.labels["region"]);
			}
			finally
			{
				configClient.DisableMetrics();
			}
		}

		[TestMethod]
		public void ConfigRejectsManualEnableWhenDisabledInYaml()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(ConfigTestHelpers.MetricsDisabledYaml);
			AerospikeClient configClient = scope.Client;

			CapturingMetricsListener listener = new();
			configClient.EnableMetrics(new MetricsPolicy { Listener = listener, Interval = 1 });

			Assert.IsFalse(configClient.Cluster.MetricsEnabled);
			Assert.AreEqual(0, listener.EnableCount);
		}

		[TestMethod]
		public void GetClusterStatsWithMetricsEnabled()
		{
			CapturingMetricsListener listener = new();
			client.EnableMetrics(new MetricsPolicy { Listener = listener, Interval = 1 });
			try
			{
				Key key = new(SuiteHelpers.ns, SuiteHelpers.set, "metrics-stats-key");
				client.Put(null, key, new Bin("m", 1));
				client.Get(null, key);

				ClusterStats stats = client.GetClusterStats();
				Assert.IsNotNull(stats);
				Assert.IsTrue(stats.nodes.Length > 0);
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		private static void WaitForSnapshots(CapturingMetricsListener listener, int minCount)
		{
			for (int i = 0; i < 30; i++)
			{
				if (listener.SnapshotCount >= minCount)
				{
					return;
				}
				Util.Sleep(500);
			}

			Assert.Fail($"Expected at least {minCount} metrics snapshot(s), received {listener.SnapshotCount}");
		}

		private sealed class CapturingMetricsListener : IMetricsListener
		{
			public int EnableCount { get; private set; }
			public int SnapshotCount { get; private set; }
			public int DisableCount { get; private set; }

			public void OnEnable(Cluster cluster, MetricsPolicy policy) => EnableCount++;

			public void OnSnapshot(Cluster cluster) => SnapshotCount++;

			public void OnNodeClose(Node node) { }

			public void OnDisable(Cluster cluster) => DisableCount++;
		}
	}
}
