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
using Aerospike.Client.Config;
using System.Reflection;
using System.Reflection.Emit;

namespace Aerospike.Test
{
	[TestClass]
	public class TestMetricsExport : TestSync
	{
		[TestMethod]
		public void MetricsSnapshotPublicScopesHaveExactShape()
		{
			AssertPublicShape<ClusterMetricsSnapshot>(
				(nameof(ClusterMetricsSnapshot.ConnectionsInUse), typeof(int)),
				(nameof(ClusterMetricsSnapshot.ConnectionsInPool), typeof(int)),
				(nameof(ClusterMetricsSnapshot.ActiveNodes), typeof(int)),
				(nameof(ClusterMetricsSnapshot.RecoverQueueSize), typeof(int)),
				(nameof(ClusterMetricsSnapshot.ConnectionsOpened), typeof(long)),
				(nameof(ClusterMetricsSnapshot.ConnectionsClosed), typeof(long)),
				(nameof(ClusterMetricsSnapshot.InvalidNodes), typeof(long)),
				(nameof(ClusterMetricsSnapshot.CommandCount), typeof(long)),
				(nameof(ClusterMetricsSnapshot.CommandRetries), typeof(long)),
				(nameof(ClusterMetricsSnapshot.ClientTimeouts), typeof(long)),
				(nameof(ClusterMetricsSnapshot.ServerTimeouts), typeof(long)),
				(nameof(ClusterMetricsSnapshot.BatchPartialFailures), typeof(long)),
				(nameof(ClusterMetricsSnapshot.Latency), typeof(ClusterLatencyMetricsSnapshot)),
				(nameof(ClusterMetricsSnapshot.Validation), typeof(ValidationMetricsSnapshot)),
				(nameof(ClusterMetricsSnapshot.RecoverEnqueued), typeof(long)),
				(nameof(ClusterMetricsSnapshot.RecoverCompleted), typeof(long)),
				(nameof(ClusterMetricsSnapshot.RecoverAborted), typeof(long)),
				(nameof(ClusterMetricsSnapshot.TimeoutImmediate), typeof(long)),
				(nameof(ClusterMetricsSnapshot.QueryRecordsQueued), typeof(long)),
				(nameof(ClusterMetricsSnapshot.QueryServersConcurrent), typeof(long)),
				(nameof(ClusterMetricsSnapshot.QueryAbandoned), typeof(long)),
				(nameof(ClusterMetricsSnapshot.CpuPercent), typeof(double)),
				(nameof(ClusterMetricsSnapshot.MemoryBytes), typeof(long)),
				(nameof(ClusterMetricsSnapshot.TendDuration), typeof(IReadOnlyList<long>)));
			AssertPublicShape<ValidationMetricsSnapshot>(
				(nameof(ValidationMetricsSnapshot.OpenFailures), typeof(long)),
				(nameof(ValidationMetricsSnapshot.TlsHandshakeFailures), typeof(long)),
				(nameof(ValidationMetricsSnapshot.AuthFailures), typeof(long)));
			AssertPublicShape<NodeMetricsSnapshot>(
				(nameof(NodeMetricsSnapshot.Name), typeof(string)),
				(nameof(NodeMetricsSnapshot.Address), typeof(string)),
				(nameof(NodeMetricsSnapshot.Port), typeof(int)),
				(nameof(NodeMetricsSnapshot.ErrorRate), typeof(int)),
				(nameof(NodeMetricsSnapshot.Connections), typeof(ConnectionMetricsSnapshot)),
				(nameof(NodeMetricsSnapshot.Latency), typeof(NodeLatencyMetricsSnapshot)),
				(nameof(NodeMetricsSnapshot.Namespaces), typeof(IReadOnlyList<NamespaceMetricsSnapshot>)));
			AssertPublicShape<ConnectionMetricsSnapshot>(
				(nameof(ConnectionMetricsSnapshot.InUse), typeof(int)),
				(nameof(ConnectionMetricsSnapshot.InPool), typeof(int)),
				(nameof(ConnectionMetricsSnapshot.Opened), typeof(long)),
				(nameof(ConnectionMetricsSnapshot.Closed), typeof(long)),
				(nameof(ConnectionMetricsSnapshot.PoolExhausted), typeof(long)),
				(nameof(ConnectionMetricsSnapshot.OpenFailures), typeof(long)),
				(nameof(ConnectionMetricsSnapshot.TlsHandshakeFailures), typeof(long)),
				(nameof(ConnectionMetricsSnapshot.AuthFailures), typeof(long)));
			AssertPublicShape<NamespaceMetricsSnapshot>(
				(nameof(NamespaceMetricsSnapshot.Name), typeof(string)),
				(nameof(NamespaceMetricsSnapshot.Errors), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.ClientTimeouts), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.ServerTimeouts), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.Timeouts), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.KeyBusy), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.RecordTooBig), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.DeviceOverload), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.BytesIn), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.BytesOut), typeof(long)),
				(nameof(NamespaceMetricsSnapshot.Latency), typeof(NamespaceLatencyMetricsSnapshot)));
			AssertPublicShape<ClusterLatencyMetricsSnapshot>(
				(nameof(ClusterLatencyMetricsSnapshot.Read), typeof(IReadOnlyList<long>)),
				(nameof(ClusterLatencyMetricsSnapshot.Write), typeof(IReadOnlyList<long>)),
				(nameof(ClusterLatencyMetricsSnapshot.Batch), typeof(IReadOnlyList<long>)),
				(nameof(ClusterLatencyMetricsSnapshot.Query), typeof(IReadOnlyList<long>)));
			AssertPublicShape<NodeLatencyMetricsSnapshot>(
				(nameof(NodeLatencyMetricsSnapshot.Connection), typeof(IReadOnlyList<long>)));
			AssertPublicShape<NamespaceLatencyMetricsSnapshot>(
				(nameof(NamespaceLatencyMetricsSnapshot.Read), typeof(IReadOnlyList<long>)),
				(nameof(NamespaceLatencyMetricsSnapshot.Write), typeof(IReadOnlyList<long>)),
				(nameof(NamespaceLatencyMetricsSnapshot.Batch), typeof(IReadOnlyList<long>)),
				(nameof(NamespaceLatencyMetricsSnapshot.Query), typeof(IReadOnlyList<long>)));
		}

		[TestMethod]
		public void MetricsSnapshotIsAlwaysAvailable()
		{
			if (client.Cluster.MetricsEnabled)
			{
				client.DisableMetrics();
			}

			MetricsSnapshot before = client.GetMetricsSnapshot();
			Assert.IsFalse(before.MetricsEnabled);
			Assert.IsNotNull(before.Cluster);
			Assert.IsNotNull(before.Cluster.Latency);
			Assert.IsNotNull(before.Cluster.Validation);
			Assert.IsNotNull(before.Usage);
			Assert.IsNotNull(before.Nodes);
			Assert.AreEqual(client.Nodes.Length, before.Nodes.Count);
			Assert.IsTrue(before.Nodes.All(node =>
				node.Latency != null &&
				node.Connections != null &&
				node.Namespaces != null));

			MetricsPolicy policy = new()
			{
				OperationalEnabled = false,
				UsageEnabled = true,
				Exporter = MetricsExporterMode.None
			};

			client.EnableMetrics(policy);
			Key key = new(
				SuiteHelpers.ns,
				SuiteHelpers.set,
				"metrics-tier-matrix");
			client.Get(null, key);
			MetricsSnapshot active = client.GetMetricsSnapshot();
			Assert.IsTrue(active.MetricsEnabled);
			Assert.IsFalse(active.OperationalMetricsEnabled);
			Assert.IsTrue(active.UsageMetricsEnabled);
			Assert.AreEqual(0L, active.Cluster.CommandCount);
			Assert.IsTrue(active.Usage.ApiBlocking > 0);
			Assert.AreEqual(0, active.Nodes.Sum(node => node.Namespaces.Count));
			Assert.AreEqual(
				active.Nodes.Sum(node => node.Connections.InPool),
				active.Cluster.ConnectionsInPool);
			Assert.AreEqual(
				active.Nodes.Sum(node => node.Connections.InUse),
				active.Cluster.ConnectionsInUse);

			client.DisableMetrics();
			MetricsSnapshot frozen = client.GetMetricsSnapshot();
			Assert.IsFalse(frozen.MetricsEnabled);
			Assert.IsFalse(frozen.OperationalMetricsEnabled);
			Assert.IsTrue(frozen.UsageMetricsEnabled);
			Assert.AreEqual(active.Usage.ApiBlocking, frozen.Usage.ApiBlocking);
		}

		[TestMethod]
		public void MetricsSnapshotIsRetainableImmutableAndOrdered()
		{
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true,
				UsageEnabled = true
			};
			client.EnableMetrics(policy);

			try
			{
				MetricsSnapshot retained = client.GetMetricsSnapshot();
				long retainedCommands = retained.Cluster.CommandCount;
				long retainedBlocking = retained.Usage.ApiBlocking;
				Key key = new(
					SuiteHelpers.ns,
					SuiteHelpers.set,
					"metrics-snapshot-immutable");

				client.Put(null, key, new Bin("m", 1));
				MetricsSnapshot current = client.GetMetricsSnapshot();

				Assert.AreEqual(retainedCommands, retained.Cluster.CommandCount);
				Assert.AreEqual(retainedBlocking, retained.Usage.ApiBlocking);
				Assert.IsTrue(current.Cluster.CommandCount > retainedCommands);
				Assert.IsTrue(current.Usage.ApiBlocking > retainedBlocking);
				Assert.IsTrue(
					current.Cluster.Latency.Write.Sum() >
					retained.Cluster.Latency.Write.Sum());
				CollectionAssert.AreEqual(
					current.Nodes.Select(node => node.Name)
						.OrderBy(name => name, StringComparer.Ordinal)
						.ToArray(),
					current.Nodes.Select(node => node.Name).ToArray());

				IList<NodeMetricsSnapshot> nodes =
					(IList<NodeMetricsSnapshot>)current.Nodes;
				AssertThrows<NotSupportedException>(() =>
					nodes.Add(current.Nodes[0]));

				IDictionary<string, string> labels =
					(IDictionary<string, string>)current.Labels;
				AssertThrows<NotSupportedException>(() =>
					labels.Add("new", "value"));
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsSnapshotFreezesOperationalProcessMetricsOnDisable()
		{
			client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true
			});
			client.DisableMetrics();

			MetricsSnapshot first = client.GetMetricsSnapshot();
			Thread.Sleep(25);
			MetricsSnapshot second = client.GetMetricsSnapshot();

			Assert.IsFalse(first.MetricsEnabled);
			Assert.AreEqual(
				first.Cluster.CpuPercent,
				second.Cluster.CpuPercent);
			Assert.AreEqual(
				first.Cluster.MemoryBytes,
				second.Cluster.MemoryBytes);
		}

		[TestMethod]
		public void MetricsFrozenNodeMovesToDepartedAfterDisable()
		{
			using ConfigClientScope scope = ConfigClientScope.Create("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 5000
				""");
			scope.Client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None
			});
			Node node = scope.Client.Cluster.Nodes[0];
			string nodeName = node.Name;
			scope.Client.DisableMetrics();

			MethodInfo removeNodes = typeof(Cluster).GetMethod(
				"RemoveNodes",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(removeNodes);
			removeNodes.Invoke(
				scope.Client.Cluster,
				[new HashSet<Node>(ReferenceEqualityComparer.Instance) { node }]);

			MetricsSnapshot snapshot = scope.Client.GetMetricsSnapshot();
			Assert.IsFalse(snapshot.Nodes.Any(current => current.Name == nodeName));
			Assert.IsTrue(snapshot.NodesDeparted.Any(
				current => current.Name == nodeName));
		}

		[TestMethod]
		public void MetricsCloseFinalIncludesAlreadyInactiveNode()
		{
			using ConfigClientScope scope = ConfigClientScope.Create("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				""");
			CountingExporter exporter = new();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromHours(1)
			};
			policy.AddExporter(exporter);
			scope.Client.EnableMetrics(policy);
			Node node = scope.Client.Cluster.Nodes[0];
			string nodeName = node.Name;
			FieldInfo active = typeof(Node).GetField(
				"active",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(active);
			active.SetValue(node, false);

			scope.Client.Close();

			Assert.IsNotNull(exporter.LastSnapshot);
			Assert.IsTrue(exporter.LastSnapshot.NodesDeparted.Any(
				current => current.Name == nodeName));
		}

		[TestMethod]
		public void MetricsDisableNormalizesInactiveNodeExactlyOnce()
		{
			using ConfigClientScope scope = ConfigClientScope.Create("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 5000
				""");
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.None,
				ExportInterval = TimeSpan.FromHours(1)
			};
			scope.Client.EnableMetrics(policy);
			Node node = scope.Client.Cluster.Nodes[0];
			FieldInfo active = typeof(Node).GetField(
				"active",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(active);
			active.SetValue(node, false);

			scope.Client.DisableMetrics();
			NodeMetricsSnapshot before =
				scope.Client.GetMetricsSnapshot()
					.NodesDeparted.Single(
						current => current.Name == node.Name);
			MetricsSnapshot frozen =
				scope.Client.GetMetricsSnapshot();
			Assert.IsFalse(frozen.Compatibility.Nodes.Any(
				current => current.Name == node.Name));
			Assert.AreEqual(
				1,
				frozen.Compatibility.NodesDeparted.Count(
					current => current.Name == node.Name));

			MethodInfo transfer = typeof(Cluster).GetMethod(
				"TransferNodeDeparture",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(transfer);
			transfer.Invoke(
				scope.Client.Cluster,
				[node]);
			NodeMetricsSnapshot after =
				scope.Client.GetMetricsSnapshot()
					.NodesDeparted.Single(
						current => current.Name == node.Name);

			Assert.AreEqual(
				before.Connections.Opened,
				after.Connections.Opened);
			Assert.AreEqual(
				before.Connections.Closed,
				after.Connections.Closed);
		}

		[TestMethod]
		public void MetricsFrozenLegacyProcessValuesMatchFinalExport()
		{
			CountingExporter exporter = new();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromHours(1)
			};
			policy.AddExporter(exporter);
			client.EnableMetrics(policy);
			client.DisableMetrics();

			MetricsSnapshot final = exporter.LastSnapshot;
			MetricsSnapshot pulled = client.GetMetricsSnapshot();
			Assert.IsNotNull(final.Compatibility);
			Assert.IsNotNull(pulled.Compatibility);
			Assert.AreEqual(
				final.Compatibility.CpuPercent,
				pulled.Compatibility.CpuPercent);
			Assert.AreEqual(
				final.Compatibility.MemoryBytes,
				pulled.Compatibility.MemoryBytes);
		}

		[TestMethod]
		public void MetricsRemovalWaitsForDisableRollback()
		{
			using ConfigClientScope scope = ConfigClientScope.Create("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				""");
			BlockingRollbackListener listener = new();
			scope.Client.EnableMetrics(new MetricsPolicy
			{
				Listener = listener
			});
			Node node = scope.Client.Cluster.Nodes[0];
			Exception disableFailure = null;
			Task disable = Task.Run(() =>
			{
				try
				{
					scope.Client.DisableMetrics();
				}
				catch (Exception e)
				{
					disableFailure = e;
				}
			});
			Assert.IsTrue(
				listener.DisableEntered.Wait(
					TimeSpan.FromSeconds(5)));

			ManualResetEventSlim removalStarted = new(false);
			Task removal = Task.Run(() =>
			{
				removalStarted.Set();
				MethodInfo removeNodes = typeof(Cluster).GetMethod(
					"RemoveNodes",
					BindingFlags.Instance |
						BindingFlags.NonPublic);
				Assert.IsNotNull(removeNodes);
				removeNodes.Invoke(
					scope.Client.Cluster,
					[new HashSet<Node> { node }]);
			});
			Assert.IsTrue(
				removalStarted.Wait(TimeSpan.FromSeconds(5)));
			try
			{
				Assert.IsFalse(
					removal.Wait(TimeSpan.FromMilliseconds(100)),
					"Removal must wait for the lifecycle rollback.");
			}
			finally
			{
				listener.DisableRelease.Set();
			}
			Assert.IsTrue(disable.Wait(TimeSpan.FromSeconds(5)));
			Assert.IsTrue(removal.Wait(TimeSpan.FromSeconds(5)));
			Assert.IsInstanceOfType<InvalidOperationException>(
				disableFailure);
			Assert.AreEqual(2, listener.EnableCount);
			Assert.AreEqual(1, listener.NodeCloseCount);
		}

		[TestMethod]
		public void MetricsPolicyResolvesSourceAwareExporterDefaults()
		{
			MetricsPolicy legacyDefault = new();
			Assert.AreEqual(
				MetricsExporterMode.File,
				legacyDefault.ResolveExporterMode());

			MetricsPolicy listenerPolicy = new()
			{
				Listener = new EmptyMetricsListener()
			};
			Assert.AreEqual(
				MetricsExporterMode.None,
				listenerPolicy.ResolveExporterMode());

			CountingExporter exporter = new();
			MetricsPolicy custom = new();
			custom.AddExporter(exporter);
			custom.AddExporter(exporter);
			Assert.AreEqual(
				MetricsExporterMode.Custom,
				custom.ResolveExporterMode());
			Assert.AreEqual(1, custom.Exporters.Count);

			MetricsPolicy copy = new(custom);
			Assert.AreEqual(1, copy.Exporters.Count);
			Assert.AreSame(exporter, copy.Exporters[0]);
		}

		[TestMethod]
		public void MetricsConfigurationParsesDurationsAndSizes()
		{
			Assert.AreEqual(
				TimeSpan.FromMilliseconds(500),
				MetricsConfigurationParser.ParseDuration("500ms", "test"));
			Assert.AreEqual(
				TimeSpan.FromMinutes(2),
				MetricsConfigurationParser.ParseDuration("2m", "test"));
			Assert.AreEqual(
				1024L * 1024L,
				MetricsConfigurationParser.ParseSize("1mb", "test"));
			Assert.AreEqual(
				0L,
				MetricsConfigurationParser.ParseSize("0", "test"));
			Assert.AreEqual(
				2L * 1024L * 1024L * 1024L,
				MetricsConfigurationParser.ParseSize("2GB", "test"));
			AssertThrows<AerospikeException>(() =>
				MetricsConfigurationParser.ParseDuration("0s", "test"));
			AssertThrows<AerospikeException>(() =>
				MetricsConfigurationParser.ParseDuration(
					long.MaxValue + "h",
					"test"));
			AssertThrows<AerospikeException>(() =>
				MetricsConfigurationParser.ParseSize("-1", "test"));
			AssertThrows<AerospikeException>(() =>
				MetricsConfigurationParser.ParseSize(
					long.MaxValue + "gb",
					"test"));
		}

		[TestMethod]
		public void MetricsConfigurationReloadPreservesExporterBaselineAndRejectsInvalid()
		{
			using ConfigClientScope scope = ConfigClientScope.Create("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				""");
			CountingExporter exporter = new();
			MetricsPolicy baseline = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromHours(1)
			};
			baseline.AddExporter(exporter);
			scope.Client.EnableMetrics(baseline);

			scope.RewriteYaml(MetricsConfigYaml("none", "40ms"));
			Assert.AreEqual(
				MetricsExporterMode.None,
				scope.Client.Cluster.MetricsPolicy.ResolveExporterMode());
			Assert.AreEqual(
				TimeSpan.FromMilliseconds(40),
				scope.Client.Cluster.MetricsPolicy.ExportInterval);

			scope.RewriteYaml(MetricsConfigYaml("custom", "40ms"));
			Assert.IsTrue(exporter.WaitForCount(1, TimeSpan.FromSeconds(5)));
			Assert.AreSame(
				exporter,
				scope.Client.Cluster.MetricsPolicy.Exporters.Single());

			scope.RewriteYaml(MetricsConfigYaml("custom", "0ms"));
			Assert.AreEqual(
				TimeSpan.FromMilliseconds(40),
				scope.Client.Cluster.MetricsPolicy.ExportInterval);
			Assert.AreEqual(
				MetricsExporterMode.Custom,
				scope.Client.Cluster.MetricsPolicy.ResolveExporterMode());
		}

		[TestMethod]
		public void MetricsYamlOmittedExporterPreservesBaselineChannels()
		{
			const string initialYaml = """
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				""";
			const string omittedExporterYaml = """
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				  metrics:
				    enabled: true
				    export_interval: 1h
				""";

			using (ConfigClientScope customScope =
				ConfigClientScope.Create(initialYaml))
			{
				CountingExporter exporter = new();
				MetricsPolicy baseline = new()
				{
					Exporter = MetricsExporterMode.Custom,
					ExportInterval = TimeSpan.FromHours(1)
				};
				baseline.AddExporter(exporter);
				customScope.Client.EnableMetrics(baseline);
				customScope.RewriteYaml(omittedExporterYaml);

				Assert.AreEqual(
					MetricsExporterMode.Custom,
					customScope.Client.Cluster.MetricsPolicy
						.ResolveExporterMode());
				Assert.AreSame(
					exporter,
					customScope.Client.Cluster.MetricsPolicy
						.Exporters.Single());
			}

			using ConfigClientScope listenerScope =
				ConfigClientScope.Create(initialYaml);
			EmptyMetricsListener listener = new();
			listenerScope.Client.EnableMetrics(new MetricsPolicy
			{
				Listener = listener,
				ExportInterval = TimeSpan.FromHours(1)
			});
			listenerScope.RewriteYaml(omittedExporterYaml);

			Assert.AreEqual(
				MetricsExporterMode.None,
				listenerScope.Client.Cluster.MetricsPolicy
					.ResolveExporterMode());
			Assert.AreSame(
				listener,
				listenerScope.Client.Cluster.MetricsPolicy.Listener);
		}

		[TestMethod]
		public void MetricsYamlPublishesOneIdentityGeneration()
		{
			using ConfigClientScope scope = ConfigClientScope.Create(
				MetricsIdentityYaml(
					"initial-app",
					"initial-region",
					true));
			Assert.AreEqual(
				"initial-app",
				scope.Client.GetMetricsSnapshot().AppId);

			scope.RewriteYaml(MetricsIdentityYaml(
				"active-app",
				"active-region",
				true));
			MetricsSnapshot active =
				scope.Client.GetMetricsSnapshot();
			Assert.AreEqual("active-app", active.AppId);
			Assert.AreEqual(
				"active-region",
				active.Labels["region"]);
			Assert.AreEqual(
				"active-app",
				active.Compatibility.AppId);
			Assert.AreEqual(
				"active-region",
				active.Compatibility.Labels["region"]);

			scope.RewriteYaml(MetricsIdentityYaml(
				"disabled-app",
				"disabled-region",
				false));
			MetricsSnapshot frozen =
				scope.Client.GetMetricsSnapshot();
			Assert.AreEqual("active-app", frozen.AppId);
			Assert.AreEqual(
				"active-region",
				frozen.Labels["region"]);
			Assert.AreEqual(
				"active-app",
				frozen.Compatibility.AppId);
			Assert.AreEqual(
				"active-region",
				frozen.Compatibility.Labels["region"]);
		}

		[TestMethod]
		public void MetricsEnableRollsBackReporterStartFailure()
		{
			FieldInfo managerField = typeof(Cluster).GetField(
				"metricsManager",
				BindingFlags.Instance | BindingFlags.NonPublic);
			MetricsManager manager =
				(MetricsManager)managerField.GetValue(client.Cluster);
			FieldInfo starter = typeof(MetricsManager).GetField(
				"ReporterStarter",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(starter);
			object original = starter.GetValue(manager);
			starter.SetValue(manager, null);
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom
			};
			policy.AddExporter(new CountingExporter());

			try
			{
				AssertThrows<NullReferenceException>(
					() => client.EnableMetrics(policy));
				Assert.IsFalse(client.Cluster.MetricsEnabled);
				Assert.IsNull(client.Cluster.Nodes[0].GetMetrics());
			}
			finally
			{
				starter.SetValue(manager, original);
			}
		}

		[TestMethod]
		public void MetricsEnableRollsBackWriterConstructionFailure()
		{
			FieldInfo managerField = typeof(Cluster).GetField(
				"metricsManager",
				BindingFlags.Instance | BindingFlags.NonPublic);
			MetricsManager manager =
				(MetricsManager)managerField.GetValue(client.Cluster);
			FieldInfo factory = typeof(MetricsManager).GetField(
				"WriterFactory",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(factory);
			object original = factory.GetValue(manager);
			factory.SetValue(manager, null);

			try
			{
				AssertThrows<NullReferenceException>(() =>
					client.EnableMetrics(new MetricsPolicy
					{
						Exporter = MetricsExporterMode.File,
						ReportDir = Path.Combine(
							Path.GetTempPath(),
							$"unused-{Guid.NewGuid():N}")
					}));
				Assert.IsFalse(client.Cluster.MetricsEnabled);
				Assert.IsNull(client.Cluster.Nodes[0].GetMetrics());
			}
			finally
			{
				factory.SetValue(manager, original);
			}
		}

		[TestMethod]
		public void MetricsYamlReporterFailureKeepsPreviousRevision()
		{
			using ConfigClientScope scope = ConfigClientScope.Create("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				""");
			CountingExporter exporter = new();
			MetricsPolicy baseline = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromHours(1)
			};
			baseline.AddExporter(exporter);
			scope.Client.EnableMetrics(baseline);
			scope.RewriteYaml(MetricsConfigYaml("none", "1h"));

			IConfigProvider provider =
				ConfigTestHelpers.GetConfigProvider(scope.Client);
			IConfigurationData previousData = provider.ConfigurationData;
			MetricsPolicy previousPolicy =
				scope.Client.Cluster.MetricsPolicy;
			FieldInfo managerField = typeof(Cluster).GetField(
				"metricsManager",
				BindingFlags.Instance | BindingFlags.NonPublic);
			MetricsManager manager =
				(MetricsManager)managerField.GetValue(scope.Client.Cluster);
			FieldInfo starter = typeof(MetricsManager).GetField(
				"ReporterStarter",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(starter);
			object original = starter.GetValue(manager);
			starter.SetValue(manager, null);

			try
			{
				File.WriteAllText(
					scope.YamlPath,
					MetricsConfigYaml("custom", "1h"));
				AssertThrows<TargetInvocationException>(() =>
					ConfigTestHelpers.TriggerConfigReload(scope.Client));
				Assert.AreSame(previousData, provider.ConfigurationData);
				Assert.AreSame(
					previousPolicy,
					scope.Client.Cluster.MetricsPolicy);
				Assert.AreEqual(
					MetricsExporterMode.None,
					scope.Client.Cluster.MetricsPolicy.ResolveExporterMode());
				Assert.IsTrue(scope.Client.Cluster.MetricsEnabled);
			}
			finally
			{
				starter.SetValue(manager, original);
			}
		}

		[TestMethod]
		public void MetricsReporterExportsPeriodicAndFinalSnapshots()
		{
			CountingExporter exporter = new();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromMilliseconds(25)
			};
			policy.AddExporter(exporter);

			client.EnableMetrics(policy);

			try
			{
				Assert.IsTrue(exporter.WaitForCount(1, TimeSpan.FromSeconds(5)));
				Assert.IsTrue(exporter.LastSnapshot.MetricsEnabled);
			}
			finally
			{
				client.DisableMetrics();
			}

			Assert.IsTrue(exporter.Count >= 2);
			Assert.IsTrue(exporter.LastSnapshot.MetricsEnabled);
		}

		[TestMethod]
		public void MetricsReporterIsolatesFinalFailures()
		{
			ThrowingExporter failing = new();
			CountingExporter healthy = new();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromHours(1)
			};
			policy.AddExporter(failing);
			policy.AddExporter(healthy);

			client.EnableMetrics(policy);
			client.DisableMetrics();

			Assert.AreEqual(1, failing.Count);
			Assert.AreEqual(1, healthy.Count);
		}

		[TestMethod]
		public void MetricsReporterSharesSnapshotInOrderWithoutDisposingExporters()
		{
			List<string> calls = new();
			OwnedExporter first = new("first", calls);
			OwnedExporter second = new("second", calls);
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromHours(1)
			};
			policy.AddExporter(first);
			policy.AddExporter(second);

			client.EnableMetrics(policy);
			client.DisableMetrics();

			CollectionAssert.AreEqual(
				new[] { "first", "second" },
				calls);
			Assert.AreSame(first.Snapshot, second.Snapshot);
			Assert.IsFalse(first.Disposed);
			Assert.IsFalse(second.Disposed);
		}

		[TestMethod]
		public void ApplicationOwnedMetricsWriterReceivesLegacyPayload()
		{
			string reportDir = Path.Combine(
				Path.GetTempPath(),
				$"as-metrics-custom-writer-{Guid.NewGuid():N}");
			Directory.CreateDirectory(reportDir);

			try
			{
				using MetricsWriter writer = new(reportDir);
				LegacyPayloadExporter capture = new();
				MetricsPolicy policy = new()
				{
					Exporter = MetricsExporterMode.Custom,
					ExportInterval = TimeSpan.FromHours(1)
				};
				policy.AddExporter(writer);
				policy.AddExporter(capture);

				client.EnableMetrics(policy);
				client.DisableMetrics();

				Assert.IsNotNull(capture.Snapshot?.Compatibility);
				Assert.AreEqual(
					2,
					File.ReadAllLines(
						Directory.GetFiles(reportDir, "metrics-*.log").Single()).Length);
			}
			finally
			{
				if (client.Cluster.MetricsEnabled)
				{
					client.DisableMetrics();
				}
				if (Directory.Exists(reportDir))
				{
					Directory.Delete(reportDir, true);
				}
			}
		}

		[TestMethod]
		public void MetricsWriterAcceptsPullSnapshotsInEveryLifecycleState()
		{
			string reportDir = Path.Combine(
				Path.GetTempPath(),
				$"as-metrics-pull-writer-{Guid.NewGuid():N}");
			Directory.CreateDirectory(reportDir);

			try
			{
				using MetricsWriter writer = new(reportDir);
				writer.Export(client.GetMetricsSnapshot());
				client.EnableMetrics(new MetricsPolicy
				{
					Exporter = MetricsExporterMode.None
				});
				writer.Export(client.GetMetricsSnapshot());
				client.DisableMetrics();
				writer.Export(client.GetMetricsSnapshot());

				Assert.AreEqual(
					4,
					File.ReadAllLines(
						Directory.GetFiles(
							reportDir,
							"metrics-*.log").Single()).Length);
			}
			finally
			{
				if (client.Cluster.MetricsEnabled)
				{
					client.DisableMetrics();
				}
				if (Directory.Exists(reportDir))
				{
					Directory.Delete(reportDir, true);
				}
			}
		}

		[TestMethod]
		public void MetricsDisableFreezesManualWriterCompatibilityPayload()
		{
			string reportDir = Path.Combine(
				Path.GetTempPath(),
				$"as-metrics-frozen-writer-{Guid.NewGuid():N}");
			Directory.CreateDirectory(reportDir);

			try
			{
				client.EnableMetrics(new MetricsPolicy
				{
					Exporter = MetricsExporterMode.None
				});
				client.DisableMetrics();
				Node node = client.Cluster.Nodes[0];
				long retryCount = client.GetClusterStats().RetryCount;
				long errorCount = node.GetErrorCount();

				using MetricsWriter writer = new(reportDir);
				writer.Export(client.GetMetricsSnapshot());

				client.Cluster.AddRetry();
				client.Cluster.AddDelayQueueTimeout();
				node.AddError(SuiteHelpers.ns);

				Assert.AreEqual(
					retryCount + 1,
					client.GetClusterStats().RetryCount);
				Assert.AreEqual(errorCount + 1, node.GetErrorCount());
				writer.Export(client.GetMetricsSnapshot());

				string[] lines = File.ReadAllLines(
					Directory.GetFiles(reportDir, "metrics-*.log").Single());
				Assert.AreEqual(3, lines.Length);
				Assert.AreEqual(
					RemoveTimestamp(lines[1]),
					RemoveTimestamp(lines[2]));
			}
			finally
			{
				if (client.Cluster.MetricsEnabled)
				{
					client.DisableMetrics();
				}
				if (Directory.Exists(reportDir))
				{
					Directory.Delete(reportDir, true);
				}
			}
		}

		[TestMethod]
		public void MetricsCompatibilityCountersUseCanonicalStoreCapture()
		{
			client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true,
				Sampler = MetricsSampler.FromProbability(1)
			});

			try
			{
				MetricsCompatibilitySnapshot before =
					client.GetMetricsSnapshot().Compatibility;

				client.Cluster.AddCommandCount();
				client.Cluster.AddRetry();
				client.Cluster.AddDelayQueueTimeout();

				MetricsCompatibilitySnapshot after =
					client.GetMetricsSnapshot().Compatibility;
				Assert.AreEqual(before.CommandCount, after.CommandCount);
				Assert.AreEqual(before.RetryCount, after.RetryCount);
				Assert.AreEqual(
					before.DelayQueueTimeoutCount + 1,
					after.DelayQueueTimeoutCount);
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsExternalConnectionLatencyUsesNodeAndLegacyConnScopes()
		{
			client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true
			});

			try
			{
				Node node = client.Cluster.Nodes[0];
				node.GetMetrics().AddLatency(
					"must-not-be-a-namespace",
					Latency.LatencyType.CONN,
					1);

				MetricsSnapshot snapshot = client.GetMetricsSnapshot();
				NodeMetricsSnapshot canonical = snapshot.Nodes.Single(
					item => item.Name == node.Name);
				MetricsNodeCompatibilitySnapshot compatibility =
					snapshot.Compatibility.Nodes.Single(
						item => item.Name == node.Name);
				MetricsNamespaceCompatibilitySnapshot legacy =
					compatibility.Namespaces.Single(
						item => item.Name == string.Empty);

				Assert.AreEqual(1L, canonical.Latency.Connection.Sum());
				Assert.IsFalse(canonical.Namespaces.Any(
					item => item.Name == "must-not-be-a-namespace"));
				Assert.AreEqual(
					1L,
					legacy.Latency[(int)Latency.LatencyType.CONN].Sum());
				Assert.IsFalse(compatibility.Namespaces.Any(item =>
					item.Name != string.Empty &&
					item.Latency[(int)Latency.LatencyType.CONN].Sum() != 0));

				ConnectionMetricsHandle connection =
					client.Cluster.StartConnectionMetrics(node);
				connection.Complete(node, null);
				MetricsSnapshot afterConnection =
					client.GetMetricsSnapshot();
				NodeMetricsSnapshot canonicalAfterConnection =
					afterConnection.Nodes.Single(
						item => item.Name == node.Name);
				Assert.AreEqual(
					2L,
					canonicalAfterConnection.Latency.Connection.Sum(),
					"A successful connection must add exactly one sample.");
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsSnapshotCopiesExternalMutationsOnceForBothPayloads()
		{
			client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None,
				OperationalEnabled = true
			});

			try
			{
				Node node = client.Cluster.Nodes[0];
				NodeMetrics metrics = node.GetMetrics();
				const string ns = "external-copy";
				Task writer = Task.Run(() =>
				{
					for (int i = 0; i < 500; i++)
					{
						metrics.BytesInCounter.Increment(ns);
						metrics.AddLatency(
							ns,
							Latency.LatencyType.READ,
							1);
						metrics.AddLatency(
							ns,
							Latency.LatencyType.CONN,
							1);
					}
				});

				for (int i = 0; i < 25; i++)
				{
					MetricsSnapshot snapshot = client.GetMetricsSnapshot();
					NodeMetricsSnapshot canonical = snapshot.Nodes.Single(
						item => item.Name == node.Name);
					MetricsNodeCompatibilitySnapshot compatibility =
						snapshot.Compatibility.Nodes.Single(
							item => item.Name == node.Name);
					NamespaceMetricsSnapshot canonicalNs =
						canonical.Namespaces.SingleOrDefault(
							item => item.Name == ns);
					MetricsNamespaceCompatibilitySnapshot legacyNs =
						compatibility.Namespaces.SingleOrDefault(
							item => item.Name == ns);

					Assert.AreEqual(
						canonicalNs?.BytesIn ?? 0,
						legacyNs?.BytesIn ?? 0);
					Assert.AreEqual(
						canonicalNs?.Latency.Read.Sum() ?? 0,
						legacyNs?.Latency[
							(int)Latency.LatencyType.READ].Sum() ?? 0);
					Assert.AreEqual(
						canonical.Latency.Connection.Sum(),
						compatibility.Namespaces
							.Where(item => item.Name == string.Empty)
							.SelectMany(item =>
								item.Latency[
									(int)Latency.LatencyType.CONN])
							.Sum());
				}
				writer.GetAwaiter().GetResult();

				MetricsSnapshot copied = client.GetMetricsSnapshot();
				NodeMetricsSnapshot copiedNode = copied.Nodes.Single(
					item => item.Name == node.Name);
				MetricsNodeCompatibilitySnapshot copiedCompatibility =
					copied.Compatibility.Nodes.Single(
						item => item.Name == node.Name);
				long copiedBytes = copiedNode.Namespaces.Single(
					item => item.Name == ns).BytesIn;
				long copiedConnections =
					copiedNode.Latency.Connection.Sum();
				long copiedLegacyBytes =
					copiedCompatibility.Namespaces.Single(
						item => item.Name == ns).BytesIn;
				long copiedLegacyConnections =
					copiedCompatibility.Namespaces.Single(
						item => item.Name == string.Empty)
						.Latency[(int)Latency.LatencyType.CONN].Sum();

				metrics.BytesInCounter.Increment(ns, 10);
				metrics.AddLatency(ns, Latency.LatencyType.CONN, 1);

				Assert.AreEqual(
					copiedBytes,
					copiedNode.Namespaces.Single(
						item => item.Name == ns).BytesIn);
				Assert.AreEqual(
					copiedConnections,
					copiedNode.Latency.Connection.Sum());
				Assert.AreEqual(
					copiedLegacyBytes,
					copiedCompatibility.Namespaces.Single(
						item => item.Name == ns).BytesIn);
				Assert.AreEqual(
					copiedLegacyConnections,
					copiedCompatibility.Namespaces.Single(
						item => item.Name == string.Empty)
							.Latency[(int)Latency.LatencyType.CONN].Sum());
				Assert.AreEqual(500L, copiedBytes);
				Assert.AreEqual(500L, copiedConnections);
				Assert.AreEqual(500L, copiedLegacyBytes);
				Assert.AreEqual(500L, copiedLegacyConnections);
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsShapeReloadPreservesLegacyByteCounters()
		{
			using ConfigClientScope scope = ConfigClientScope.Create("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				  metrics:
				    enabled: true
				    exporter: none
				    latency_columns: 3
				    extended:
				      operational:
				        enabled: true
				""");
			Node node = scope.Client.Cluster.Nodes[0];
			NodeMetrics metrics = node.GetMetrics();
			metrics.BytesInCounter.Increment(SuiteHelpers.ns, 42);

			scope.RewriteYaml("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				  metrics:
				    enabled: true
				    exporter: none
				    latency_columns: 5
				    extended:
				      operational:
				        enabled: true
				""");

			Assert.AreSame(metrics, node.GetMetrics());
			Assert.AreEqual(
				42L,
				node.GetMetrics().BytesInCounter.GetCountByNS(
					SuiteHelpers.ns));
			Assert.AreEqual(
				5,
				scope.Client.Cluster.MetricsPolicy.LatencyColumns);
		}

		[TestMethod]
		public void NodeMetricsRemainMutableAndIdentityStableUntilDisable()
		{
			client.DisableMetrics();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.None,
				LatencyColumns = 3
			};
			client.EnableMetrics(policy);
			Node node = client.Cluster.Nodes[0];
			NodeMetrics metrics = node.GetMetrics();

			Assert.IsNotNull(metrics);
			metrics.BytesInCounter.Increment(SuiteHelpers.ns, 17);
			metrics.AddLatency(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				1);
			Histograms original = metrics.Histograms;

			node.ReshapeMetrics(new MetricsPolicy(policy)
			{
				LatencyColumns = 5
			});

			Assert.AreSame(metrics, node.GetMetrics());
			Assert.AreNotSame(original, metrics.Histograms);
			Assert.AreEqual(
				17L,
				metrics.BytesInCounter.GetCountByNS(SuiteHelpers.ns));
			metrics.AddLatency(
				SuiteHelpers.ns,
				Latency.LatencyType.READ,
				1);

			client.DisableMetrics();

			Assert.IsNull(node.GetMetrics());
			Assert.IsNull(metrics.BytesInCounter);
			Assert.IsNull(metrics.BytesOutCounter);
			Assert.IsNull(metrics.Histograms);
		}

		[TestMethod]
		public void ExplicitMetricsReconfigurePreservesNodeBucketIdentity()
		{
			client.DisableMetrics();
			MetricsPolicy originalPolicy = new()
			{
				Exporter = MetricsExporterMode.None,
				LatencyColumns = 3
			};
			client.EnableMetrics(originalPolicy);
			Node node = client.Cluster.Nodes[0];
			NodeMetrics metrics = node.GetMetrics();
			Histograms originalHistograms = metrics.Histograms;
			metrics.BytesInCounter.Increment(SuiteHelpers.ns, 23);

			client.EnableMetrics(new MetricsPolicy(originalPolicy)
			{
				ExportInterval = TimeSpan.FromSeconds(2)
			});

			Assert.AreSame(metrics, node.GetMetrics());
			Assert.AreSame(originalHistograms, metrics.Histograms);
			Assert.AreEqual(
				23L,
				metrics.BytesInCounter.GetCountByNS(SuiteHelpers.ns));

			client.EnableMetrics(new MetricsPolicy(originalPolicy)
			{
				LatencyColumns = 5
			});

			Assert.AreSame(metrics, node.GetMetrics());
			Assert.AreNotSame(originalHistograms, metrics.Histograms);
			Assert.AreEqual(
				23L,
				metrics.BytesInCounter.GetCountByNS(SuiteHelpers.ns));
			client.DisableMetrics();
		}

		[TestMethod]
		public void PublicNodeMetricsMutationsFeedSnapshotsAndLegacyOutput()
		{
			client.DisableMetrics();
			string reportDir = Path.Combine(
				Path.GetTempPath(),
				$"as-metrics-public-{Guid.NewGuid():N}");
			client.EnableMetrics(new MetricsPolicy
			{
				Exporter = MetricsExporterMode.None,
				LatencyColumns = 3
			});

			try
			{
				Node node = client.Cluster.Nodes[0];
				NodeMetrics metrics = node.GetMetrics();
				const string bytesNamespace = "metrics-bytes-only";
				const string latencyNamespace = "metrics-latency-only";
				const string counterNamespace = "metrics-counter-only";
				metrics.BytesInCounter.Increment(bytesNamespace, 31);
				metrics.BytesOutCounter.Increment(bytesNamespace, 37);
				metrics.AddLatency(
					latencyNamespace,
					Latency.LatencyType.READ,
					1);
				node.AddError(counterNamespace);

				MetricsSnapshot snapshot = client.GetMetricsSnapshot();
				NodeMetricsSnapshot operational = snapshot.Nodes.Single(
					current => current.Name == node.Name);
				NamespaceMetricsSnapshot bytes = operational.Namespaces.Single(
					current => current.Name == bytesNamespace);
				NamespaceMetricsSnapshot latency = operational.Namespaces.Single(
					current => current.Name == latencyNamespace);
				Assert.AreEqual(31L, bytes.BytesIn);
				Assert.AreEqual(37L, bytes.BytesOut);
				Assert.AreEqual(1L, latency.Latency.Read.Sum());

				MetricsNodeCompatibilitySnapshot compatibility =
					snapshot.Compatibility.Nodes.Single(
						current => current.Name == node.Name);
				CollectionAssert.IsSubsetOf(
					new[]
					{
						bytesNamespace,
						latencyNamespace,
						counterNamespace
					},
					compatibility.Namespaces
						.Select(current => current.Name)
						.ToArray());
				Assert.AreEqual(
					1L,
					compatibility.Namespaces.Single(
						current => current.Name == counterNamespace).Errors);

				using MetricsWriter writer = new(reportDir);
				writer.Export(snapshot);
				string output = File.ReadAllText(
					Directory.GetFiles(reportDir, "metrics-*.log").Single());
				StringAssert.Contains(output, bytesNamespace);
				StringAssert.Contains(output, latencyNamespace);
				StringAssert.Contains(output, counterNamespace);
			}
			finally
			{
				client.DisableMetrics();
				if (Directory.Exists(reportDir))
				{
					Directory.Delete(reportDir, true);
				}
			}
		}

		[TestMethod]
		public void MetricsReporterSuspendsAfterThreeFailures()
		{
			RecoveringExporter exporter = new();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromMilliseconds(20)
			};
			policy.AddExporter(exporter);

			client.EnableMetrics(policy);

			try
			{
				Assert.IsTrue(exporter.Recovered.Wait(TimeSpan.FromSeconds(5)));
				Assert.AreEqual(4, exporter.Count);
			}
			finally
			{
				client.DisableMetrics();
			}

			Assert.AreEqual(5, exporter.Count);
		}

		[TestMethod]
		public void MetricsReporterExplicitEnableResetsSuspension()
		{
			RecoveringExporter exporter = new();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromMilliseconds(250)
			};
			policy.AddExporter(exporter);
			client.EnableMetrics(policy);

			try
			{
				Assert.IsTrue(SpinWait.SpinUntil(
					() => exporter.Count >= 3,
					TimeSpan.FromSeconds(5)));

				client.EnableMetrics(policy);

				Assert.IsTrue(
					exporter.Recovered.Wait(TimeSpan.FromMilliseconds(700)),
					"Explicit activation should clear the prior failure state.");
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsReporterUsesDeterministicFixedDelayScheduling()
		{
			ManualMetricsScheduler scheduler = new();
			DeterministicExporter first = new();
			DeterministicExporter current = first;
			MetricsReporter reporter = new(
				new MetricsReporterConfig(TimeSpan.FromMilliseconds(100)),
				() =>
				{
					current.Export(client.GetMetricsSnapshot());
					current.Acknowledged++;
				},
				snapshot =>
				{
					current.Export(snapshot);
					current.Acknowledged++;
					return true;
				},
				scheduler);

			reporter.Start();
			try
			{
				reporter.Resume();
				int waitVersion = scheduler.WaitFor(100, 0);

				scheduler.Advance(99);
				waitVersion = scheduler.WaitFor(1, waitVersion);
				Assert.AreEqual(0, first.Count);

				scheduler.Advance(1);
				first.WaitForCount(1);
				waitVersion = scheduler.WaitFor(100, waitVersion);
				Assert.AreEqual(1, first.Acknowledged);

				scheduler.Advance(350);
				first.WaitForCount(2);
				waitVersion = scheduler.WaitFor(100, waitVersion);
				Assert.AreEqual(
					2,
					first.Count,
					"Missed ticks must coalesce into one export.");

				first.OnExport = () => scheduler.Elapse(40);
				scheduler.Advance(100);
				first.WaitForCount(3);
				waitVersion = scheduler.WaitFor(100, waitVersion);
				first.OnExport = null;

				reporter.Reconfigure(new MetricsReporterConfig(
					TimeSpan.FromMilliseconds(200)));
				waitVersion = scheduler.WaitFor(200, waitVersion);

				DeterministicExporter second = new();
				current = second;
				reporter.Reconfigure(new MetricsReporterConfig(
					TimeSpan.FromMilliseconds(200)));
				scheduler.Advance(200);
				second.WaitForCount(1);
				waitVersion = scheduler.WaitFor(200, waitVersion);
				Assert.AreEqual(3, first.Count);

				reporter.Quiesce();
				scheduler.Advance(500);
				Assert.AreEqual(1, second.Count);
				reporter.Resume();
				second.WaitForCount(2);

				reporter.Reconfigure(new MetricsReporterConfig(
					TimeSpan.FromDays(100)));
				scheduler.WaitFor(int.MaxValue, waitVersion);
				Assert.AreEqual(1, second.MaximumConcurrent);
			}
			finally
			{
				reporter.Stop();
			}
		}

		[TestMethod]
		public void MetricsReporterInvokesManagerOwnedFinalCallback()
		{
			ManualMetricsScheduler scheduler = new();
			DeterministicExporter exporter = new();
			int acknowledged = 0;
			MetricsReporter reporter = new(
				new MetricsReporterConfig(TimeSpan.FromMilliseconds(10)),
				() =>
				{
					exporter.Export(client.GetMetricsSnapshot());
					acknowledged++;
				},
				snapshot =>
				{
					exporter.Export(snapshot);
					acknowledged++;
					return true;
				},
				scheduler);

			reporter.Start();
			try
			{
				reporter.Resume();
				int waitVersion = scheduler.WaitFor(10, 0);
				scheduler.Advance(10);
				exporter.WaitForCount(1);
				waitVersion = scheduler.WaitFor(10, waitVersion);

				Assert.IsTrue(
					reporter.ExportFinal(client.GetMetricsSnapshot()));
				Assert.AreEqual(2, exporter.Count);
				Assert.AreEqual(2, acknowledged);
				waitVersion = scheduler.WaitFor(10, waitVersion);

				scheduler.Advance(10);
				exporter.WaitForCount(3);
				waitVersion = scheduler.WaitFor(10, waitVersion);
				Assert.AreEqual(3, acknowledged);
				Assert.AreEqual(1, exporter.MaximumConcurrent);
			}
			finally
			{
				reporter.Stop();
			}
		}

		[TestMethod]
		public void MetricsReporterRetainsSuspensionAcrossModeChanges()
		{
			using ConfigClientScope scope = ConfigClientScope.Create("""
				version: 1.2.0
				dynamic:
				  client:
				    tend_interval: 1000
				""");
			RecoveringExporter exporter = new();
			MetricsPolicy custom = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromMilliseconds(80)
			};
			custom.AddExporter(exporter);

			scope.Client.EnableMetrics(custom);

			Assert.IsTrue(SpinWait.SpinUntil(
				() => exporter.Count >= 3,
				TimeSpan.FromSeconds(5)));

			scope.RewriteYaml(MetricsConfigYaml("none", "80ms"));
			Thread.Sleep(200);
			Assert.AreEqual(3, exporter.Count);

			scope.RewriteYaml(MetricsConfigYaml("custom", "80ms"));
			Thread.Sleep(180);
			Assert.AreEqual(
				3,
				exporter.Count,
				"Three suspended eligible cycles should survive a mode change.");
			Assert.IsTrue(exporter.Recovered.Wait(TimeSpan.FromSeconds(5)));
			Assert.AreEqual(4, exporter.Count);
		}

		[TestMethod]
		public void MetricsReporterRejectsLifecycleReentrancyButAllowsPull()
		{
			ReentrantExporter exporter = new(client);
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromMilliseconds(25)
			};
			policy.AddExporter(exporter);

			client.EnableMetrics(policy);

			try
			{
				Assert.IsTrue(exporter.Completed.Wait(TimeSpan.FromSeconds(5)));
				Assert.IsNotNull(exporter.PullSnapshot);
				Assert.IsInstanceOfType<AerospikeException>(
					exporter.LifecycleException);
				Assert.IsTrue(client.Cluster.MetricsEnabled);
			}
			finally
			{
				client.DisableMetrics();
			}
		}

		[TestMethod]
		public void MetricsFinalExporterDoesNotHoldClusterMetricsLock()
		{
			BlockingExporter exporter = new();
			MetricsPolicy policy = new()
			{
				Exporter = MetricsExporterMode.Custom,
				ExportInterval = TimeSpan.FromHours(1)
			};
			policy.AddExporter(exporter);
			client.EnableMetrics(policy);
			Task disable = Task.Run(client.DisableMetrics);

			if (!exporter.Entered.Wait(TimeSpan.FromSeconds(5)))
			{
				exporter.Release.Set();
				disable.Wait(TimeSpan.FromSeconds(5));
				Assert.Fail("Final exporter was not invoked.");
			}
			bool lockTaken = false;

			try
			{
				Monitor.TryEnter(
					client.Cluster.metricsLock,
					TimeSpan.FromSeconds(1),
					ref lockTaken);
				Assert.IsTrue(
					lockTaken,
					"Exporter callback must not execute while metricsLock is held.");
				Assert.IsTrue(client.GetMetricsSnapshot().MetricsEnabled);
			}
			finally
			{
				if (lockTaken)
				{
					Monitor.Exit(client.Cluster.metricsLock);
				}
				exporter.Release.Set();
			}

			Assert.IsTrue(disable.Wait(TimeSpan.FromSeconds(5)));
			Assert.IsFalse(client.Cluster.MetricsEnabled);
		}

		[TestMethod]
		public void MetricsSnapshotDefaultInterfaceMemberIsVirtual()
		{
			MethodInfo method = typeof(IAerospikeClient).GetMethod(
				nameof(IAerospikeClient.GetMetricsSnapshot));

			Assert.IsNotNull(method);
			Assert.IsTrue(method.IsVirtual);
			Assert.IsFalse(method.IsAbstract);
		}

		[TestMethod]
		public void MetricsSnapshotDefaultInterfaceDispatchSupportsOldImplementerShape()
		{
			AssemblyBuilder assembly = AssemblyBuilder.DefineDynamicAssembly(
				new AssemblyName("MetricsDefaultInterfaceFixture"),
				AssemblyBuilderAccess.Run);
			TypeBuilder type = assembly.DefineDynamicModule("fixture")
				.DefineType(
					"LegacyAerospikeClient",
					TypeAttributes.Public | TypeAttributes.Sealed);
			type.AddInterfaceImplementation(typeof(IAerospikeClient));
			type.DefineDefaultConstructor(MethodAttributes.Public);

			foreach (MethodInfo method in typeof(IAerospikeClient).GetMethods())
			{
				if (!method.IsAbstract)
				{
					continue;
				}

				Type[] parameters = method.GetParameters()
					.Select(parameter => parameter.ParameterType)
					.ToArray();
				MethodBuilder implementation = type.DefineMethod(
					method.Name,
					MethodAttributes.Public |
						MethodAttributes.Virtual |
						MethodAttributes.Final |
						MethodAttributes.HideBySig |
						MethodAttributes.NewSlot,
					method.ReturnType,
					parameters);
				ILGenerator il = implementation.GetILGenerator();
				EmitDefaultReturn(il, method.ReturnType);
				type.DefineMethodOverride(implementation, method);
			}

			IAerospikeClient legacy =
				(IAerospikeClient)Activator.CreateInstance(type.CreateType());
			AssertThrows<NullReferenceException>(() =>
				legacy.GetMetricsSnapshot());
			InterfaceMapping map =
				legacy.GetType().GetInterfaceMap(typeof(IAerospikeClient));
			int index = Array.FindIndex(
				map.InterfaceMethods,
				method => method.Name ==
					nameof(IAerospikeClient.GetMetricsSnapshot));
			Assert.IsTrue(index >= 0);
			Assert.AreEqual(
				typeof(IAerospikeClient),
				map.TargetMethods[index].DeclaringType);
		}

		private static void AssertPublicShape<T>(
			params (string Name, Type Type)[] expected)
		{
			Type type = typeof(T);
			Assert.IsTrue(type.IsSealed, $"{type.Name} must be immutable and sealed.");
			PropertyInfo[] properties = type.GetProperties(
				BindingFlags.Instance | BindingFlags.Public);
			CollectionAssert.AreEquivalent(
				expected.Select(item => item.Name).ToArray(),
				properties.Select(property => property.Name).ToArray(),
				$"Unexpected public properties on {type.Name}.");

			foreach ((string name, Type expectedType) in expected)
			{
				PropertyInfo property = properties.Single(
					current => current.Name == name);
				Assert.AreEqual(
					expectedType,
					property.PropertyType,
					$"Unexpected type for {type.Name}.{name}.");
				Assert.IsNull(
					property.SetMethod,
					$"{type.Name}.{name} must not have a public setter.");
			}
		}

		private static MetricsNodeGeneration AddDeparture(
			MetricsStore store,
			MetricsRuntimeConfig config,
			MetricsNodeNameState state,
			string name,
			string address,
			int port,
			int errorRate,
			MetricsNodeCollector collector,
			MetricsNodeCompatibilitySnapshot compatibility)
		{
			MetricsNodeGeneration generation =
				new(store, name, config)
				{
					Collector = collector
				};
			state.Add(generation);
			store.RecordNodeDeparture(
				name,
				address,
				port,
				errorRate,
				state,
				compatibility);
			return generation;
		}

		[TestMethod]
		public void MetricsStoreDepartureAcknowledgePreservesLateWrites()
		{
			MetricsPolicy policy = new();
			MetricsRuntimeConfig config = new(policy);
			MetricsNodeCollector collector = new(config);
			collector.AddOpened();
			MetricsStore store = new(policy);
			MetricsNodeNameState state = new();
			AddDeparture(
				store,
				config,
				state,
				"N1",
				"127.0.0.1",
				3000,
				0,
				collector,
				CreateLegacyNode("N1", policy));

			MetricsDepartureCapture first = store.CaptureDepartures(config);
			Assert.AreEqual(1, first.Nodes.Count);
			Assert.AreEqual(1L, first.Nodes[0].View.Opened);

			collector.AddClosed();
			store.AcknowledgeDepartures(first.Token);

			MetricsDepartureCapture second = store.CaptureDepartures(config);
			Assert.AreEqual(1, second.Nodes.Count);
			Assert.AreEqual(0L, second.Nodes[0].View.Opened);
			Assert.AreEqual(1L, second.Nodes[0].View.Closed);
			Assert.IsNotNull(
				second.Nodes[0].Compatibility,
				"Late canonical values must retain their cumulative legacy payload.");

			store.AcknowledgeDepartures(second.Token);
			Assert.AreEqual(0, store.CaptureDepartures(config).Nodes.Count);

			collector.AddOpened();
			MetricsDepartureCapture third = store.CaptureDepartures(config);
			Assert.AreEqual(1L, third.Nodes.Single().View.Opened);
			store.AcknowledgeDepartures(third.Token);
			Assert.AreEqual(0, store.CaptureDepartures(config).Nodes.Count);

			collector.AddClosed();
			MetricsDepartureCapture fourth = store.CaptureDepartures(config);
			Assert.AreEqual(0L, fourth.Nodes.Single().View.Opened);
			Assert.AreEqual(1L, fourth.Nodes.Single().View.Closed);
		}

		[TestMethod]
		public void MetricsStoreCapsDepartedNamesAndSignalsTruncation()
		{
			MetricsPolicy policy = new();
			MetricsRuntimeConfig config = new(policy);
			MetricsStore store = new(policy);
			MetricsNodeNameState firstState = null;

			for (int i = 0; i < 257; i++)
			{
				MetricsNodeCollector collector = new(config);
				collector.AddOpened();
				MetricsNodeNameState state = new();
				firstState ??= state;
				AddDeparture(
					store,
					config,
					state,
					"N" + i,
					"127.0.0.1",
					3000,
					0,
					collector,
					CreateLegacyNode("N" + i, policy));
			}

			MetricsDepartureCapture capture = store.CaptureDepartures(config);
			Assert.AreEqual(256, capture.Nodes.Count);
			Assert.IsTrue(capture.PossiblyTruncated);

			store.AcknowledgeDepartures(capture.Token);
			MetricsDepartureCapture after = store.CaptureDepartures(config);
			Assert.IsFalse(after.PossiblyTruncated);

			MetricsNodeCollector replacement = new(config);
			replacement.AddOpened();
			AddDeparture(
				store,
				config,
				new MetricsNodeNameState(),
				"N257",
				"127.0.0.1",
				3000,
				0,
				replacement,
				CreateLegacyNode("N257", policy));
			MetricsDepartureCapture recovered =
				store.CaptureDepartures(config);
			Assert.IsTrue(recovered.Nodes.Any(node => node.Name == "N257"));
			Assert.IsNotNull(firstState);
			Assert.AreEqual(
				1L,
				firstState.Read("N0", config).Opened,
				"Acknowledgement must release retention capacity, not cumulative name state.");
		}

		[TestMethod]
		public void MetricsStoreAggregatesRepeatedDepartureNames()
		{
			MetricsPolicy policy = new();
			MetricsRuntimeConfig config = new(policy);
			MetricsStore store = new(policy);
			MetricsNodeNameState state = new();

			for (int i = 0; i < 257; i++)
			{
				MetricsNodeCollector collector = new(config);
				collector.AddOpened();
				AddDeparture(
					store,
					config,
					state,
					"N1",
					"127.0.0.1",
					3000,
					0,
					collector,
					CreateLegacyNode("N1", policy));
			}

			MetricsDepartureCapture capture = store.CaptureDepartures(config);
			Assert.AreEqual(1, capture.Nodes.Count);
			Assert.AreEqual(257L, capture.Nodes.Single().View.Opened);
			Assert.IsFalse(capture.PossiblyTruncated);

			store.AcknowledgeDepartures(capture.Token);
			MetricsDepartureCapture after = store.CaptureDepartures(config);
			Assert.AreEqual(0, after.Nodes.Count);
			Assert.IsFalse(after.PossiblyTruncated);
		}

		[TestMethod]
		public void MetricsStoreRejoinHidesDepartureAndRejectsStaleWatermark()
		{
			MetricsPolicy policy = new();
			MetricsRuntimeConfig config = new(policy);
			MetricsStore store = new(policy);
			MetricsNodeNameState state = new();
			MetricsNodeCollector firstCollector = new(config);
			firstCollector.AddOpened();
			AddDeparture(
				store,
				config,
				state,
				"N1",
				"127.0.0.1",
				3000,
				0,
				firstCollector,
				CreateLegacyNode("N1", policy));
			MetricsDepartureCapture departed =
				store.CaptureDepartures(config);

			MetricsNodeCollector secondCollector = new(config);
			secondCollector.AddOpened();
			MetricsNodeGeneration second = AddDeparture(
				store,
				config,
				state,
				"N1",
				"127.0.0.2",
				3000,
				0,
				secondCollector,
				CreateLegacyNode("N1", policy));
			state.Admit(second);
			store.AcknowledgeDepartures(departed.Token);

			Assert.AreEqual(
				0,
				store.CaptureDepartures(config).Nodes.Count,
				"An active same-name generation must hide departed output.");
			Assert.AreEqual(2L, state.Read("N1", config).Opened);

			state.Depart(second);
			store.RecordNodeDeparture(
				"N1",
				"127.0.0.2",
				3000,
				0,
				state,
				CreateLegacyNode("N1", policy));
			store.AcknowledgeDepartures(departed.Token);

			MetricsDepartureCapture repeated =
				store.CaptureDepartures(config);
			Assert.AreEqual(1, repeated.Nodes.Count);
			Assert.AreEqual(2L, repeated.Nodes.Single().View.Opened);
		}

		[TestMethod]
		public void MetricsDepartureShapeChangePreservesScalarsAndResetsHistograms()
		{
			MetricsPolicy originalPolicy = new()
			{
				LatencyColumns = 3,
				LatencyShift = 1,
				LatencyUnit = MetricsLatencyUnit.Milliseconds
			};
			MetricsRuntimeConfig original = new(
				originalPolicy,
				3,
				new MetricsHistogramShape(
					2,
					MetricsLatencyUnit.Microseconds,
					4,
					1));
			MetricsNodeCollector collector = new(original);
			MetricsNamespaceCollector ns = collector.GetNamespace(
				"test",
				original.Shape);
			ns.AddFailure(MetricsFailure.Error);
			ns.AddLatency(
				Latency.LatencyType.READ,
				5_000_000,
				original.Shape);
			MetricsNamespaceCompatibilitySnapshot compatibilityNs = new(
				"test",
				1,
				0,
				0,
				0,
				0,
				Enumerable.Range(0, Latency.GetMax())
					.Select(_ => (IReadOnlyList<long>)
						Array.AsReadOnly(new[] { 1L, 2L, 3L })));
			MetricsStore store = new(originalPolicy);
			AddDeparture(
				store,
				original,
				new MetricsNodeNameState(),
				"N1",
				"127.0.0.1",
				3000,
				0,
				collector,
				new MetricsNodeCompatibilitySnapshot(
					"N1",
					"127.0.0.1",
					3000,
					new ConnectionStats(0, 0, 0, 0),
					new ConnectionStats(0, 0, 0, 0),
					[compatibilityNs],
					originalPolicy.LatencyUnit,
					originalPolicy.LatencyColumns,
					originalPolicy.LatencyShift));

			MetricsPolicy replacementPolicy = new(originalPolicy)
			{
				LatencyColumns = 5,
				LatencyShift = 2,
				LatencyUnit = MetricsLatencyUnit.Microseconds
			};
			MetricsDepartureCapture capture =
				store.CaptureDepartures(
					new MetricsRuntimeConfig(replacementPolicy));
			MetricsDepartedNodeCapture departed = capture.Nodes.Single();
			MetricsNamespaceView canonical =
				departed.View.Namespaces["test"];

			Assert.AreEqual(1L, canonical.Errors);
			Assert.IsTrue(canonical.Latency.Values.All(
				buckets => buckets.Count == 5 &&
					buckets.All(value => value == 0)));
			Assert.AreEqual(
				MetricsLatencyUnit.Microseconds,
				departed.Compatibility.LatencyUnit);
			Assert.AreEqual(5, departed.Compatibility.LatencyColumns);
			Assert.AreEqual(2, departed.Compatibility.LatencyShift);
			Assert.IsTrue(departed.Compatibility.Namespaces.Single().Latency.All(
				buckets => buckets.Count == 5 &&
					buckets.All(value => value == 0)));
		}

		[TestMethod]
		public void MetricsDepartureRepeatedNameUsesCurrentLegacyShape()
		{
			MetricsPolicy oldPolicy = new()
			{
				LatencyColumns = 3,
				LatencyShift = 1
			};
			MetricsPolicy currentPolicy = new()
			{
				LatencyColumns = 5,
				LatencyShift = 2
			};
			MetricsRuntimeConfig oldConfig = new(oldPolicy);
			MetricsRuntimeConfig currentConfig = new(currentPolicy);
			MetricsNodeCollector oldCollector = new(oldConfig);
			MetricsNodeCollector currentCollector = new(currentConfig);
			oldCollector.AddOpened();
			currentCollector.AddOpened();
			MetricsStore store = new(currentPolicy);
			MetricsNodeNameState state = new();

			AddDeparture(
				store,
				oldConfig,
				state,
				"N1",
				"127.0.0.1",
				3000,
				0,
				oldCollector,
				CreateLegacyNodeWithNamespace("N1", oldPolicy, 2, 1));
			AddDeparture(
				store,
				currentConfig,
				state,
				"N1",
				"127.0.0.2",
				3000,
				0,
				currentCollector,
				CreateLegacyNodeWithNamespace("N1", currentPolicy, 3, 2));

			MetricsNodeCompatibilitySnapshot compatibility =
				store.CaptureDepartures(
					currentConfig).Nodes.Single().Compatibility;
			MetricsNamespaceCompatibilitySnapshot ns =
				compatibility.Namespaces.Single();

			Assert.AreEqual(5L, ns.Errors);
			Assert.IsTrue(ns.Latency.All(
				buckets => buckets.Count == 5 &&
					buckets.All(value => value == 2)));
		}

		[TestMethod]
		public void MetricsWriterPreservesHeader2GoldenFormat()
		{
			string reportDir = Path.Combine(
				Path.GetTempPath(),
				$"as-metrics-golden-{Guid.NewGuid():N}");
			Directory.CreateDirectory(reportDir);

			try
			{
				Dictionary<string, string> labels = new()
				{
					["region"] = "west",
					["zone"] = "1"
				};
				IReadOnlyList<IReadOnlyList<long>> latency =
				Array.AsReadOnly<IReadOnlyList<long>>(
				[
					Array.AsReadOnly([1L, 2L, 3L]),
					Array.AsReadOnly([4L, 5L, 6L]),
					Array.AsReadOnly([7L, 8L, 9L]),
					Array.AsReadOnly([10L, 11L, 12L]),
					Array.AsReadOnly([13L, 14L, 15L])
				]);
				MetricsNamespaceCompatibilitySnapshot ns = new(
					"test",
					9,
					10,
					11,
					12,
					13,
					latency);
				MetricsNodeCompatibilitySnapshot active = new(
					"N1",
					"127.0.0.1",
					3000,
					new ConnectionStats(1, 2, 3, 4),
					new ConnectionStats(5, 6, 7, 8),
					[ns],
					MetricsLatencyUnit.Milliseconds,
					3,
					1);
				MetricsNodeCompatibilitySnapshot departed = new(
					"N2",
					"127.0.0.2",
					3001,
					new ConnectionStats(16, 17, 18, 19),
					new ConnectionStats(20, 21, 22, 23),
					Array.Empty<MetricsNamespaceCompatibilitySnapshot>(),
					MetricsLatencyUnit.Milliseconds,
					3,
					1);
				MetricsCompatibilitySnapshot compatibility = new(
					"cluster",
					"9.9.9",
					"app",
					labels,
					12.75,
					34,
					5,
					6,
					7,
					8,
					9,
					10,
					11,
					[active],
					[departed]);
				NodeMetricsSnapshot activeNode = new(
					"N1",
					"127.0.0.1",
					3000,
					0,
					ConnectionMetricsSnapshot.Empty,
					NodeLatencyMetricsSnapshot.Empty,
					Array.Empty<NamespaceMetricsSnapshot>());
				NodeMetricsSnapshot departedNode = new(
					"N2",
					"127.0.0.2",
					3001,
					0,
					ConnectionMetricsSnapshot.Empty,
					NodeLatencyMetricsSnapshot.Empty,
					Array.Empty<NamespaceMetricsSnapshot>());
				MetricsSnapshot snapshot = new(
					DateTimeOffset.UtcNow,
					true,
					true,
					true,
					"csharp",
					"9.9.9",
					"cluster",
					"app",
					labels,
					MetricsLatencyUnit.Milliseconds,
					3,
					1,
					null,
					null,
					[activeNode],
					[departedNode],
					false,
					compatibility);

				using MetricsWriter writer = new(reportDir);
				writer.Export(snapshot);

				string[] lines = File.ReadAllLines(
					Directory.GetFiles(reportDir, "metrics-*.log").Single());
				Assert.AreEqual(3, lines.Length);
				Assert.AreEqual(
					" header(2) cluster[name,clientType,clientVersion,appId,label[],cpu,mem,recoverQueueSize,invalidNodeCount,commandCount,retryCount,delayQueueTimeoutCount,asyncThreadsInUse,asyncCompletionPortsInUse,node[]] label[name,value] node[name,address,port,syncConn,asyncConn,namespace[]] conn[inUse,inPool,opened,closed] namespace[name,errors,timeouts,keyBusy,bytesIn,bytesOut,latency[]] latency(3,1)[type[l1,l2,l3...]]",
					RemoveTimestamp(lines[0]));
				Assert.AreEqual(
					" cluster[cluster,c#,9.9.9,app,[[region,west],[zone,1]],12,34,5,6,7,8,9,10,11,[[N1,127.0.0.1,3000,2,1,3,4,6,5,7,8,[test,9,10,11,12,13,[conn[1,2,3],write[4,5,6],read[7,8,9],batch[10,11,12],query[13,14,15]]]]]]]]",
					RemoveTimestamp(lines[1]));
				Assert.AreEqual(
					" node[N2,127.0.0.2,3001,17,16,18,19,21,20,22,23,[]]]]",
					RemoveTimestamp(lines[2]));
			}
			finally
			{
				if (Directory.Exists(reportDir))
				{
					Directory.Delete(reportDir, true);
				}
			}
		}

		private static string RemoveTimestamp(string line)
		{
			Assert.IsTrue(line.Length >= 19);
			return line[19..];
		}

		private static string MetricsConfigYaml(string exporter, string interval)
		{
			return $"""
				version: 1.2.0
				dynamic:
				  metrics:
				    enabled: true
				    exporter: {exporter}
				    export_interval: {interval}
				""";
		}

		private static string MetricsIdentityYaml(
			string appId,
			string region,
			bool enabled)
		{
			return $"""
				version: 1.2.0
				dynamic:
				  client:
				    app_id: {appId}
				    tend_interval: 1000
				  metrics:
				    enabled: {enabled.ToString().ToLowerInvariant()}
				    exporter: none
				    labels:
				      region: {region}
				""";
		}

		private static void EmitDefaultReturn(ILGenerator il, Type returnType)
		{
			if (returnType == typeof(void))
			{
				il.Emit(OpCodes.Ret);
				return;
			}

			if (!returnType.IsValueType)
			{
				il.Emit(OpCodes.Ldnull);
				il.Emit(OpCodes.Ret);
				return;
			}

			LocalBuilder value = il.DeclareLocal(returnType);
			il.Emit(OpCodes.Ldloca_S, value);
			il.Emit(OpCodes.Initobj, returnType);
			il.Emit(OpCodes.Ldloc, value);
			il.Emit(OpCodes.Ret);
		}

		private static MetricsNodeCompatibilitySnapshot CreateLegacyNode(
			string name,
			MetricsPolicy policy)
		{
			return new MetricsNodeCompatibilitySnapshot(
				name,
				"127.0.0.1",
				3000,
				new ConnectionStats(0, 0, 0, 0),
				new ConnectionStats(0, 0, 0, 0),
				Array.Empty<MetricsNamespaceCompatibilitySnapshot>(),
				policy.LatencyUnit,
				policy.LatencyColumns,
				policy.LatencyShift);
		}

		private static MetricsNodeCompatibilitySnapshot CreateLegacyNodeWithNamespace(
			string name,
			MetricsPolicy policy,
			long errors,
			long latencyValue)
		{
			MetricsNamespaceCompatibilitySnapshot ns = new(
				"test",
				errors,
				0,
				0,
				0,
				0,
				Enumerable.Range(0, Latency.GetMax())
					.Select(_ => (IReadOnlyList<long>)Array.AsReadOnly(
						Enumerable.Repeat(
							latencyValue,
							policy.LatencyColumns).ToArray())));
			return new MetricsNodeCompatibilitySnapshot(
				name,
				"127.0.0.1",
				3000,
				new ConnectionStats(0, 0, 0, 0),
				new ConnectionStats(0, 0, 0, 0),
				[ns],
				policy.LatencyUnit,
				policy.LatencyColumns,
				policy.LatencyShift);
		}

		private static void AssertThrows<T>(Action action) where T : Exception
		{
			try
			{
				action();
				Assert.Fail("Expected " + typeof(T).Name);
			}
			catch (T)
			{
			}
		}

		private sealed class ManualMetricsScheduler
			: IMetricsReporterScheduler
		{
			private readonly object sync = new();
			private readonly ManualResetEventSlim waitChanged = new(false);
			private long timestamp;
			private bool signaled;
			private bool disposed;
			private int waitVersion;
			private int lastWait;

			public long Frequency => 1000;
			public long Timestamp
			{
				get
				{
					lock (sync)
					{
						return timestamp;
					}
				}
			}

			public void Set()
			{
				lock (sync)
				{
					signaled = true;
					Monitor.PulseAll(sync);
				}
			}

			public void Wait(int milliseconds)
			{
				lock (sync)
				{
					lastWait = milliseconds;
					waitVersion++;
					waitChanged.Set();

					while (!signaled && !disposed)
					{
						Monitor.Wait(sync);
					}
					signaled = false;
				}
			}

			internal void Advance(int milliseconds)
			{
				lock (sync)
				{
					timestamp += milliseconds;
					signaled = true;
					Monitor.PulseAll(sync);
				}
			}

			internal void Elapse(int milliseconds)
			{
				lock (sync)
				{
					timestamp += milliseconds;
				}
			}

			internal int WaitFor(int expected, int afterVersion)
			{
				DateTime deadline =
					DateTime.UtcNow + TimeSpan.FromSeconds(5);

				while (true)
				{
					lock (sync)
					{
						if (waitVersion > afterVersion &&
							lastWait == expected)
						{
							return waitVersion;
						}
					}

					TimeSpan remaining = deadline - DateTime.UtcNow;
					if (remaining <= TimeSpan.Zero ||
						!waitChanged.Wait(remaining))
					{
						Assert.Fail(
							$"Expected scheduler wait {expected} after version {afterVersion}.");
					}
					waitChanged.Reset();
				}
			}

			public void Dispose()
			{
				lock (sync)
				{
					disposed = true;
					Monitor.PulseAll(sync);
				}
				waitChanged.Dispose();
			}
		}

		private sealed class DeterministicExporter : IMetricsExporter
		{
			private readonly ManualResetEventSlim exported = new(false);
			private int count;
			private int concurrent;
			private int maximumConcurrent;

			internal int Count => Volatile.Read(ref count);
			internal int Acknowledged;
			internal int FailuresRemaining;
			internal Action OnExport;
			internal int MaximumConcurrent =>
				Volatile.Read(ref maximumConcurrent);

			public void Export(MetricsSnapshot snapshot)
			{
				int active = Interlocked.Increment(ref concurrent);
				UpdateMaximum(active);
				try
				{
					Interlocked.Increment(ref count);
					OnExport?.Invoke();
					if (Interlocked.Decrement(
						ref FailuresRemaining) >= 0)
					{
						throw new InvalidOperationException("expected");
					}
				}
				finally
				{
					Interlocked.Decrement(ref concurrent);
					exported.Set();
				}
			}

			internal void WaitForCount(int expected)
			{
				DateTime deadline =
					DateTime.UtcNow + TimeSpan.FromSeconds(5);

				while (Count < expected)
				{
					TimeSpan remaining = deadline - DateTime.UtcNow;
					if (remaining <= TimeSpan.Zero ||
						!exported.Wait(remaining))
					{
						Assert.Fail(
							$"Expected {expected} exports, observed {Count}.");
					}
					exported.Reset();
				}
			}

			private void UpdateMaximum(int active)
			{
				int observed;
				do
				{
					observed = Volatile.Read(
						ref maximumConcurrent);
					if (active <= observed)
					{
						return;
					}
				}
				while (Interlocked.CompareExchange(
					ref maximumConcurrent,
					active,
					observed) != observed);
			}
		}

		private sealed class BlockingRollbackListener
			: IMetricsListener
		{
			private int disableCount;
			internal readonly ManualResetEventSlim DisableEntered =
				new(false);
			internal readonly ManualResetEventSlim DisableRelease =
				new(false);
			internal int EnableCount;
			internal int NodeCloseCount;

			public void OnEnable(
				Cluster cluster,
				MetricsPolicy policy)
			{
				Interlocked.Increment(ref EnableCount);
			}

			public void OnSnapshot(Cluster cluster)
			{
			}

			public void OnNodeClose(Node node)
			{
				Interlocked.Increment(ref NodeCloseCount);
			}

			public void OnDisable(Cluster cluster)
			{
				if (Interlocked.Increment(ref disableCount) == 1)
				{
					DisableEntered.Set();
					DisableRelease.Wait();
					throw new InvalidOperationException("expected");
				}
			}
		}

		private sealed class EmptyMetricsListener : IMetricsListener
		{
			public void OnEnable(Cluster cluster, MetricsPolicy policy)
			{
			}

			public void OnSnapshot(Cluster cluster)
			{
			}

			public void OnNodeClose(Node node)
			{
			}

			public void OnDisable(Cluster cluster)
			{
			}
		}

		private sealed class CountingExporter : IMetricsExporter
		{
			private readonly ManualResetEventSlim exported = new(false);
			private int count;

			internal int Count => Volatile.Read(ref count);
			internal MetricsSnapshot LastSnapshot { get; private set; }

			public void Export(MetricsSnapshot snapshot)
			{
				LastSnapshot = snapshot;
				Interlocked.Increment(ref count);
				exported.Set();
			}

			internal bool WaitForCount(int expected, TimeSpan timeout)
			{
				DateTime deadline = DateTime.UtcNow + timeout;

				while (Count < expected)
				{
					TimeSpan remaining = deadline - DateTime.UtcNow;

					if (remaining <= TimeSpan.Zero || !exported.Wait(remaining))
					{
						return false;
					}
					exported.Reset();
				}
				return true;
			}
		}

		private sealed class ThrowingExporter : IMetricsExporter
		{
			private int count;

			internal int Count => Volatile.Read(ref count);

			public void Export(MetricsSnapshot snapshot)
			{
				Interlocked.Increment(ref count);
				throw new InvalidOperationException("expected");
			}
		}

		private sealed class OwnedExporter : IMetricsExporter, IDisposable
		{
			private readonly string name;
			private readonly List<string> calls;

			internal MetricsSnapshot Snapshot { get; private set; }
			internal bool Disposed { get; private set; }

			internal OwnedExporter(string name, List<string> calls)
			{
				this.name = name;
				this.calls = calls;
			}

			public void Export(MetricsSnapshot snapshot)
			{
				Snapshot = snapshot;
				calls.Add(name);
			}

			public void Dispose()
			{
				Disposed = true;
			}
		}

		private sealed class LegacyPayloadExporter : IMetricsExporter
		{
			internal MetricsSnapshot Snapshot { get; private set; }

			public void Export(MetricsSnapshot snapshot)
			{
				Snapshot = snapshot;
			}
		}

		private sealed class ReentrantExporter : IMetricsExporter
		{
			private readonly IAerospikeClient client;
			internal readonly ManualResetEventSlim Completed = new(false);
			internal Exception LifecycleException;
			internal MetricsSnapshot PullSnapshot;

			internal ReentrantExporter(IAerospikeClient client)
			{
				this.client = client;
			}

			public void Export(MetricsSnapshot snapshot)
			{
				PullSnapshot = client.GetMetricsSnapshot();

				try
				{
					client.DisableMetrics();
				}
				catch (Exception e)
				{
					LifecycleException = e;
				}
				Completed.Set();
			}
		}

		private sealed class BlockingExporter : IMetricsExporter
		{
			internal readonly ManualResetEventSlim Entered = new(false);
			internal readonly ManualResetEventSlim Release = new(false);

			public void Export(MetricsSnapshot snapshot)
			{
				Entered.Set();
				Release.Wait();
			}
		}

		private sealed class RecoveringExporter : IMetricsExporter
		{
			private int count;
			internal readonly ManualResetEventSlim Recovered = new(false);
			internal int Count => Volatile.Read(ref count);

			public void Export(MetricsSnapshot snapshot)
			{
				int current = Interlocked.Increment(ref count);

				if (current <= 3)
				{
					throw new InvalidOperationException("expected");
				}
				Recovered.Set();
			}
		}
	}
}
