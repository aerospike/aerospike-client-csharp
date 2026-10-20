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
using System.Runtime.Loader;

namespace Aerospike.Test
{
	[TestClass]
	public class TestMetricsExport : TestSync
	{
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
			Assert.IsNotNull(before.Usage);
			Assert.IsNotNull(before.Nodes);
			Assert.AreEqual(client.Nodes.Length, before.Nodes.Count);

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
			Assert.IsFalse(frozen.LegacyFile.Nodes.Any(
				current => current.Name == node.Name));
			Assert.AreEqual(
				1,
				frozen.LegacyFile.NodesDeparted.Count(
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
			Assert.IsNotNull(final.LegacyFile);
			Assert.IsNotNull(pulled.LegacyFile);
			Assert.AreEqual(
				final.LegacyFile.CpuPercent,
				pulled.LegacyFile.CpuPercent);
			Assert.AreEqual(
				final.LegacyFile.MemoryBytes,
				pulled.LegacyFile.MemoryBytes);
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
				active.LegacyFile.AppId);
			Assert.AreEqual(
				"active-region",
				active.LegacyFile.Labels["region"]);

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
				frozen.LegacyFile.AppId);
			Assert.AreEqual(
				"active-region",
				frozen.LegacyFile.Labels["region"]);
		}

		[TestMethod]
		public void MetricsEnableRollsBackReporterStartFailure()
		{
			FieldInfo starter = typeof(Cluster).GetField(
				"metricsReporterStarter",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(starter);
			object original = starter.GetValue(client.Cluster);
			starter.SetValue(client.Cluster, null);
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
				starter.SetValue(client.Cluster, original);
			}
		}

		[TestMethod]
		public void MetricsEnableRollsBackWriterConstructionFailure()
		{
			FieldInfo factory = typeof(Cluster).GetField(
				"metricsWriterFactory",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(factory);
			object original = factory.GetValue(client.Cluster);
			factory.SetValue(client.Cluster, null);

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
				factory.SetValue(client.Cluster, original);
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
			FieldInfo starter = typeof(Cluster).GetField(
				"metricsReporterStarter",
				BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.IsNotNull(starter);
			object original = starter.GetValue(scope.Client.Cluster);
			starter.SetValue(scope.Client.Cluster, null);

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
				starter.SetValue(scope.Client.Cluster, original);
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

				Assert.IsNotNull(capture.Snapshot?.LegacyFile);
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
			MetricsReporter reporter = new(
				new MetricsReporterConfig(
					TimeSpan.FromMilliseconds(100),
					[first],
					false),
				_ => client.GetMetricsSnapshot(),
				_ => first.Acknowledged++,
				new MetricsExporterFailureRegistry(),
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
					TimeSpan.FromMilliseconds(200),
					[first],
					false));
				waitVersion = scheduler.WaitFor(200, waitVersion);

				DeterministicExporter second = new();
				reporter.Reconfigure(new MetricsReporterConfig(
					TimeSpan.FromMilliseconds(200),
					[second],
					false));
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
					TimeSpan.FromDays(100),
					[second],
					false));
				scheduler.WaitFor(int.MaxValue, waitVersion);
				Assert.AreEqual(1, second.MaximumConcurrent);
			}
			finally
			{
				reporter.Stop();
			}
		}

		[TestMethod]
		public void MetricsReporterDeterministicallySuspendsAndBypassesFinal()
		{
			ManualMetricsScheduler scheduler = new();
			DeterministicExporter exporter = new()
			{
				FailuresRemaining = 3
			};
			int acknowledged = 0;
			MetricsReporter reporter = new(
				new MetricsReporterConfig(
					TimeSpan.FromMilliseconds(10),
					[exporter],
					false),
				_ => client.GetMetricsSnapshot(),
				_ => acknowledged++,
				new MetricsExporterFailureRegistry(),
				scheduler);

			reporter.Start();
			try
			{
				reporter.Resume();
				int waitVersion = scheduler.WaitFor(10, 0);
				for (int i = 1; i <= 3; i++)
				{
					scheduler.Advance(10);
					exporter.WaitForCount(i);
					waitVersion = scheduler.WaitFor(10, waitVersion);
				}

				Assert.IsTrue(
					reporter.ExportFinal(client.GetMetricsSnapshot()));
				Assert.AreEqual(4, exporter.Count);
				Assert.AreEqual(1, acknowledged);
				waitVersion = scheduler.WaitFor(10, waitVersion);

				for (int i = 0; i < 3; i++)
				{
					scheduler.Advance(10);
					waitVersion = scheduler.WaitFor(10, waitVersion);
					Assert.AreEqual(
						4,
						exporter.Count,
						"Suspended cycles must not call the exporter.");
				}

				scheduler.Advance(10);
				exporter.WaitForCount(5);
				Assert.AreEqual(2, acknowledged);
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

		[TestMethod]
		public void MetricsSnapshotDefaultDispatchSupportsReleased850Binary()
		{
			string configuration = new DirectoryInfo(
				AppContext.BaseDirectory).Parent.Name;
			string fixtureBin = Path.GetFullPath(Path.Combine(
				AppContext.BaseDirectory,
				"..",
				"..",
				"..",
				"Compatibility",
				"released-8.5.0",
				"Released850Implementer",
				"bin"));
			string fixturePath =
				new[] { configuration, "Debug_Unix", "Debug" }
					.Distinct()
					.Select(candidate => Path.Combine(
						fixtureBin,
						candidate,
						"net8.0",
						"Released850Implementer.dll"))
					.FirstOrDefault(File.Exists);
			Assert.IsTrue(
				fixturePath != null,
				"Released 8.5.0 compatibility fixture was not built.");

			ReleasedCompatibilityLoadContext context = new();
			Assembly fixture = context.LoadFromAssemblyPath(fixturePath);
			Type legacyType = fixture.GetType(
				"Aerospike.Compatibility.Released850.LegacyClient",
				true);
			Assert.IsFalse(legacyType.GetMethods(
				BindingFlags.Instance |
				BindingFlags.Public |
				BindingFlags.NonPublic)
				.Any(method =>
					method.Name.EndsWith(
						nameof(IAerospikeClient.GetMetricsSnapshot))));

			object instance = Activator.CreateInstance(legacyType);
			Assert.IsInstanceOfType<IAerospikeClient>(instance);
			IAerospikeClient legacy = (IAerospikeClient)instance;
			AssertThrows<NullReferenceException>(() =>
				legacy.GetMetricsSnapshot());

			InterfaceMapping map = legacyType.GetInterfaceMap(
				typeof(IAerospikeClient));
			int index = Array.FindIndex(
				map.InterfaceMethods,
				method => method.Name ==
					nameof(IAerospikeClient.GetMetricsSnapshot));
			Assert.IsTrue(index >= 0);
			Assert.AreEqual(
				typeof(IAerospikeClient),
				map.TargetMethods[index].DeclaringType);
		}

		[TestMethod]
		public void MetricsDepartureAcknowledgePreservesCallbackWrites()
		{
			MetricsPolicy policy = new();
			MetricsRuntimeConfig config = new(policy);
			MetricsNodeCollector collector = new(config);
			collector.AddOpened();
			MetricsDepartureStore store = new();
			store.Add(
				"N1",
				"127.0.0.1",
				3000,
				0,
				new MetricsNodeDeparture(null, collector),
				CreateLegacyNode("N1", policy));

			MetricsDepartureCapture first = store.Capture(config);
			Assert.AreEqual(1, first.Nodes.Count);
			Assert.AreEqual(1L, first.Nodes[0].View.Opened);

			collector.AddClosed(ConnectionCloseReason.NodeRemoved, true);
			store.Acknowledge(first.Token);

			MetricsDepartureCapture second = store.Capture(config);
			Assert.AreEqual(1, second.Nodes.Count);
			Assert.AreEqual(0L, second.Nodes[0].View.Opened);
			Assert.AreEqual(1L, second.Nodes[0].View.Closed);
			Assert.IsNotNull(
				second.Nodes[0].Legacy,
				"Late canonical values must retain their cumulative legacy payload.");

			store.Acknowledge(second.Token);
			Assert.AreEqual(0, store.Capture(config).Nodes.Count);

			collector.AddOpened();
			MetricsDepartureCapture third = store.Capture(config);
			Assert.AreEqual(1L, third.Nodes.Single().View.Opened);
			store.Acknowledge(third.Token);
			Assert.AreEqual(0, store.Capture(config).Nodes.Count);

			collector.AddClosed(ConnectionCloseReason.Error, true);
			MetricsDepartureCapture fourth = store.Capture(config);
			Assert.AreEqual(0L, fourth.Nodes.Single().View.Opened);
			Assert.AreEqual(1L, fourth.Nodes.Single().View.Closed);
			Assert.AreEqual(1L, fourth.Nodes.Single().View.ClosedError);
		}

		[TestMethod]
		public void MetricsDepartureStoreCapsNamesAndSignalsTruncation()
		{
			MetricsPolicy policy = new();
			MetricsRuntimeConfig config = new(policy);
			MetricsDepartureStore store = new();

			for (int i = 0; i < 257; i++)
			{
				MetricsNodeCollector collector = new(config);
				store.Add(
					"N" + i,
					"127.0.0.1",
					3000,
					0,
					new MetricsNodeDeparture(null, collector),
					CreateLegacyNode("N" + i, policy));
			}

			MetricsDepartureCapture capture = store.Capture(config);
			Assert.AreEqual(256, capture.Nodes.Count);
			Assert.IsTrue(capture.PossiblyTruncated);

			store.Acknowledge(capture.Token);
			MetricsDepartureCapture after = store.Capture(config);
			Assert.IsFalse(after.PossiblyTruncated);
		}

		[TestMethod]
		public void MetricsDepartureStoreAggregatesRepeatedNameContributions()
		{
			MetricsPolicy policy = new();
			MetricsRuntimeConfig config = new(policy);
			MetricsDepartureStore store = new();

			for (int i = 0; i < 257; i++)
			{
				MetricsNodeCollector collector = new(config);
				collector.AddOpened();
				store.Add(
					"N1",
					"127.0.0.1",
					3000,
					0,
					new MetricsNodeDeparture(null, collector),
					CreateLegacyNode("N1", policy));
			}

			MetricsDepartureCapture capture = store.Capture(config);
			Assert.AreEqual(1, capture.Nodes.Count);
			Assert.AreEqual(257L, capture.Nodes.Single().View.Opened);
			Assert.IsFalse(capture.PossiblyTruncated);

			store.Acknowledge(capture.Token);
			MetricsDepartureCapture after = store.Capture(config);
			Assert.AreEqual(0, after.Nodes.Count);
			Assert.IsFalse(after.PossiblyTruncated);
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
			LegacyMetricsFileNamespaceSnapshot legacyNs = new(
				"test",
				1,
				0,
				0,
				0,
				0,
				Enumerable.Range(0, Latency.GetMax())
					.Select(_ => (IReadOnlyList<long>)
						Array.AsReadOnly(new[] { 1L, 2L, 3L })));
			MetricsDepartureStore store = new();
			store.Add(
				"N1",
				"127.0.0.1",
				3000,
				0,
				new MetricsNodeDeparture(null, collector),
				new LegacyMetricsFileNodeSnapshot(
					"N1",
					"127.0.0.1",
					3000,
					new ConnectionStats(0, 0, 0, 0),
					new ConnectionStats(0, 0, 0, 0),
					[legacyNs],
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
				store.Capture(new MetricsRuntimeConfig(replacementPolicy));
			MetricsDepartedNodeCapture departed = capture.Nodes.Single();
			MetricsNamespaceView canonical =
				departed.View.Namespaces["test"];

			Assert.AreEqual(1L, canonical.Errors);
			Assert.IsTrue(canonical.Latency.Values.All(
				buckets => buckets.Count == 5 &&
					buckets.All(value => value == 0)));
			Assert.AreEqual(
				MetricsLatencyUnit.Microseconds,
				departed.Legacy.LatencyUnit);
			Assert.AreEqual(5, departed.Legacy.LatencyColumns);
			Assert.AreEqual(2, departed.Legacy.LatencyShift);
			Assert.IsTrue(departed.Legacy.Namespaces.Single().Latency.All(
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
			MetricsDepartureStore store = new();

			store.Add(
				"N1",
				"127.0.0.1",
				3000,
				0,
				new MetricsNodeDeparture(null, oldCollector),
				CreateLegacyNodeWithNamespace("N1", oldPolicy, 2, 1));
			store.Add(
				"N1",
				"127.0.0.2",
				3000,
				0,
				new MetricsNodeDeparture(null, currentCollector),
				CreateLegacyNodeWithNamespace("N1", currentPolicy, 3, 2));

			LegacyMetricsFileNodeSnapshot legacy =
				store.Capture(currentConfig).Nodes.Single().Legacy;
			LegacyMetricsFileNamespaceSnapshot ns =
				legacy.Namespaces.Single();

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
				LegacyMetricsFileNamespaceSnapshot ns = new(
					"test",
					9,
					10,
					11,
					12,
					13,
					latency);
				LegacyMetricsFileNodeSnapshot active = new(
					"N1",
					"127.0.0.1",
					3000,
					new ConnectionStats(1, 2, 3, 4),
					new ConnectionStats(5, 6, 7, 8),
					[ns],
					MetricsLatencyUnit.Milliseconds,
					3,
					1);
				LegacyMetricsFileNodeSnapshot departed = new(
					"N2",
					"127.0.0.2",
					3001,
					new ConnectionStats(16, 17, 18, 19),
					new ConnectionStats(20, 21, 22, 23),
					Array.Empty<LegacyMetricsFileNamespaceSnapshot>(),
					MetricsLatencyUnit.Milliseconds,
					3,
					1);
				LegacyMetricsFileSnapshot legacy = new(
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
					Array.Empty<NodeMetricsSnapshot>(),
					Array.Empty<NodeMetricsSnapshot>(),
					false,
					legacy);

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

		private static LegacyMetricsFileNodeSnapshot CreateLegacyNode(
			string name,
			MetricsPolicy policy)
		{
			return new LegacyMetricsFileNodeSnapshot(
				name,
				"127.0.0.1",
				3000,
				new ConnectionStats(0, 0, 0, 0),
				new ConnectionStats(0, 0, 0, 0),
				Array.Empty<LegacyMetricsFileNamespaceSnapshot>(),
				policy.LatencyUnit,
				policy.LatencyColumns,
				policy.LatencyShift);
		}

		private static LegacyMetricsFileNodeSnapshot CreateLegacyNodeWithNamespace(
			string name,
			MetricsPolicy policy,
			long errors,
			long latencyValue)
		{
			LegacyMetricsFileNamespaceSnapshot ns = new(
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
			return new LegacyMetricsFileNodeSnapshot(
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

		private sealed class ReleasedCompatibilityLoadContext
			: AssemblyLoadContext
		{
			protected override Assembly Load(
				AssemblyName assemblyName)
			{
				if (assemblyName.Name ==
					typeof(IAerospikeClient).Assembly.GetName().Name)
				{
					return typeof(IAerospikeClient).Assembly;
				}
				return null;
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
