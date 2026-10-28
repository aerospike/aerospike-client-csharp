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
	internal sealed partial class MetricsManager
	{
		internal void ExportCycle(Cluster cluster)
		{
			MetricsSnapshot snapshot = BuildSnapshot(
				cluster,
				null,
				true);
			ExportSnapshot(snapshot, false);
		}

		internal bool ExportFinal(MetricsSnapshot snapshot)
		{
			return ExportSnapshot(snapshot, true);
		}

		internal void ExportFinalAndStop(
			Cluster cluster,
			MetricsSnapshot snapshot)
		{
			MetricsReporter reporter = Reporter;

			if (reporter != null)
			{
				RunOutsideMetricsLock(cluster, () =>
				{
					reporter.ExportFinal(snapshot);
					reporter.Stop();
				});
			}

			Reporter = null;
			MetricsWriter writer = FileWriter;
			FileWriter = null;

			if (writer != null)
			{
				RunOutsideMetricsLock(cluster, writer.Dispose);
			}
			ConfigureExporters(Array.Empty<IMetricsExporter>());
		}

		private bool ExportSnapshot(MetricsSnapshot snapshot, bool final)
		{
			IReadOnlyList<IMetricsExporter> current;
			lock (exportLock)
			{
				current = exporters;
			}

			bool allSucceeded = true;

			foreach (IMetricsExporter exporter in current)
			{
				MetricsExporterFailureState state = ExporterFailures.Get(exporter);

				if (!final && state.SuspendRemaining > 0)
				{
					state.SuspendRemaining--;
					allSucceeded = false;
					continue;
				}

				try
				{
					exporter.Export(snapshot);

					if (!final)
					{
						state.ConsecutiveFailures = 0;
						state.SuspendRemaining = 0;
					}
				}
				catch (Exception e)
				{
					allSucceeded = false;
					Log.Warn("Metrics exporter failed: " +
						Util.GetErrorMessage(e));

					if (!final)
					{
						state.ConsecutiveFailures++;

						if (state.ConsecutiveFailures >= FailureThreshold)
						{
							state.ConsecutiveFailures = 0;
							state.SuspendRemaining = SuspendedCycles;
						}
					}
				}
			}

			if (allSucceeded)
			{
				snapshot.DepartureToken?.Store.AcknowledgeDepartures(
					snapshot.DepartureToken);
			}
			return allSucceeded;
		}
	}
}
