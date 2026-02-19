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
using Aerospike.Client.OpenTelemetry;
using OpenTelemetry;
using OpenTelemetry.Metrics;

/// <summary>
/// Example demonstrating how to use Aerospike metrics exporters.
/// This example shows both the built-in file exporter (MetricsWriter) and
/// the OpenTelemetry exporter running simultaneously.
/// </summary>
class MetricsExporterExample
{
	static void Main(string[] args)
	{
		Console.WriteLine("=== Aerospike Metrics Exporter Example ===\n");

		// Configuration - adjust these for your environment
		var host = args.Length > 0 ? args[0] : "localhost";
		var port = args.Length > 1 ? int.Parse(args[1]) : 3000;
		var metricsDir = Path.GetFullPath("MetricsOutput");

		Console.WriteLine($"Metrics output directory: {metricsDir}");
		if (Directory.Exists(metricsDir)) Directory.Delete(metricsDir, true);
		Directory.CreateDirectory(metricsDir);

		var clientPolicy = new ClientPolicy();

		try
		{
			// Step 1: Setup OpenTelemetry MeterProvider
			// The MeterProvider must be configured to listen for the Aerospike meter
			Console.WriteLine("1. Setting up OpenTelemetry MeterProvider...");
			using var meterProvider = Sdk.CreateMeterProviderBuilder()
				.AddMeter(OpenTelemetryMetricsExporter.DefaultMeterName)
				.AddConsoleExporter((options, readerOptions) =>
				{
					readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 5000;
				})
				.Build();

			// Step 2: Create exporters
			// MetricsWriter: Built-in file exporter (included in AerospikeClient)
			// OpenTelemetryMetricsExporter: OTel exporter (from AerospikeClient.OpenTelemetry package)
			Console.WriteLine("2. Creating exporters...");
			var fileExporter = new MetricsWriter(metricsDir, latencyColumns: 7, latencyShift: 1);
			var otelExporter = new OpenTelemetryMetricsExporter();

			// Step 3: Connect to Aerospike
			Console.WriteLine($"3. Connecting to Aerospike at {host}:{port}...");
			using var client = new AerospikeClient(clientPolicy, host, port);
			Console.WriteLine($"   Connected! Nodes: {client.Nodes.Length}");

			// Step 4: Configure metrics policy and add exporters
			// Multiple exporters can be added to export to different destinations
			var metricsPolicy = new MetricsPolicy { Interval = 3 };
			metricsPolicy.AddExporter(fileExporter);
			metricsPolicy.AddExporter(otelExporter);

			// Step 5: Enable metrics
			Console.WriteLine("4. Enabling metrics with 3-second export interval...");
			client.EnableMetrics(metricsPolicy);

			// Step 6: Perform operations
			Console.WriteLine("5. Performing operations for 10 seconds...");
			var key = new Key("test", "exporter-example", "key1");
			var startTime = DateTime.Now;
			int iterations = 0;

			while ((DateTime.Now - startTime).TotalSeconds < 10)
			{
				client.Put(null, key, new Bin("value", iterations));
				client.Get(null, key);
				iterations++;
				Thread.Sleep(100);
			}
			Console.WriteLine($"   Completed {iterations} Put+Get operations");

			// Step 7: Wait for final export cycle
			Console.WriteLine("\n6. Waiting for final export cycle (5 seconds)...");
			Thread.Sleep(5000);

			// Step 8: Disable metrics and cleanup
			// Important: Disable metrics BEFORE disposing exporters
			Console.WriteLine("\n7. Disabling metrics...");
			client.DisableMetrics();

			Console.WriteLine("8. Disposing exporters...");
			otelExporter.Dispose();
			fileExporter.Dispose();

			// Display results
			PrintFileExporterResults(metricsDir);

			Console.WriteLine("\n========================================");
			Console.WriteLine("           EXAMPLE COMPLETE             ");
			Console.WriteLine("========================================");
		}
		catch (AerospikeException ex)
		{
			Console.WriteLine($"\nAerospike error: {ex.Message}");
			Console.WriteLine($"Make sure Aerospike server is running on {host}:{port}");
		}

		Console.WriteLine($"\nMetrics files are in: {metricsDir}");
	}

	static void PrintFileExporterResults(string metricsDir)
	{
		Console.WriteLine("\n========================================");
		Console.WriteLine("           FILE EXPORTER RESULTS        ");
		Console.WriteLine("========================================");

		var logFiles = Directory.GetFiles(metricsDir, "metrics-*.log");
		if (logFiles.Length > 0)
		{
			var lines = File.ReadAllLines(logFiles[0]);
			Console.WriteLine($"File: {Path.GetFileName(logFiles[0])}");
			Console.WriteLine($"Total lines: {lines.Length}");

			var exports = lines.Where(l => l.Contains("metrics[")).ToList();
			Console.WriteLine($"Export batches: {exports.Count}");

			Console.WriteLine("\nExport timestamps:");
			foreach (var exp in exports)
			{
				Console.WriteLine($"  {exp}");
			}

			Console.WriteLine("\nSample metrics:");
			var commands = lines.FirstOrDefault(l => l.Contains("commands_total"));
			if (commands != null) Console.WriteLine($"  {commands.Trim()}");

			var connections = lines.FirstOrDefault(l => l.Contains("connections.in_pool"));
			if (connections != null) Console.WriteLine($"  {connections.Trim()}");

			var latency = lines.FirstOrDefault(l => l.Contains("latency.bucket") && l.Contains("write") && l.Contains("le=\"1ms\""));
			if (latency != null) Console.WriteLine($"  {latency.Trim()}");

			var bytesIn = lines.FirstOrDefault(l => l.Contains("bytes_in_total"));
			if (bytesIn != null) Console.WriteLine($"  {bytesIn.Trim()}");
		}
		else
		{
			Console.WriteLine("No log files found!");
		}
	}
}
