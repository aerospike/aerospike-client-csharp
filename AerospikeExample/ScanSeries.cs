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

namespace Aerospike.Example;

public sealed class ScanSeries : SyncExample
{
	private readonly ConcurrentDictionary<string, Metrics> setMap = new();

	/// <summary>
	/// Scan each node in series and tally records across all sets.
	/// </summary>
	public override void RunExample()
	{
		console.Info($"Scan series: namespace={ns} set={set}");
		setMap.Clear();

		ScanPolicy scanPolicy = new()
		{
			recordsPerSecond = 5000
		};

		DateTime begin = DateTime.Now;

		foreach (Node node in client.Nodes)
		{
			console.Info($"Scan node {node.Name}");
			client.ScanNode(scanPolicy, node, ns, set, ScanCallback);

			foreach (KeyValuePair<string, Metrics> entry in setMap)
			{
				console.Info($"Node {node.Name} set {entry.Key} count: {entry.Value.count}");
				entry.Value.total += entry.Value.count;
				entry.Value.count = 0;
			}
		}

		double seconds = (DateTime.Now - begin).TotalSeconds;
		console.Info($"Elapsed time: {seconds} seconds");

		long total = 0;

		foreach (KeyValuePair<string, Metrics> entry in setMap)
		{
			console.Info($"Total set {entry.Key} count: {entry.Value.total}");
			total += entry.Value.total;
		}

		console.Info($"Grand total: {total}");
		console.Info($"Records/second: {Math.Round(total / seconds)}");
	}

	private void ScanCallback(Key key, Record record)
	{
		Metrics metrics = setMap.GetOrAdd(key.setName, _ => new Metrics());
		Interlocked.Increment(ref metrics.count);
	}

	private sealed class Metrics
	{
		public long count;
		public long total;
	}
}
