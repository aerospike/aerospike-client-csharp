/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class ScanSeries : SyncExample
	{
		private Dictionary<string, Metrics> setMap = new Dictionary<string, Metrics>();

		public ScanSeries(Console console) : base(console)
		{
		}

		/// <summary>
		/// Scan all nodes in series and read all records in all sets.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			console.Info("Scan series: namespace=" + args.ns + " set=" + args.set);
			setMap.Clear();

			// Use low scan priority.  This will take more time, but it will reduce
			// the load on the server.
			ScanPolicy policy = new ScanPolicy();
			policy.maxRetries = 1;
			policy.priority = Priority.LOW;

			Node[] nodes = client.Nodes;
			DateTime begin = DateTime.Now;

			foreach (Node node in nodes)
			{
				console.Info("Scan node " + node.Name);
				client.ScanNode(policy, node, args.ns, args.set, ScanCallback);

				foreach (KeyValuePair<string, Metrics> entry in setMap)
				{
					console.Info("Node " + node.Name + " set " + entry.Key + " count: " + entry.Value.count);
					entry.Value.count = 0;
				}
			}

			DateTime end = DateTime.Now;
			double seconds = end.Subtract(begin).TotalSeconds;
			console.Info("Elapsed time: " + seconds + " seconds");

			long total = 0;

			foreach (KeyValuePair<string, Metrics> entry in setMap)
			{
				console.Info("Total set " + entry.Key + " count: " + entry.Value.total);
				total += entry.Value.total;
			}
			console.Info("Grand total: " + total);
			double performance = Math.Round((double)total / seconds);
			console.Info("Records/second: " + performance);
		}

		public void ScanCallback(Key key, Record record)
		{
			Metrics metrics;

			if (! setMap.TryGetValue(key.setName, out metrics))
			{
				metrics = new Metrics();
			}
			metrics.count++;
			metrics.total++;
			setMap[key.setName] = metrics;
		}

		private class Metrics
		{
			public long count = 0;
			public long total = 0;
		}
	}
}
