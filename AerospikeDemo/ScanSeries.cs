/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using System.Collections.Concurrent;
using Aerospike.Client;
using System.Threading;

namespace Aerospike.Demo
{
	public class ScanSeries : SyncExample
	{
		private ConcurrentDictionary<string, Metrics> setMap = new ConcurrentDictionary<string, Metrics>();

		public ScanSeries(Console console) : base(console)
		{
		}

		/// <summary>
		/// Scan all nodes in series and read all records in all sets.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			console.Info("Scan series: namespace=" + args.ns + " set=" + args.set);
			setMap.Clear();

			ScanPolicy policy = new ScanPolicy();
			policy.recordsPerSecond = 5000;

			// Low scan priority will take more time, but it will reduce the load on the server.
			// policy.priority = Priority.LOW;

			Node[] nodes = client.Nodes;
			DateTime begin = DateTime.Now;

			foreach (Node node in nodes)
			{
				console.Info("Scan node " + node.Name);
				client.ScanNode(policy, node, args.ns, args.set, ScanCallback);

				foreach (KeyValuePair<string, Metrics> entry in setMap)
				{
					console.Info("Node " + node.Name + " set " + entry.Key + " count: " + entry.Value.count);
					entry.Value.total += entry.Value.count;
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
			// It's not strictly necessary to make this callback thread-safe when ScanNode()
			// is used in series because only one node thread is processing results at any
			// point in time.  The reason it's thread-safe here is because of a previous
			// .NET Core bug which incorrectly threw an exception error "Operations that 
			// change non-concurrent collections must have exclusive access".
			Metrics metrics;

			if (setMap.TryGetValue(key.setName, out metrics))
			{
				Interlocked.Increment(ref metrics.count);
				return;
			}

			// Set not found.  Must lock to create metrics entry.
			lock (setMap)
			{
				// Retry lookup under lock.
				if (setMap.TryGetValue(key.setName, out metrics))
				{
					Interlocked.Increment(ref metrics.count);
					return;
				}

				metrics = new Metrics();
				metrics.count = 1;
				setMap[key.setName] = metrics;
			}
		}

		private class Metrics
		{
			public long count = 0;
			public long total = 0;
		}
	}
}
