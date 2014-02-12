/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
