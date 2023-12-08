/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Concurrent;

namespace Aerospike.Test
{
	[TestClass]
	public class TestScan : TestSync
	{
		private readonly ConcurrentDictionary<string, Metrics> setMap = new ConcurrentDictionary<string, Metrics>();

		[TestMethod]
		public void ScanParallel()
		{
			ScanPolicy policy = new ScanPolicy();
			if (args.testProxy)
			{
				policy.totalTimeout = args.proxyTotalTimeout;
			}

			if (!args.testProxy)
			{
				client.ScanAll(policy, args.ns, args.set, ScanCallback);
			}
			else
			{
				var recordSet = proxyClient.ScanAll(policy, args.ns, args.set);
				Metrics metrics;
				while (recordSet.Next())
				{
					var key = recordSet.Key;

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
			}
		}

		[TestMethod]
		public void ScanSeries()
		{
			if (!args.testProxy || (args.testProxy && nativeClient != null))
			{
				Node[] nodes = nativeClient.Nodes;

				foreach (Node node in nodes)
				{

					nativeClient.ScanNode(null, node, args.ns, args.set, ScanCallback);

					foreach (KeyValuePair<string, Metrics> entry in setMap)
					{
						entry.Value.count = 0;
					}
				}
			}
		}

		public void ScanCallback(Key key, Record record)
		{
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

		public class Metrics
		{
			public long count = 0;
		}
	}
}
