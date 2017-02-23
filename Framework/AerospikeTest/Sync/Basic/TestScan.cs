/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestScan : TestSync
	{
		private readonly IDictionary<string, Metrics> setMap = new Dictionary<string, Metrics>();

		[TestMethod]
		public void ScanParallel()
		{
			ScanPolicy policy = new ScanPolicy();
			client.ScanAll(policy, args.ns, args.set, ScanCallback);
		}

		[TestMethod]
		public void ScanSeries()
		{
			// Use low scan priority.  This will take more time, but it will reduce
			// the load on the server.
			ScanPolicy policy = new ScanPolicy();
			policy.maxRetries = 1;
			policy.priority = Priority.LOW;

			Node[] nodes = client.Nodes;

			foreach (Node node in nodes)
			{
				client.ScanNode(policy, node, args.ns, args.set, ScanCallback);

				foreach (KeyValuePair<string, Metrics> entry in setMap)
				{
					entry.Value.count = 0;
				}
			}
		}

		public void ScanCallback(Key key, Record record)
		{
			Metrics metrics;

			if (!setMap.TryGetValue(key.setName, out metrics))
			{
				metrics = new Metrics();
			}
			metrics.count++;
			metrics.total++;
			setMap[key.setName] = metrics;
		}

		public class Metrics
		{
			public long count = 0;
			public long total = 0;
		}		
	}
}
