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
using System;
using System.Text;
using Aerospike.Client;
using BenchmarkDotNet.Attributes;

// iterations 25, 50, 10,000, 100,000 - want to trigger GC
[SimpleJob(launchCount: 1, warmupCount: 3, iterationCount: 25)]
public class BenchmarkAsync
{
	private readonly AsyncClient Client;

	[ParamsSource(nameof(pkRange))]
	public long pk;

	public IEnumerable<int> pkRange => Enumerable.Range(1, 25);

	private volatile int[] dataIntArray;

	private volatile Dictionary<object, object> dataDictionary;

	private volatile Dictionary<object, object>[] dataListDictionary;

	public Random random;

	public BenchmarkAsync()
	{
		var now = DateTime.Now;
		
		var policy = new AsyncClientPolicy
		{
			asyncMaxCommands = 1000,
			minConnsPerNode = 100,
			maxConnsPerNode = 100
		};
		Host[] hosts = new Host[] { new Host("localhost", 3000) };
		Client = new AsyncClient(policy, hosts);

		//random = new Random(42);

		/*try
		{
			Client.Truncate(null, "test", "test", now);
			Thread.Sleep(500);
		}
		catch 
		{
		}*/
	}

	[Benchmark]
	public void Put()
	{
		var key = new Key("test", "test", pk);
		var bins = new Bin[]
		{
			new Bin("binLong", new Value.LongValue(pk)),
			new Bin("binString", new Value.StringValue(pk.ToString())),
			new Bin("binDouble", new Value.DoubleValue((double)pk)),
			new Bin("binList", new Value.ListValue(dataIntArray)),
			new Bin("binMap", new Value.MapValue(dataDictionary)),
			new Bin("binListMap", new Value.ListValue(dataListDictionary))
		};

		Client.Put(null, key, bins);
	}

	[Benchmark]
	public void Get()
	{
		var key = new Key("test", "test", pk);
		Client.Get(null, key);
	}

	[GlobalSetup]
	public void CreateData()
	{
		//long length = random.Next(1, 100);
		long length = 10;
		dataIntArray = Enumerable.Range(0, (int)length).ToArray();
		dataDictionary = new Dictionary<object, object>(dataIntArray.Cast<object>().Select(v => new KeyValuePair<object, object>(v, v)));
		dataListDictionary = new Dictionary<object, object>[length];
		Array.Fill(dataListDictionary, dataDictionary);
	}
}
