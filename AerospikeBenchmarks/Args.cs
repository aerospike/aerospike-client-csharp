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
using Microsoft.Extensions.Configuration;
using System.Text;

namespace Aerospike.Benchmarks
{
	sealed class Args
	{
		internal Host[] hosts;
		internal int port;
		internal AuthMode authMode;
		internal string user;
		internal string password;
		internal string clusterName;
		internal string ns;
		internal string set;
		internal string binName;
		internal string tlsName;
		internal TlsPolicy tlsPolicy;
		internal WritePolicy writePolicy;
		internal Policy policy;
		internal BatchPolicy batchPolicy;
		internal BinType binType;
		internal Value fixedValue;
		internal int commandMax;
		internal int threadMax;
		internal int transactionMax;
		internal int records;
		internal int recordsInit;
		internal int readPct;
		internal int binSize;
		internal int latencyColumns;
		internal int latencyShift;
		internal int batchSize;
		internal int throughput;
		internal bool initialize;
		internal bool sync;
		internal bool latency;
		internal bool latencyAltFormat;
		internal bool debug;
		internal bool singleBin;

		public Args()
		{
			var builder = new ConfigurationBuilder()
				.AddJsonFile("settings.json", optional: true, reloadOnChange: true);
			IConfigurationRoot section = builder.Build();

			IConfigurationSection cs = section.GetSection("Port");
			port = int.Parse(cs.Value);


			port = int.Parse(section.GetSection("Port").Value);
			authMode = (AuthMode)Enum.Parse(typeof(AuthMode), section.GetSection("AuthMode").Value, true);
			user = section.GetSection("User").Value;
			password = section.GetSection("Password").Value;
			clusterName = section.GetSection("ClusterName").Value;
			ns = section.GetSection("Namespace").Value;
			set = section.GetSection("Set").Value;
			initialize = bool.Parse(section.GetSection("Initialize").Value);

			bool tlsEnable = bool.Parse(section.GetSection("TlsEnable").Value);

			if (tlsEnable)
			{
				tlsName = section.GetSection("TlsName").Value;
				tlsPolicy = new TlsPolicy(
					section.GetSection("TlsProtocols").Value,
					section.GetSection("TlsRevoke").Value,
					section.GetSection("TlsClientCertFile").Value,
					bool.Parse(section.GetSection("TlsLoginOnly").Value)
					);
			}

			hosts = Host.ParseHosts(section.GetSection("Host").Value, tlsName, port);
			commandMax = int.Parse(section.GetSection("AsyncMaxCommands").Value);
			sync = bool.Parse(section.GetSection("Sync").Value);

			if (sync)
			{
				threadMax = int.Parse(section.GetSection("SyncThreads").Value);
			}

			transactionMax = int.Parse(section.GetSection("TransactionMax").Value);

			records = int.Parse(section.GetSection("Records").Value);
			int recordsInitPct = int.Parse(section.GetSection("InitPct").Value);
			recordsInit = records / 100 * recordsInitPct;
			readPct = int.Parse(section.GetSection("ReadPct").Value);

			if (!(readPct >= 0 && readPct <= 100))
			{
				throw new Exception("ReadPct must be in range 0 - 100");
			}

			binType = (BinType)Enum.Parse(typeof(BinType), section.GetSection("BinType").Value, true);
			binSize = int.Parse(section.GetSection("BinSize").Value);
			batchSize = int.Parse(section.GetSection("BatchSize").Value);

			bool fixValue = bool.Parse(section.GetSection("FixedValue").Value);

			if (fixValue)
			{
				SetFixedValue();
			}

			latency = bool.Parse(section.GetSection("Latency").Value);

			if (latency)
			{
				latencyColumns = int.Parse(section.GetSection("LatencyColumns").Value);
				latencyShift = int.Parse(section.GetSection("LatencyShift").Value);
				latencyAltFormat = bool.Parse(section.GetSection("LatencyAltFormat").Value);

				if (!(latencyColumns >= 2 && latencyColumns <= 10))
				{
					throw new Exception("Latency columns must be between 2 and 10 inclusive.");
				}

				if (!(latencyShift >= 1 && latencyShift <= 5))
				{
					throw new Exception("Latency exponent shift must be between 1 and 5 inclusive.");
				}
			}

			throughput = int.Parse(section.GetSection("ThroughPut").Value);
			debug = bool.Parse(section.GetSection("Debug").Value);

			int timeout = int.Parse(section.GetSection("Timeout").Value);
			int timeoutDelay = int.Parse(section.GetSection("TimeoutDelay").Value);
			int maxRetries = int.Parse(section.GetSection("MaxRetries").Value);
			int sleepBetweenRetries = int.Parse(section.GetSection("SleepBetweenRetries").Value);
			Replica replica = (Replica)Enum.Parse(typeof(Replica), section.GetSection("Replica").Value, true);

			ReadModeAP readModeAP = (ReadModeAP)Enum.Parse(typeof(ReadModeAP), section.GetSection("ReadModeAP").Value, true);
			ReadModeSC readModeSC = (ReadModeSC)Enum.Parse(typeof(ReadModeSC), section.GetSection("ReadModeSC").Value, true);

			policy = new Policy();
			policy.totalTimeout = timeout;
			policy.maxRetries = maxRetries;
			policy.sleepBetweenRetries = sleepBetweenRetries;
			policy.replica = replica;
			policy.readModeAP = readModeAP;
			policy.readModeSC = readModeSC;
			policy.TimeoutDelay = timeoutDelay;

			writePolicy = new WritePolicy();
			writePolicy.totalTimeout = timeout;
			writePolicy.maxRetries = maxRetries;
			writePolicy.sleepBetweenRetries = sleepBetweenRetries;
			writePolicy.replica = replica;
			writePolicy.TimeoutDelay = timeoutDelay;

			batchPolicy = new BatchPolicy();
			batchPolicy.totalTimeout = timeout;
			batchPolicy.maxRetries = maxRetries;
			batchPolicy.sleepBetweenRetries = sleepBetweenRetries;
			batchPolicy.replica = replica;
			batchPolicy.readModeAP = readModeAP;
			batchPolicy.readModeSC = readModeSC;
			batchPolicy.TimeoutDelay = timeoutDelay;
		}

		/// <summary>
		/// Some database calls need to know how the server is configured.
		/// </summary>
		internal void SetServerSpecific(AerospikeClient client)
		{
			Node node = client.Nodes[0];
			string featuresFilter = "features";
			string namespaceFilter = "namespace/" + ns;
			Dictionary<string, string> tokens = Info.Request(null, node, featuresFilter, namespaceFilter);

			string namespaceTokens = tokens[namespaceFilter];

			if (namespaceTokens == null)
			{
				throw new Exception(string.Format("Failed to get namespace info: host={0} namespace={1}", node, ns));
			}

			singleBin = parseBoolean(namespaceTokens, "single-bin");
			binName = singleBin ? "" : "bin";  // Single bin servers don't need a bin name.
		}

		private static bool parseBoolean(String namespaceTokens, String name)
		{
			string search = name + '=';
			int begin = namespaceTokens.IndexOf(search);

			if (begin < 0)
			{
				return false;
			}

			begin += search.Length;
			int end = namespaceTokens.IndexOf(';', begin);

			if (end < 0)
			{
				end = namespaceTokens.Length;
			}

			string value = namespaceTokens.Substring(begin, end - begin);
			return Convert.ToBoolean(value);
		}

		public string GetBinName(string name)
		{
			// Single bin servers don't need a bin name.
			return singleBin ? "" : name;
		}

		public void SetFixedValue()
		{
			// Fixed values are used when the extra random call overhead is not wanted
			// in the benchmark measurement.
			RandomShift random = new RandomShift();
			fixedValue = GetValue(random);
		}

		public Value GetValue(RandomShift random)
		{
			if (fixedValue != null)
			{
				return fixedValue;
			}

			// Generate random value.
			switch (binType)
			{
				case BinType.Integer:
					return Value.Get(random.Next());

				case BinType.String:
					StringBuilder sb = new StringBuilder(binSize);

					for (int i = 0; i < binSize; i++)
					{
						sb.Append((char)random.Next(33, 127));
					}
					return Value.Get(sb.ToString());

				case BinType.Byte:
					byte[] bytes = new byte[binSize];
					random.NextBytes(bytes);
					return Value.Get(bytes);

				default:
					return null;
			}
		}

		public void Print()
		{
			if (initialize)
			{
				Console.WriteLine("Initialize " + recordsInit + " records");
			}
			else
			{
				Console.WriteLine("Read/write using " + records + " records");
			}

			Console.WriteLine("hosts: " + Util.ArrayToString(hosts) + ", namespace: " + ns +
				", set: " + set);

			string throughputStr = (throughput == 0) ? "unlimited" : throughput.ToString() + " tps";
			string transactionStr = (transactionMax == 0) ? "unlimited" : transactionMax.ToString();

			Console.WriteLine("threads: " + threadMax + ", transactions: " + transactionStr +
				", throughput: " + throughputStr + ", debug: " + debug);

			Console.Write("write policy:");
			Console.WriteLine(
				" socketTimeout: " + writePolicy.socketTimeout
				+ ", totalTimeout: " + writePolicy.totalTimeout
				+ ", maxRetries: " + writePolicy.maxRetries
				+ ", sleepBetweenRetries: " + writePolicy.sleepBetweenRetries
				);


			Console.Write("bin type: " + binType.ToString());

			switch (binType)
			{
				case BinType.Integer:
					break;

				default:
					Console.Write("[" + binSize + "]");
					break;
			}

			string randStr = fixedValue != null ? "false" : "true";
			Console.WriteLine(", random values: " + randStr);

			if (!sync)
			{
				Console.WriteLine("Async max concurrent commands: " + commandMax);
			}
			Console.WriteLine();
		}
	}

	public enum BinType
	{
		Integer,
		String,
		Byte
	}
}
