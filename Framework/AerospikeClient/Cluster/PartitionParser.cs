/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Parse node's master (and optionally prole) partitions.
	/// </summary>
	public sealed class PartitionParser
	{
		internal const string PartitionGeneration = "partition-generation";
		internal const string ReplicasAll = "replicas-all";
		internal const string Replicas = "replicas";

		private Dictionary<string, Partitions> map;
		private readonly byte[] buffer;
		private readonly int partitionCount;
		private readonly int generation;
		private int length;
		private int offset;
		private bool copied;
		private bool regimeError;

		public PartitionParser(Connection conn, Node node, Dictionary<string, Partitions> map, int partitionCount)
		{
			// Send format 1:  partition-generation\nreplicas\n
			// Send format 2:  partition-generation\nreplicas-all\n
			// Send format 3:  partition-generation\nreplicas-master\n
			this.partitionCount = partitionCount;
			this.map = map;

			string command;
			if (node.HasReplicas)
			{
				command = Replicas;
			}
			else
			{
				command = ReplicasAll;
			}

			Info info = new Info(conn, PartitionGeneration, command);
			this.length = info.length;

			if (length == 0)
			{
				throw new AerospikeException.Parse("Partition info is empty");
			}
			this.buffer = info.buffer;

			generation = ParseGeneration();

			ParseReplicasAll(node, command);
		}

		public int Generation
		{
			get {return generation;}
		}

		public bool IsPartitionMapCopied
		{
			get {return copied;}
		}

		public Dictionary<string, Partitions> PartitionMap
		{
			get {return map;}
		}

		public int ParseGeneration()
		{
			ExpectName(PartitionGeneration);

			int begin = offset;

			while (offset < length)
			{
				if (buffer[offset] == '\n')
				{
					string s = ByteUtil.Utf8ToString(buffer, begin, offset - begin).Trim();
					offset++;
					return Convert.ToInt32(s);
				}
				offset++;
			}
			throw new AerospikeException.Parse("Failed to find partition-generation value");
		}

		private void ParseReplicasAll(Node node, string command)
		{
			// Use low-level info methods and parse byte array directly for maximum performance.
			// Receive format: replicas-all\t
			//                 <ns1>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
			//                 <ns2>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
			ExpectName(command);

			int begin = offset;
			int regime = 0;

			while (offset < length)
			{
				if (buffer[offset] == ':')
				{
					// Parse namespace.
					string ns = ByteUtil.Utf8ToString(buffer, begin, offset - begin).Trim();

					if (ns.Length <= 0 || ns.Length >= 32)
					{
						string response = GetTruncatedResponse();
						throw new AerospikeException.Parse("Invalid partition namespace " + ns + ". Response=" + response);
					}
					begin = ++offset;

					// Parse regime.
					if (command == Replicas)
					{
						while (offset < length)
						{
							byte b = buffer[offset];

							if (b == ',')
							{
								break;
							}
							offset++;
						}
						regime = Convert.ToInt32(Encoding.UTF8.GetString(buffer, begin, offset - begin));
						begin = ++offset;
					}

					// Parse replica count.
					while (offset < length)
					{
						byte b = buffer[offset];

						if (b == ',')
						{
							break;
						}
						offset++;
					}
					int replicaCount = Convert.ToInt32(Encoding.UTF8.GetString(buffer, begin, offset - begin));

					// Ensure replicaCount is uniform.
					Partitions partitions;

					if (!map.TryGetValue(ns, out partitions))
					{
						// Create new replica array. 
						partitions = new Partitions(partitionCount, replicaCount, regime != 0);
						CopyPartitionMap();
						map[ns] = partitions;
					}
					else if (partitions.replicas.Length != replicaCount)
					{
						if (Log.InfoEnabled())
						{
							Log.Info("Namespace " + ns + " replication factor changed from " + partitions.replicas.Length + " to " + replicaCount);
						}

						// Resize partition map. 
						Partitions tmp = new Partitions(partitions, replicaCount);

						CopyPartitionMap();
						partitions = tmp;
						map[ns] = partitions;
					}

					// Parse partition bitmaps.
					for (int i = 0; i < replicaCount; i++)
					{
						begin = ++offset;

						// Find bitmap endpoint
						while (offset < length)
						{
							byte b = buffer[offset];

							if (b == ',' || b == ';')
							{
								break;
							}
							offset++;
						}

						if (offset == begin)
						{
							string response = GetTruncatedResponse();
							throw new AerospikeException.Parse("Empty partition id for namespace " + ns + ". Response=" + response);
						}

						// Log.info("Map: " + namespace + '[' + i + "] " + node);
						DecodeBitmap(node, partitions, i, regime, begin);
					}
					begin = ++offset;
				}
				else
				{
					offset++;
				}
			}
		}

		private void DecodeBitmap(Node node, Partitions partitions, int index, int regime, int begin)
		{
			Node[] nodeArray = partitions.replicas[index];
			int[] regimes = partitions.regimes;
			char[] chars = Encoding.ASCII.GetChars(buffer, begin, offset - begin);
			byte[] restoreBuffer = Convert.FromBase64CharArray(chars, 0, chars.Length);

			for (int i = 0; i < partitionCount; i++)
			{
				Node nodeOld = nodeArray[i];

				if ((restoreBuffer[i >> 3] & (0x80 >> (i & 7))) != 0)
				{
					// Node owns this partition.
					int regimeOld = regimes[i];

					if (regime >= regimeOld)
					{
						// Log.info("Map: " + i);
						if (regime > regimeOld)
						{
							regimes[i] = regime;
						}

						if (nodeOld != null && nodeOld != node)
						{
							// Force previously mapped node to refresh it's partition map on next cluster tend.
							nodeOld.partitionGeneration = -1;
						}
						nodeArray[i] = node;
					}
					else
					{
						if (!regimeError)
						{
							if (Log.InfoEnabled())
							{
								Log.Info(node.ToString() + " regime(" + regime + ") < old regime(" + regimeOld + ")");
							}
							regimeError = true;
						}
					}
				}
			}
		}

		private void CopyPartitionMap()
		{
			if (!copied)
			{
				// Make shallow copy of map.
				map = new Dictionary<string, Partitions>(map);
				copied = true;
			}
		}

		private void ExpectName(string name)
		{
			int begin = offset;

			while (offset < length)
			{
				if (buffer[offset] == '\t')
				{
					string s = ByteUtil.Utf8ToString(buffer, begin, offset - begin).Trim();

					if (name.Equals(s))
					{
						offset++;
						return;
					}
					break;
				}
				offset++;
			}
			throw new AerospikeException.Parse("Failed to find " + name);
		}

		private string GetTruncatedResponse()
		{
			int max = (length > 200) ? 200 : length;
			return ByteUtil.Utf8ToString(buffer, 0, max);
		}
	}
}
