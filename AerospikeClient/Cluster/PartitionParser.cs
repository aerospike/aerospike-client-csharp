/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
		internal const string ReplicasMaster = "replicas-master";
		internal const string ReplicasAll = "replicas-all";

		private Dictionary<string, Node[][]> map;
		private readonly byte[] buffer;
		private readonly int partitionCount;
		private readonly int generation;
		private int length;
		private int offset;
		private bool copied;

		public PartitionParser(Connection conn, Node node, Dictionary<string, Node[][]> map, int partitionCount, bool requestProleReplicas)
		{
			// Send format 1:  partition-generation\nreplicas-master\n
			// Send format 2:  partition-generation\nreplicas-all\n
			this.partitionCount = partitionCount;
			this.map = map;

			string command = (requestProleReplicas) ? ReplicasAll : ReplicasMaster;
			Info info = new Info(conn, PartitionGeneration, command);
			this.length = info.length;

			if (length == 0)
			{
				throw new AerospikeException.Parse("Partition info is empty");
			}
			this.buffer = info.buffer;

			generation = ParseGeneration();

			if (requestProleReplicas)
			{
				ParseReplicasAll(node);
			}
			else
			{
				ParseReplicasMaster(node);
			}
		}

		public int Generation
		{
			get {return generation;}
		}

		public bool IsPartitionMapCopied
		{
			get {return copied;}
		}

		public Dictionary<string, Node[][]> PartitionMap
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

		private void ParseReplicasMaster(Node node)
		{
			// Use low-level info methods and parse byte array directly for maximum performance.
			// Receive format: replicas-master\t<ns1>:<base 64 encoded bitmap1>;<ns2>:<base 64 encoded bitmap2>...\n
			ExpectName(ReplicasMaster);

			int begin = offset;

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

					// Parse partition bitmap.
					while (offset < length)
					{
						byte b = buffer[offset];

						if (b == ';' || b == '\n')
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

					Node[][] replicaArray;

					if (!map.TryGetValue(ns, out replicaArray))
					{
						replicaArray = new Node[1][];
						replicaArray[0] = new Node[partitionCount];
						CopyPartitionMap();
						map[ns] = replicaArray;
					}

					// Log.info("Map: " + namespace + "[0] " + node);
					DecodeBitmap(node, replicaArray[0], begin);
					begin = ++offset;
				}
				else
				{
					offset++;
				}
			}
		}

		private void ParseReplicasAll(Node node)
		{
			// Use low-level info methods and parse byte array directly for maximum performance.
			// Receive format: replicas-all\t
			//                 <ns1>:<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
			//                 <ns2>:<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
			ExpectName(ReplicasAll);

			int begin = offset;

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

					// Ensure replicaArray is correct size.
					Node[][] replicaArray;

					if (!map.TryGetValue(ns, out replicaArray))
					{
						// Create new replica array. 
						replicaArray = new Node[replicaCount][];

						for (int i = 0; i < replicaCount; i++)
						{
							replicaArray[i] = new Node[partitionCount];
						}

						CopyPartitionMap();
						map[ns] = replicaArray;
					}
					else if (replicaArray.Length != replicaCount)
					{
						if (Log.InfoEnabled())
						{
							Log.Info("Namespace " + ns + " replication factor changed from " + replicaArray.Length + " to " + replicaCount);
						}

						// Resize replica array. 
						Node[][] replicaTarget = new Node[replicaCount][];

						if (replicaArray.Length < replicaCount)
						{
							int i = 0;

							// Copy existing entries.
							for (; i < replicaArray.Length; i++)
							{
								replicaTarget[i] = replicaArray[i];
							}

							// Create new entries.
							for (; i < replicaCount; i++)
							{
								replicaTarget[i] = new Node[partitionCount];
							}
						}
						else
						{
							// Copy existing entries.
							for (int i = 0; i < replicaCount; i++)
							{
								replicaTarget[i] = replicaArray[i];
							}
						}

						CopyPartitionMap();
						replicaArray = replicaTarget;
						map[ns] = replicaArray;
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
						DecodeBitmap(node, replicaArray[i], begin);
					}
					begin = ++offset;
				}
				else
				{
					offset++;
				}
			}
		}

		private void DecodeBitmap(Node node, Node[] nodeArray, int begin)
		{
			char[] chars = Encoding.ASCII.GetChars(buffer, begin, offset - begin);
			byte[] restoreBuffer = Convert.FromBase64CharArray(chars, 0, chars.Length);

			for (int i = 0; i < partitionCount; i++)
			{
				Node nodeOld = nodeArray[i];

				if ((restoreBuffer[i >> 3] & (0x80 >> (i & 7))) != 0)
				{
					// Node owns this partition.
					// Log.info("Map: " + i);
					if (nodeOld != null && nodeOld != node)
					{
						// Force previously mapped node to refresh it's partition map on next cluster tend.
						nodeOld.partitionGeneration = -1;
					}
					nodeArray[i] = node;
				}
				else
				{
					// Node does not own partition.
					if (node == nodeOld)
					{
						// Must erase previous map.
						nodeArray[i] = null;
					}
				}
			}
		}

		private void CopyPartitionMap()
		{
			if (!copied)
			{
				// Make shallow copy of map.
				map = new Dictionary<string, Node[][]>(map);
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
