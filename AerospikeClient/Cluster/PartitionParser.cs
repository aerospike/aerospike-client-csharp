/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Parse node's master (and optionally prole) partitions.
	/// </summary>
	public sealed class PartitionParser : Info
	{
		internal const string PartitionGeneration = "partition-generation";
		internal const string Replicas = "replicas";

		private Dictionary<string, Partitions> map;
		private readonly int partitionCount;
		private readonly int generation;
		private bool copied;
		private bool regimeError;

		public PartitionParser(Connection conn, Node node, Dictionary<string, Partitions> map, int partitionCount)
			: base(node, conn, PartitionGeneration, Replicas)
		{
			// Send format: partition-generation\nreplicas\n
			this.partitionCount = partitionCount;
			this.map = map;

			if (length == 0)
			{
				throw new AerospikeException.Parse("Partition info is empty");
			}

			this.generation = ParseGeneration();
			ParseReplicasAll(node, Replicas);
		}

		public int Generation
		{
			get { return generation; }
		}

		public bool IsPartitionMapCopied
		{
			get { return copied; }
		}

		public Dictionary<string, Partitions> PartitionMap
		{
			get { return map; }
		}

		public int ParseGeneration()
		{
			ParseName(PartitionGeneration);
			int gen = ParseInt();
			Expect('\n');
			return gen;
		}

		private void ParseReplicasAll(Node node, string command)
		{
			// Use low-level info methods and parse byte array directly for maximum performance.
			// Receive format: replicas\t
			//                 <ns1>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
			//                 <ns2>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
			ParseName(command);

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
							Log.Info(node.cluster.context, "Namespace " + ns + " replication factor changed from " + partitions.replicas.Length + " to " + replicaCount);
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

			Span<byte> bufferChars = buffer.AsSpan(start: begin, length: offset - begin);
			bufferChars = bufferChars.TrimEnd((byte)'\n');
			Span<byte> restoreBuffer = stackalloc byte[Base64.GetMaxDecodedFromUtf8Length(bufferChars.Length)];
			var decodeStatus = Base64.DecodeFromUtf8(bufferChars, restoreBuffer, out _, out int restoreBufferLength);
			Debug.Assert(decodeStatus == OperationStatus.Done);
			restoreBuffer = restoreBuffer[..restoreBufferLength]; // To get proper exception for out-of-bounds access.

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
								Log.Info(node.cluster.context, node.ToString() + " regime(" + regime + ") < old regime(" + regimeOld + ")");
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
	}
}
