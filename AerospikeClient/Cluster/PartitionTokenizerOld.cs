/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Parse node partitions using old protocol. This is more code than a String.split() implementation, 
	/// but it's faster because there are much fewer interim strings.
	/// </summary>
	public sealed class PartitionTokenizerOld
	{
		private const string ReplicasName = "replicas-write";

		private readonly byte[] buffer;
		private int length;
		private int offset;

		public PartitionTokenizerOld(Connection conn)
		{
			// Use low-level info methods and parse byte array directly for maximum performance.
			// Send format:    replicas-write\n
			// Receive format: replicas-write\t<ns1>:<partition id1>;<ns2>:<partition id2>...\n
			Info info = new Info(conn, ReplicasName);
			this.length = info.GetLength();

			if (length == 0)
			{
				throw new AerospikeException.Parse(ReplicasName + " is empty");
			}
			this.buffer = info.GetBuffer();
			this.offset = ReplicasName.Length + 1; // Skip past name and tab
		}

		public Dictionary<string, Node[]> UpdatePartition(Dictionary<string, Node[]> map, Node node)
		{
			Partition partition;
			bool copied = false;

			while ((partition = GetNext()) != null)
			{
				Node[] nodeArray;

				if (!map.TryGetValue(partition.ns, out nodeArray))
				{
					if (!copied)
					{
						// Make shallow copy of map.
						map = new Dictionary<string, Node[]>(map);
						copied = true;
					}
					nodeArray = new Node[Node.PARTITIONS];
					map[partition.ns] = nodeArray;
				}
				// Log.debug(partition.toString() + ',' + node.getName());
				nodeArray[partition.partitionId] = node;
			}
			return (copied)? map : null;
		}

		private Partition GetNext()
		{
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

					// Parse partition id.
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

					uint partitionId = ByteUtil.Utf8DigitsToInt(buffer, begin, offset);

					if (partitionId < 0 || partitionId >= Node.PARTITIONS)
					{
						string response = GetTruncatedResponse();
						string partitionString = ByteUtil.Utf8ToString(buffer, begin, offset - begin);
						throw new AerospikeException.Parse("Invalid partition id " + partitionString + " for namespace " + ns + ". Response=" + response);
					}
					begin = ++offset;
					return new Partition(ns, partitionId);
				}
				offset++;
			}
			return null;

		}

		private string GetTruncatedResponse()
		{
			int max = (length > 200) ? 200 : length;
			return ByteUtil.Utf8ToString(buffer, 0, max);
		}
	}
}
