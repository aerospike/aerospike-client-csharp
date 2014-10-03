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
using System;
using System.Text;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Parse node partitions using new protocol. This is more code than a String.split() implementation, 
	/// but it's faster because there are much fewer interim strings.
	/// </summary>
	public sealed class PartitionInfo
	{
		private readonly byte[] buffer;
		private int length;
		private int offset;

		public PartitionInfo(Connection conn, params string[] names)
		{
			// Use low-level info methods and parse byte array directly for maximum performance.
			// Send format:    partition-generation\nreplicas-master\n
			// Receive format: partition-generation\t<gen>\nreplicas-master\t<ns1>:<base 64 encoded bitmap>;<ns2>:<base 64 encoded bitmap>... \n
			Info info = new Info(conn, names);
			this.length = info.GetLength();

			if (length == 0)
			{
				throw new AerospikeException.Parse("Partition info is empty");
			}
			this.buffer = info.GetBuffer();
		}

		public int ParseGeneration()
		{
			ExpectName("partition-generation");

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

		public Dictionary<string, Node[]> ParsePartitions(Dictionary<string, Node[]> map, Node node)
		{
			ExpectName("replicas-master");
			
			int begin = offset;
			bool copied = false;

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
					Node[] nodeArray;

					if (!map.TryGetValue(ns, out nodeArray))
					{
						if (!copied)
						{
							// Make shallow copy of map.
							map = new Dictionary<string, Node[]>(map);
							copied = true;
						}
						nodeArray = new Node[Node.PARTITIONS];
						map[ns] = nodeArray;
					}

					int bitMapLength = offset - begin;
					char[] chars = Encoding.ASCII.GetChars(buffer, begin, bitMapLength);
					byte[] restoreBuffer = Convert.FromBase64CharArray(chars, 0, chars.Length);

					for (int i = 0; i < Node.PARTITIONS; i++)
					{
						if ((restoreBuffer[i >> 3] & (0x80 >> (i & 7))) != 0)
						{
							//Log.info("Map: " + namespace + ',' + i + ',' + node);
							nodeArray[i] = node;
						}
					}
					begin = ++offset;
				}
				else
				{
					offset++;
				}
			}
			return (copied)? map : null;
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
