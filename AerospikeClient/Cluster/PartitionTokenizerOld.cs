/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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