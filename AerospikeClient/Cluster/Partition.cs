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
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class Partition
	{
		public static Partition Write(Cluster cluster, Policy policy, Key key)
		{
			if (cluster == null) return null;

			// Must copy hashmap reference for copy on write semantics to work.
			Dictionary<string, Partitions> map = cluster.partitionMap;
			Partitions partitions;

			if (!map.TryGetValue(key.ns, out partitions))
			{
				throw new AerospikeException.InvalidNamespace(key.ns, map.Count);
			}
			return new Partition(partitions, key, policy.replica, null, false);
		}

		public static Partition Read(Cluster cluster, Policy policy, Key key)
		{
			if (cluster == null) return null;

			// Must copy hashmap reference for copy on write semantics to work.
			Dictionary<string, Partitions> map = cluster.partitionMap;
			Partitions partitions;

			if (!map.TryGetValue(key.ns, out partitions))
			{
				throw new AerospikeException.InvalidNamespace(key.ns, map.Count);
			}

			Replica replica;
			bool linearize;

			if (partitions.scMode)
			{
				switch (policy.readModeSC)
				{
					case ReadModeSC.SESSION:
						replica = Replica.MASTER;
						linearize = false;
						break;

					case ReadModeSC.LINEARIZE:
						replica = policy.replica == Replica.PREFER_RACK ? Replica.SEQUENCE : policy.replica;
						linearize = true;
						break;

					default:
						replica = policy.replica;
						linearize = false;
						break;
				}
			}
			else
			{
				replica = policy.replica;
				linearize = false;
			}
			return new Partition(partitions, key, replica, null, linearize);
		}

		public static Replica GetReplicaSC(Policy policy)
		{
			switch (policy.readModeSC)
			{
				case ReadModeSC.SESSION:
					return Replica.MASTER;

				case ReadModeSC.LINEARIZE:
					return policy.replica == Replica.PREFER_RACK ? Replica.SEQUENCE : policy.replica;

				default:
					return policy.replica;
			}
		}

		public static Node GetNodeBatchWrite
		(
			Cluster cluster,
			Key key,
			Replica replica,
			Node prevNode,
			uint sequence
		)
		{
			Dictionary<string, Partitions> map = cluster.partitionMap;
			Partitions partitions;

			if (!map.TryGetValue(key.ns, out partitions))
			{
				throw new AerospikeException.InvalidNamespace(key.ns, map.Count);
			}

			Partition p = new Partition(partitions, key, replica, prevNode, false);
			p.sequence = sequence;
			return p.GetNodeWrite(cluster);
		}

		public static Node GetNodeBatchRead
		(
			Cluster cluster,
			Key key,
			Replica replica,
			Replica replicaSC,
			Node prevNode,
			uint sequence,
			uint sequenceSC
		)
		{
			// Must copy hashmap reference for copy on write semantics to work.
			Dictionary<string, Partitions> map = cluster.partitionMap;
			Partitions partitions;

			if (!map.TryGetValue(key.ns, out partitions))
			{
				throw new AerospikeException.InvalidNamespace(key.ns, map.Count);
			}

			if (partitions.scMode)
			{
				replica = replicaSC;
				sequence = sequenceSC;
			}

			Partition p = new Partition(partitions, key, replica, prevNode, false);
			p.sequence = sequence;
			return p.GetNodeRead(cluster);
		}

		private Partitions partitions;
		private readonly string ns;
		private Node prevNode;
		private readonly Replica replica;
		public uint partitionId;
		private uint sequence;
		private readonly bool linearize;

		private Partition(Partitions partitions, Key key, Replica replica, Node prevNode, bool linearize)
		{
			this.partitions = partitions;
			this.ns = key.ns;
			this.replica = replica;
			this.prevNode = prevNode;
			this.linearize = linearize;
			this.partitionId = GetPartitionId(key.digest);
		}

		public Partition(string ns, Replica replica)
		{
			this.ns = ns;
			this.replica = replica;
			this.linearize = false;
		}

		public static uint GetPartitionId(byte[] digest)
		{
			// If support for a big endian cpu is added, this code will need to change to 
			// ByteUtil.LittleBytesToInt() .
			return BitConverter.ToUInt32(digest, 0) % Node.PARTITIONS;
		}

		public Node GetNodeQuery(Cluster cluster, Partitions partitions, PartitionStatus ps)
		{
			this.partitions = partitions;
			this.partitionId = (uint)ps.id;
			this.sequence = (uint)ps.sequence;
			this.prevNode = ps.node;

			Node node = GetNodeRead(cluster);
			ps.node = node;
			ps.sequence = (int)this.sequence;
			ps.retry = false;
			return node;
		}

		public Node GetNodeRead(Cluster cluster)
		{
			switch (replica)
			{
				default:
				case Replica.SEQUENCE:
					return GetSequenceNode(cluster);

				case Replica.PREFER_RACK:
					return GetRackNode(cluster);

				case Replica.MASTER:
					return GetMasterNode(cluster);

				case Replica.MASTER_PROLES:
					return GetMasterProlesNode(cluster);

				case Replica.RANDOM:
					return cluster.GetRandomNode();
			}
		}

		public Node GetNodeWrite(Cluster cluster)
		{
			switch (replica)
			{
				default:
				case Replica.SEQUENCE:
				case Replica.PREFER_RACK:
					return GetSequenceNode(cluster);

				case Replica.MASTER:
				case Replica.MASTER_PROLES:
				case Replica.RANDOM:
					return GetMasterNode(cluster);
			}
		}

		public void PrepareRetryRead(bool timeout)
		{
			if (!timeout || !linearize)
			{
				sequence++;
			}
		}

		public void PrepareRetryWrite(bool timeout)
		{
			if (!timeout)
			{
				sequence++;
			}
		}

		public Node GetSequenceNode(Cluster cluster)
		{
			Node[][] replicas = partitions.replicas;
			uint max = (uint)replicas.Length;

			for (uint i = 0; i < max; i++)
			{
				uint index = sequence % max;
				Node node = Volatile.Read(ref replicas[index][partitionId]);

				if (node != null && node.Active)
				{
					return node;
				}
				sequence++;
			}
			Node[] nodeArray = cluster.Nodes;
			throw new AerospikeException.InvalidNode(nodeArray.Length, this);
		}

		private Node GetRackNode(Cluster cluster)
		{
			Node[][] replicas = partitions.replicas;
			uint max = (uint)replicas.Length;
			uint seq1 = 0;
			uint seq2 = 0;
			Node fallback1 = null;
			Node fallback2 = null;

			foreach (int rackId in cluster.rackIds)
			{
				uint seq = sequence;

				for (uint i = 0; i < max; i++)
				{
					uint index = seq % max;
					Node node = Volatile.Read(ref replicas[index][partitionId]);
					// Log.Info("Try " + rackId + ',' + index + ',' + prevNode + ',' + node + ',' + node.HasRack(ns, rackId));

					if (node != null)
					{
						// Avoid retrying on node where command failed
						// even if node is the only one on the same rack.
						if (node != prevNode)
						{
							if (node.HasRack(ns, rackId))
							{
								if (node.Active)
								{
									// Log.Info("Found node on same rack: " + node);
									prevNode = node;
									sequence = seq;
									return node;
								}
							}
							else if (fallback1 == null && node.Active)
							{
								// Meets all criteria except not on same rack.
								fallback1 = node;
								seq1 = seq;
							}
						}
						else if (fallback2 == null && node.Active)
						{
							// Previous node is the least desirable fallback.
							fallback2 = node;
							seq2 = seq;
						}
					}
					seq++;
				}
			}

			// Return node on a different rack if it exists.
			if (fallback1 != null)
			{
				// Log.Info("Found fallback node: " + fallback1);
				prevNode = fallback1;
				sequence = seq1;
				return fallback1;
			}

			// Return previous node if it still exists.
			if (fallback2 != null)
			{
				// Log.Info("Found previous node: " + fallback2);
				prevNode = fallback2;
				sequence = seq2;
				return fallback2;
			}

			// Failed to find suitable node.			
			Node[] nodeArray = cluster.Nodes;
			throw new AerospikeException.InvalidNode(nodeArray.Length, this);
		}

		public Node GetMasterNode(Cluster cluster)
		{
			Node node = Volatile.Read(ref partitions.replicas[0][partitionId]);

			if (node != null && node.Active)
			{
				return node;
			}

			Node[] nodeArray = cluster.Nodes;
			throw new AerospikeException.InvalidNode(nodeArray.Length, this);
		}

		public Node GetMasterProlesNode(Cluster cluster)
		{
			Node[][] replicas = partitions.replicas;

			for (int i = 0; i < replicas.Length; i++)
			{
				int index = Math.Abs(Interlocked.Increment(ref cluster.replicaIndex) % replicas.Length);
				Node node = Volatile.Read(ref replicas[index][partitionId]);

				if (node != null && node.Active)
				{
					return node;
				}
			}

			Node[] nodeArray = cluster.Nodes;
			throw new AerospikeException.InvalidNode(nodeArray.Length, this);
		}

		public override string ToString()
		{
			return ns + ':' + partitionId;
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = prime + ns.GetHashCode();
			result = prime * result + (int)partitionId;
			return result;
		}

		public override bool Equals(object obj)
		{
			Partition other = (Partition)obj;
			return this.ns.Equals(other.ns) && this.partitionId == other.partitionId;
		}
	}
}
