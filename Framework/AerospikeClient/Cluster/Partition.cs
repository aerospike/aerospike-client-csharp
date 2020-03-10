/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
			// Must copy hashmap reference for copy on write semantics to work.
			Dictionary<string, Partitions> map = cluster.partitionMap;
			Partitions partitions;

			if (!map.TryGetValue(key.ns, out partitions))
			{
				throw new AerospikeException.InvalidNamespace(key.ns, map.Count);
			}
			return new Partition(partitions, key, policy.replica, false);
		}

		public static Partition Read(Cluster cluster, Policy policy, Key key)
		{
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
			return new Partition(partitions, key, replica, linearize);
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

		public static Node GetNodeBatchRead(Cluster cluster, Key key, Replica replica, Replica replicaSC, uint sequence, uint sequenceSC)
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

			Partition p = new Partition(partitions, key, replica, false);
			p.sequence = sequence;
			return p.GetNodeRead(cluster);
		}

		private readonly Partitions partitions;
		private readonly string ns;
		private readonly Replica replica;
		public readonly uint partitionId;
		private uint sequence;
		private readonly bool linearize;

		private Partition(Partitions partitions, Key key, Replica replica, bool linearize)
		{
			this.partitions = partitions;
			this.ns = key.ns;
			this.replica = replica;
			this.linearize = linearize;
			this.partitionId = GetPartitionId(key.digest);
		}

		public static uint GetPartitionId(byte[] digest)
		{
			// If support for a big endian cpu is added, this code will need to change to 
			// ByteUtil.LittleBytesToInt() .
			return BitConverter.ToUInt32(digest, 0) % Node.PARTITIONS;
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

			for (int i = 0; i < replicas.Length; i++)
			{
				uint index = sequence % (uint)replicas.Length;
				Node node = replicas[index][partitionId];

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
			Node fallback = null;
			bool retry = (sequence > 0);

			for (int i = 1; i <= replicas.Length; i++)
			{
				uint index = sequence % (uint)replicas.Length;
				Node node = replicas[index][partitionId];

				if (node != null && node.Active)
				{
					// If fallback exists, do not retry on node where command failed,
					// even if fallback is not on the same rack.
					if (retry && fallback != null && i == replicas.Length)
					{
						return fallback;
					}

					if (node.HasRack(ns, cluster.rackId))
					{
						return node;
					}

					if (fallback == null)
					{
						fallback = node;
					}
				}
				sequence++;
			}

			if (fallback != null)
			{
				return fallback;
			}

			Node[] nodeArray = cluster.Nodes;
			throw new AerospikeException.InvalidNode(nodeArray.Length, this);
		}

		public Node GetMasterNode(Cluster cluster)
		{
			Node node = partitions.replicas[0][partitionId];

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
				Node node = replicas[index][partitionId];

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
