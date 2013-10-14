/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System;

namespace Aerospike.Client
{
	public sealed class Partition
	{
		public readonly string ns;
		public readonly uint partitionId;

		public Partition(Key key)
		{
			this.ns = key.ns;
			this.partitionId = BitConverter.ToUInt32(key.digest, 0) % Node.PARTITIONS;
		}

		public Partition(string ns, uint partitionId)
		{
			this.ns = ns;
			this.partitionId = partitionId;
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
			Partition other = (Partition) obj;
			return this.ns.Equals(other.ns) && this.partitionId == other.partitionId;
		}
	}
}