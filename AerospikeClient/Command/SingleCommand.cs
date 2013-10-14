/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
namespace Aerospike.Client
{
	public abstract class SingleCommand : SyncCommand
	{
		private readonly Cluster cluster;
		private readonly Partition partition;

		public SingleCommand(Cluster cluster, Key key)
		{
			this.cluster = cluster;
			this.partition = new Partition(key);
			this.receiveBuffer = ThreadLocalData2.GetBuffer();
		}

		public void ResizeReceiveBuffer(int size)
		{
			if (size > receiveBuffer.Length)
			{
				receiveBuffer = ThreadLocalData2.ResizeBuffer(size);
			}
		}

		protected internal sealed override Node GetNode()
		{
			return cluster.GetNode(partition);
		}
	}
}