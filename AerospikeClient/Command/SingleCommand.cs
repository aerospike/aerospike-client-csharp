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
		protected internal readonly Key key;
		private readonly Partition partition;

		public SingleCommand(Cluster cluster, Key key)
		{
			this.cluster = cluster;
			this.key = key;
			this.partition = new Partition(key);
		}

		protected internal sealed override Node GetNode()
		{
			return cluster.GetNode(partition);
		}

		protected internal void EmptySocket(Connection conn)
		{
			// There should not be any more bytes.
			// Empty the socket to be safe.
			long sz = ByteUtil.BytesToLong(dataBuffer, 0);
			int headerLength = dataBuffer[8];
			int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;

			// Read remaining message bytes.
			if (receiveSize > 0)
			{
				SizeBuffer(receiveSize);
				conn.ReadFully(dataBuffer, receiveSize);
			}
		}
	}
}