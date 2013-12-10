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
using System.Net.Sockets;

namespace Aerospike.Client
{
	public abstract class AsyncSingleCommand : AsyncCommand
	{
		protected internal readonly Key key;
		private readonly Partition partition;

		public AsyncSingleCommand(AsyncCluster cluster, Key key) 
			: base(cluster)
		{
			this.key = key;
			this.partition = new Partition(key);
		}

		protected internal sealed override AsyncNode GetNode()
		{
			return (AsyncNode)cluster.GetNode(partition);
		}

		protected internal sealed override void ParseCommand()
		{
			ParseResult();
			Finish();
		}

		protected internal abstract void ParseResult();
	}
}