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

namespace Aerospike.Client
{
	public sealed class AsyncOperate : AsyncRead
	{
		private readonly WritePolicy policy;
		private readonly Operation[] operations;

		public AsyncOperate(AsyncCluster cluster, WritePolicy policy, RecordListener listener, Key key, Operation[] operations) 
			: base(cluster, policy, listener, key, null)
		{
			this.policy = (policy == null) ? new WritePolicy() : policy;
			this.operations = operations;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(policy, key, operations);
		}
	}
}