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
	public sealed class AsyncScan : AsyncMultiCommand
	{
		private readonly ScanPolicy policy;
		private readonly RecordSequenceListener listener;
		private readonly string ns;
		private readonly string setName;
		private readonly string[] binNames;

		public AsyncScan
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			ScanPolicy policy,
			RecordSequenceListener listener,
			string ns,
			string setName,
			string[] binNames
		) : base(parent, cluster, node, true)
		{
			this.policy = policy;
			this.listener = listener;
			this.ns = ns;
			this.setName = setName;
			this.binNames = binNames;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetScan(policy, ns, setName, binNames);
		}

		protected internal override void ParseRow(Key key)
		{
			Record record = ParseRecord();
			listener.OnRecord(key, record);
		}
	}
}