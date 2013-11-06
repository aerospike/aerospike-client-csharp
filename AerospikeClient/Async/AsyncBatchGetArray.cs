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
	public sealed class AsyncBatchGetArray : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batchNamespace;
		private readonly Policy policy;
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly HashSet<string> binNames;
		private readonly Record[] records;
		private readonly int readAttr;

		public AsyncBatchGetArray
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batchNamespace,
			Policy policy,
			Dictionary<Key, BatchItem> keyMap,
			HashSet<string> binNames,
			Record[] records,
			int readAttr
		) : base(parent, cluster, node, false)
		{
			this.batchNamespace = batchNamespace;
			this.policy = policy;
			this.keyMap = keyMap;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchGet(batchNamespace, binNames, readAttr);
		}

		protected internal override void ParseRow(Key key)
		{
			BatchItem item = keyMap[key];

			if (item != null)
			{
				if (resultCode == 0)
				{
					int index = item.Index;
					records[index] = ParseRecord();
				}
			}
			else
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Unexpected batch key returned: " + key.ns + ',' + ByteUtil.BytesToHexString(key.digest));
				}
			}
		}
	}
}