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
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly Record[] records;

		public AsyncBatchGetArray(AsyncMultiExecutor parent, AsyncCluster cluster, AsyncNode node, Dictionary<Key, BatchItem> keyMap, HashSet<string> binNames, Record[] records) 
			: base(parent, cluster, node, false, binNames)
		{
			this.keyMap = keyMap;
			this.records = records;
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