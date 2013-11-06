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
	public sealed class AsyncBatchExistsArray : AsyncMultiCommand
	{
		private readonly BatchNode.BatchNamespace batchNamespace;
		private readonly Policy policy;
		private readonly Dictionary<Key, BatchItem> keyMap;
		private readonly bool[] existsArray;

		public AsyncBatchExistsArray
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			BatchNode.BatchNamespace batchNamespace,
			Policy policy,
			Dictionary<Key, BatchItem> keyMap,
			bool[] existsArray
		) : base(parent, cluster, node, false)
		{
			this.batchNamespace = batchNamespace;
			this.policy = policy;
			this.keyMap = keyMap;
			this.existsArray = existsArray;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetBatchExists(batchNamespace);
		}

		protected internal override void ParseRow(Key key)
		{
			if (opCount > 0)
			{
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			BatchItem item = keyMap[key];

			if (item != null)
			{
				int index = item.Index;
				existsArray[index] = resultCode == 0;
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