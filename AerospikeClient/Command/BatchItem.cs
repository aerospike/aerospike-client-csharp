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
	public sealed class BatchItem
	{
		public static Dictionary<Key, BatchItem> GenerateMap(Key[] keys)
		{
			Dictionary<Key, BatchItem> keyMap = new Dictionary<Key, BatchItem>(keys.Length);

			for (int i = 0; i < keys.Length; i++)
			{
				Key key = keys[i];
				BatchItem item;

				if (! keyMap.TryGetValue(key, out item))
				{
					item = new BatchItem(i);
					keyMap[key] = item;
				}
				else
				{
					item.AddDuplicate(i);
				}
			}
			return keyMap;
		}

		private int index;
		private List<int> duplicates;

		public BatchItem(int idx)
		{
			this.index = idx;
		}

		public void AddDuplicate(int idx)
		{
			if (duplicates == null)
			{
				duplicates = new List<int>();
				duplicates.Add(index);
				index = 0;
			}
			duplicates.Add(idx);
		}

		public int Index
		{
			get
			{
				return (duplicates == null)? index : duplicates[index++];
			}
		}
	}
}