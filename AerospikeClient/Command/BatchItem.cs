/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
