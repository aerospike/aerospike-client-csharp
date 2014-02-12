/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
