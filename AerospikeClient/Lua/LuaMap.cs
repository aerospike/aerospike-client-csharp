/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System;
using System.Collections;
using System.Collections.Generic;
using Neo.IronLua;

namespace Aerospike.Client
{
	public class LuaMap : LuaTable, LuaData
	{
		protected internal readonly IDictionary<object,object> map;

		public LuaMap(IDictionary<object,object> map)
		{
			this.map = map;
		}

		public LuaMap(LuaTable table)
		{
			map = new Dictionary<object, object>(table.Values);
		}

		public LuaMap()
		{
			map = new Dictionary<object, object>(32);
		}

		public static LuaMap create(int capacity)
		{
			return new LuaMap(new Dictionary<object, object>(capacity));
		}

		public static int size(LuaMap map)
		{
			return map.map.Count;
		}

		public static bool remove(LuaMap map, object key)
		{
			if (key != null)
			{
				return map.map.Remove(key);
			}
			return false;
		}

		public static LuaMap clone(LuaMap map)
		{
			return new LuaMap(new Dictionary<object,object>(map.map));
		}

		public static LuaMap merge(LuaMap map1, LuaMap map2, Func<object, object, LuaResult> func)
		{
			Dictionary<object, object> map = new Dictionary<object, object>(map1.map);
			foreach (KeyValuePair<object, object> entry in map2.map)
			{
				if (func != null)
				{
					object value;
					if (map.TryGetValue(entry.Key, out value))
					{
						LuaResult res = func(value, entry.Value);
						map[entry.Key] = res[0];
						continue;
					}
				}
				map[entry.Key] = entry.Value;
			}
			return new LuaMap(map);
		}

		public static LuaMap diff(LuaMap map1, LuaMap map2)
		{
			Dictionary<object, object> map = new Dictionary<object, object>(map1.map.Count + map2.map.Count);

			foreach (KeyValuePair<object, object> entry in map1.map)
			{
				if (!map2.map.ContainsKey(entry.Key))
				{
					map[entry.Key] = entry.Value;
				}
			}

			foreach (KeyValuePair<object, object> entry in map2.map)
			{
				if (!map1.map.ContainsKey(entry.Key))
				{
					map[entry.Key] = entry.Value;
				}
			}
			return new LuaMap(map);
		}
		
		public static Func<object[]> pairs(LuaMap map)
		{
			LuaMapIterator iter = new LuaMapIterator(map.map.GetEnumerator());
			return new Func<object[]>(iter.NextPair);
		}

		public static Func<object> keys(LuaMap map)
		{
			LuaMapIterator iter = new LuaMapIterator(map.map.GetEnumerator());
			return new Func<object>(iter.NextKey);
		}

		public static Func<object> values(LuaMap map)
		{
			LuaMapIterator iter = new LuaMapIterator(map.map.GetEnumerator());
			return new Func<object>(iter.NextValue);
		}

		protected override object OnIndex(object key)
		{
			object obj;

			if (map.TryGetValue(key, out obj))
			{
				return obj;
			}
			return null;
		}

		protected override bool OnNewIndex(object key, object value)
		{
			map[key] = value;
			return true;
		}

		public override string ToString()
		{
			return Util.MapToString(map);
		}

		public object LuaToObject()
		{
			Dictionary<object, object> target = new Dictionary<object, object>(map.Count);

			foreach (KeyValuePair<object, object> entry in map)
			{
				object key = LuaInstance.LuaToObject(entry.Key);
				object value = LuaInstance.LuaToObject(entry.Value);
				target[key] = value;
			}
			return target;
		}
	}

	public class LuaMapIterator
	{
		protected internal IEnumerator iter;

		public LuaMapIterator(IEnumerator iter)
		{
			this.iter = iter;
		}

		public object[] NextPair()
		{
			if (iter.MoveNext())
			{
				KeyValuePair<object,object> pair = (KeyValuePair<object,object>)iter.Current;
				return new object[] { pair.Key, pair.Value };
			}
			return null;
		}

		public object NextKey()
		{
			if (iter.MoveNext())
			{
				KeyValuePair<object, object> pair = (KeyValuePair<object, object>)iter.Current;
				return pair.Key;
			}
			return null;
		}

		public object NextValue()
		{
			if (iter.MoveNext())
			{
				KeyValuePair<object, object> pair = (KeyValuePair<object, object>)iter.Current;
				return pair.Value;
			}
			return null;
		}
	}
}
