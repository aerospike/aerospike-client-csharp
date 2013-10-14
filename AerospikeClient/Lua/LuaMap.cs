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
using System.Collections;
using System.Collections.Generic;
using LuaInterface;

namespace Aerospike.Client
{
	public class LuaMap : LuaData
	{
		protected internal readonly Dictionary<object, object> map;

		public LuaMap(Dictionary<object, object> map)
		{
			this.map = map;
		}

		public LuaMap(LuaTable table)
		{
			map = new Dictionary<object, object>(table.Keys.Count);
			IDictionaryEnumerator iter = table.GetEnumerator();

			while (iter.MoveNext())
			{
				map[iter.Key] = iter.Value;
			}
		}

		public LuaMap()
		{
			map = new Dictionary<object, object>();
		}

		public static int size(LuaMap map)
		{
			return map.map.Count;
		}

		public static LuaMapIterator create_iterator(LuaMap map)
		{
			return new LuaMapIterator(map.map.GetEnumerator());
		}

		public static object next_key(LuaMapIterator iter)
		{
			return iter.NextKey();
		}

		public static object next_value(LuaMapIterator iter)
		{
			return iter.NextValue();
		}

		public static bool next(LuaMapIterator iter)
		{
			return iter.iter.MoveNext();
		}

		public static object key(LuaMapIterator iter)
		{
			return iter.iter.Current.Key;
		}

		public static object value(LuaMapIterator iter)
		{
			return iter.iter.Current.Value;
		}

		public object this[object key]
		{
			get 
			{
				object obj;

				if (map.TryGetValue(key, out obj))
				{
					return obj;
				}
				return null; 
			}
			set 
			{
				map[key] = value;
			}
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

		public static void LoadLibrary(Lua lua)
		{
			Type type = typeof(LuaMap);
			lua.RegisterFunction("map.create", null, type.GetConstructor(Type.EmptyTypes));
			lua.RegisterFunction("map.create_set", null, type.GetConstructor(new Type[] { typeof(LuaTable) }));
			lua.RegisterFunction("map.size", null, type.GetMethod("size", new Type[] { type }));
			lua.RegisterFunction("map.create_iterator", null, type.GetMethod("create_iterator", new Type[] { type }));
			lua.RegisterFunction("map.next_key", null, type.GetMethod("next_key", new Type[] { typeof(LuaMapIterator) }));
			lua.RegisterFunction("map.next_value", null, type.GetMethod("next_value", new Type[] { typeof(LuaMapIterator) }));
			lua.RegisterFunction("map.next", null, type.GetMethod("next", new Type[] { typeof(LuaMapIterator) }));
			lua.RegisterFunction("map.key", null, type.GetMethod("key", new Type[] { typeof(LuaMapIterator) }));
			lua.RegisterFunction("map.value", null, type.GetMethod("value", new Type[] { typeof(LuaMapIterator) }));
		}
	}

	public class LuaMapIterator
	{
		protected internal Dictionary<object, object>.Enumerator iter;

		public LuaMapIterator(Dictionary<object, object>.Enumerator iter)
		{
			this.iter = iter;
		}

		public object NextKey()
		{
			if (iter.MoveNext())
			{
				return iter.Current.Key;
			}
			return null;
		}

		public object NextValue()
		{
			if (iter.MoveNext())
			{
				return iter.Current.Value;
			}
			return null;
		}
	}
}