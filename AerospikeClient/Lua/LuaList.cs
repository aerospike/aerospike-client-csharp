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
	public class LuaList : LuaData
	{
		protected internal readonly List<object> list;

		public LuaList(List<object> list)
		{
			this.list = list;
		}

		public LuaList(LuaTable table)
		{
			ICollection values = table.Values;
			list = new List<object>(values.Count);

			foreach (object value in values)
			{
				list.Add(value);
			}
		}

		public LuaList()
		{
			list = new List<object>();
		}

		public static int size(LuaList list)
		{
			return list.list.Count;
		}

		public static void append(LuaList list, object obj)
		{
			list.list.Add(obj);
		}

		public static void prepend(LuaList list, object obj)
		{
			list.list.Insert(0, obj);
		}

		public static LuaList take(LuaList list, int count)
		{
			List<object> src = list.list;

			if (count > src.Count)
			{
				count = src.Count;
			}
			return new LuaList(src.GetRange(0, count));
		}

		public static LuaList drop(LuaList list, int count)
		{
			List<object> src = list.list;

			if (count >= src.Count)
			{
				return new LuaList(new List<object>(0));
			}
			return new LuaList(src.GetRange(count, src.Count - count));
		}

		public static LuaListIterator create_iterator(LuaList list)
		{
			return new LuaListIterator(list.list.GetEnumerator());
		}

		public static object next(LuaListIterator iter)
		{
			return iter.next();
		}

		public object this[int index]
		{
			get { return list[index-1]; }
			set { list[index-1] = value; }
		}

		public override string ToString()
		{
			return Util.ListToString(list);
		}

		public object LuaToObject()
		{
			List<object> target = new List<object>(list.Count);

			foreach (object value in list)
			{
				object obj = LuaInstance.LuaToObject(value);
				target.Add(obj);
			}
			return target;
		}

		public static void LoadLibrary(Lua lua)
		{
			Type type = typeof(LuaList);
			lua.RegisterFunction("list.create", null, type.GetConstructor(Type.EmptyTypes));
			lua.RegisterFunction("list.create_set", null, type.GetConstructor(new Type[] { typeof(LuaTable) }));
			lua.RegisterFunction("list.size", null, type.GetMethod("size", new Type[] { type }));
			lua.RegisterFunction("list.append", null, type.GetMethod("append", new Type[] { type, typeof(object) }));
			lua.RegisterFunction("list.prepend", null, type.GetMethod("prepend", new Type[] { type, typeof(object) }));
			lua.RegisterFunction("list.take", null, type.GetMethod("take", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("list.drop", null, type.GetMethod("drop", new Type[] { type, typeof(int) }));
			lua.RegisterFunction("list.create_iterator", null, type.GetMethod("create_iterator", new Type[] { type }));
			lua.RegisterFunction("list.next", null, type.GetMethod("next", new Type[] { typeof(LuaListIterator) }));
		}
	}

	public class LuaListIterator
	{
		protected internal List<object>.Enumerator iter;

		public LuaListIterator(List<object>.Enumerator iter)
		{
			this.iter = iter;
		}

		public object next()
		{
			if (iter.MoveNext())
			{
				return iter.Current;
			}
			return null;
		}
	}
}