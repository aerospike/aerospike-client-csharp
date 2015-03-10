/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
	public class LuaList : LuaData
	{
		protected internal readonly List<object> list;

		public LuaList(List<object> list)
		{
			this.list = list;
		}

		public LuaList(LuaTable table)
		{
			list = new List<object>(table.ArrayList);
		}

		public LuaList()
		{
			list = new List<object>();
		}

		public static LuaList create(int capacity)
		{
			return new LuaList(new List<object>(capacity));
		}

		public static int size(LuaList list)
		{
			return list.list.Count;
		}

		public static Func<object> iterator(LuaList list)
		{
			LuaListIterator iter = new LuaListIterator(list.list.GetEnumerator());
			return new Func<object>(iter.next);
		}

		public static void insert(LuaList list, int index, object obj)
		{
			list.list.Insert(index - 1, obj);
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

		public static void remove(LuaList list, int index)
		{
			list.list.RemoveAt(index - 1);
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

		public static void trim(LuaList list, int index)
		{
			list.list.RemoveRange(index-1, list.list.Count - index + 1);
		}
		
		public static LuaList clone(LuaList list)
		{
			return new LuaList(new List<object>(list.list));
		}

		public static void concat(LuaList list1, LuaList list2)
		{
			list1.list.AddRange(list2.list);
		}

		public static LuaList merge(LuaList list1, LuaList list2)
		{
			List<object> list = new List<object>(list1.list);
			list.AddRange(list2.list);
			return new LuaList(list);
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
