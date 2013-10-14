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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public class LuaMapIterator
	{
		protected internal Dictionary<object,object>.Enumerator iter;

		public LuaMapIterator(Dictionary<object,object>.Enumerator iter)
		{
			this.iter = iter;
		}

		public object[] Next()
		{
			if (iter.MoveNext())
			{
				return new object[] {iter.Current.Key, iter.Current.Value};
			}
			return null;
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