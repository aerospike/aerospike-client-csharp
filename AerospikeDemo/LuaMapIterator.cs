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
