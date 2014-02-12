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
