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
using System.Collections.Concurrent;
using System.Threading;
using LuaInterface;

namespace Aerospike.Client
{
	public abstract class LuaStream
	{
		public static void LoadLibrary(Lua lua)
		{
			Type type = typeof(LuaStream);
			lua.RegisterFunction("stream.read", null, type.GetMethod("read", new Type[] { type }));
			lua.RegisterFunction("stream.write", null, type.GetMethod("write", new Type[] { type, typeof(object) }));
			lua.RegisterFunction("stream.readable", null, type.GetMethod("readable", new Type[] { type }));
			lua.RegisterFunction("stream.writeable", null, type.GetMethod("writeable", new Type[] { type }));
		}

		public static object read(LuaStream stream)
		{
			return stream.Read();
		}

		public static void write(LuaStream stream, object obj)
		{
			stream.Write(obj);
		}

		public static bool readable(LuaStream stream)
		{
			return stream.Readable();
		}

		public static bool writeable(LuaStream stream)
		{
			return stream.Writeable();
		}

		public abstract object Read();
		public abstract void Write(object obj);
		public abstract bool Readable();
		public abstract bool Writeable();
	}

	public sealed class LuaInputStream : LuaStream
	{
		private readonly BlockingCollection<object> queue;

		public LuaInputStream(BlockingCollection<object> queue)
		{
			this.queue = queue;
		}

		public override object Read()
		{
			try
			{
				return queue.Take();
			}
			catch (ThreadInterruptedException)
			{
				return null;
			}
		}

		public override void Write(object value)
		{
			throw new Exception("LuaInputStream is not writeable.");
		}

		public override bool Readable()
		{
			return true;
		}

		public override bool Writeable()
		{
			return false;
		}

		public override string ToString()
		{
			return typeof(LuaInputStream).FullName;
		}
	}

	public sealed class LuaOutputStream : LuaStream
	{
		private readonly ResultSet resultSet;

		public LuaOutputStream(ResultSet resultSet)
		{
			this.resultSet = resultSet;
		}

		public override object Read()
		{
			throw new Exception("LuaOutputStream is not readable.");
		}

		public override void Write(object obj)
		{
			object target = LuaInstance.LuaToObject(obj);
			resultSet.Put(target);
		}

		public override bool Readable()
		{
			return false;
		}

		public override bool Writeable()
		{
			return true;
		}

		public override string ToString()
		{
			return typeof(LuaOutputStream).FullName;
		}
	}
}
