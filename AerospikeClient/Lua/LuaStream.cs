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