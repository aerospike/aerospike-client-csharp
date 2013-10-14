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
using LuaInterface;

namespace Aerospike.Client
{
	public class LuaAerospike
	{
		public static void log(int level, string message)
		{
			switch (level)
			{
				case 1:
					Log.LogMessage(Log.Level.WARN, message);
					break;
				case 2:
					Log.LogMessage(Log.Level.INFO, message);
					break;
				case 3:
				case 4:
					Log.LogMessage(Log.Level.DEBUG, message);
					break;
			}
		}

		public static void LoadLibrary(Lua lua)
		{
			Type type = typeof(LuaAerospike);
			lua.RegisterFunction("aero.log", null, type.GetMethod("log", new Type[] { typeof(int), typeof(string) }));
		}
	}
}