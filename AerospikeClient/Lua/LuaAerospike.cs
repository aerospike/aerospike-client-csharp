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
