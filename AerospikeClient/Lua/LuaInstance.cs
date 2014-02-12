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
using System.Reflection;
using System.IO;
using LuaInterface;

namespace Aerospike.Client
{
	public sealed class LuaInstance
	{
		private readonly Lua lua;
		private readonly LuaFunction require;

		public LuaInstance()
		{
			lua = new Lua();
			lua["package.path"] = LuaConfig.PackagePath;
			require = lua.GetFunction("require");

			LuaTable packages = (LuaTable)lua["package.loaded"];
			LoadSystem(packages, "aslib");
			LuaBytes.LoadLibrary(lua);
			LuaList.LoadLibrary(lua);
			LuaMap.LoadLibrary(lua);
			LuaStream.LoadLibrary(lua);
			LoadSystem(packages, "as");
			LoadSystem(packages, "stream_ops");
			LoadSystem(packages, "aerospike");
			LuaAerospike.LoadLibrary(lua);
		}

		public void LoadSystem(LuaTable packages, string packageName)
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			string resourcePath = "Aerospike.Client.Resources." + packageName + ".lua";

			using (Stream stream = assembly.GetManifestResourceStream(resourcePath))
			{
				using (StreamReader reader = new StreamReader(stream))
				{
					string chunk = reader.ReadToEnd();
					lua.DoString(chunk, packageName);
					packages[packageName] = true;
				}
			}
		}

		public void Load(string packageName)
		{
			require.Call(packageName);
		}

		public void Call(string functionName, params object[] args)
		{
			LuaFunction function = (LuaFunction)lua[functionName];
			function.Call(args);
		}

		public LuaFunction GetFunction(string functionName)
		{
			return (LuaFunction)lua[functionName];
		}

		public static object BytesToLua(int type, byte[] buf, int offset, int len) 
		{
			if (len <= 0) {
				return null;
			}
		
			switch (type) {
			case ParticleType.STRING:
				return ByteUtil.Utf8ToString(buf, offset, len);
			
			case ParticleType.INTEGER:
				return ByteUtil.BytesToNumber(buf, offset, len);
		
			case ParticleType.BLOB:
				byte[] dest = new byte[len];
				Array.Copy(buf, offset, dest, 0, len);
				return new LuaBytes(dest);

			case ParticleType.CSHARP_BLOB:
				return ByteUtil.BytesToObject(buf, offset, len);
			
			case ParticleType.LIST:
			{
				MsgUnpacker unpacker = new MsgUnpacker(true);
				return unpacker.ParseList(buf, offset, len);
			}

			case ParticleType.MAP: 
			{
				MsgUnpacker unpacker = new MsgUnpacker(true);
				return unpacker.ParseMap(buf, offset, len);
			}
		
			default:
				return null;
			}
		}

		public static object LuaToObject(object source)
		{
			if (source is LuaData)
			{
				LuaData data = (LuaData)source;
				return data.LuaToObject();
			}
			return source;
		}
	}
}
