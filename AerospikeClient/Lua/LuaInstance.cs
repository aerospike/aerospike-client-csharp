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
				Unpacker unpacker = new Unpacker(buf, offset, len, true);
				return unpacker.UnpackList();
			}

			case ParticleType.MAP: 
			{
				Unpacker unpacker = new Unpacker(buf, offset, len, true);
				return unpacker.UnpackMap();
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
