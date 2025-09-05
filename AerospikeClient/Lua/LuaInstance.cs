/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
using Neo.IronLua;
using System.Reflection;
using System.Text;

namespace Aerospike.Client
{
	public sealed class LuaInstance
	{
		private readonly Lua lua;
		private readonly LuaGlobal global;
		private readonly HashSet<string> packages;
		public bool debug;

		public LuaInstance()
		{
			lua = new Lua();
			packages = new HashSet<string>();

			try
			{
				global = lua.CreateEnvironment();
				global["package.path"] = LuaConfig.PackagePath;

				LuaAerospike.LoadLibrary(this);
				LuaStream.LoadLibrary(this);

				Assembly assembly = Assembly.GetExecutingAssembly();
				LoadSystemPackage(assembly, "aslib");
				LoadSystemPackage(assembly, "stream_ops");
				LoadSystemPackage(assembly, "aerospike");
			}
			catch (Exception)
			{
				lua.Dispose();
				throw;
			}
		}

		public void Close()
		{
			lua.Dispose();
		}

		public void LoadPackage(Statement statement)
		{
			if (!packages.Contains(statement.packageName))
			{
				if (statement.packageContents != null)
				{
					LoadPackageFromString(statement.packageName, statement.packageContents);
				}
				else if (statement.resourceAssembly == null || statement.resourcePath == null)
				{
					LoadPackageFromFile(statement.packageName);
				}
				else
				{
					LoadPackageFromResource(statement.resourceAssembly, statement.resourcePath, statement.packageName);
				}
				packages.Add(statement.packageName);
			}
		}

		private void LoadSystemPackage(Assembly assembly, string packageName)
		{
			string resourcePath = "Aerospike.Client.LuaResources." + packageName + ".lua";
			LoadPackageFromResource(assembly, resourcePath, packageName);
		}

		private void LoadPackageFromResource(Assembly assembly, string resourcePath, string packageName)
		{
			using (Stream stream = assembly.GetManifestResourceStream(resourcePath))
			{
				using (StreamReader reader = new StreamReader(stream))
				{
					global.DoChunk(reader, packageName);
					/*
					if (debug)
					{
						global.DoChunk(lua.CompileChunk(reader, packageName, Lua.DefaultDebugEngine));
					}
					else
					{
						global.DoChunk(reader, packageName);
					}
					*/
				}
			}
		}

		private void LoadPackageFromFile(string packageName)
		{
			string path = FindFile(packageName);
			global.DoChunk(path);
			/*
			if (debug)
			{
				global.DoChunk(lua.CompileChunk(path, Lua.DefaultDebugEngine));
			}
			else
			{
				global.DoChunk(path);
			}
			*/
		}

		private void LoadPackageFromString(string packageName, string packageContents)
		{
			global.DoChunk(packageContents, packageName);
			/*
			if (debug)
			{
				global.DoChunk(lua.CompileChunk(packageContents, packageName, Lua.DefaultDebugEngine));
			}
			else
			{
				global.DoChunk(packageContents, packageName);
			}
			*/
		}

		private string FindFile(string packageName)
		{
			StringBuilder sb = new StringBuilder(256);
			string paths = LuaConfig.PackagePath;

			foreach (char c in paths)
			{
				if (c == '?')
				{
					sb.Append(packageName);
				}
				else if (c == ';')
				{
					string path = sb.ToString();

					if (File.Exists(path))
					{
						return path;
					}
					sb.Length = 0;
				}
				else
				{
					sb.Append(c);
				}
			}

			if (sb.Length > 0)
			{
				string path = sb.ToString();

				if (File.Exists(path))
				{
					return path;
				}
			}
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Failed to find '" + packageName + "' in '" + paths + "'");
		}

		public void ClearPackages()
		{
			packages.Clear();
		}

		public void Call(string functionName, params object[] args)
		{
			global.CallMember(functionName, args);
		}

		public object GetFunction(string functionName)
		{
			return global[functionName];
		}

		public void Register(string name, object value)
		{
			global[name] = value;
		}

		public static object BytesToLua(ParticleType type, byte[] buf, int offset, int len)
		{
			if (len <= 0)
			{
				return null;
			}

			switch (type)
			{
				case ParticleType.STRING:
					return ByteUtil.Utf8ToString(buf, offset, len);

				case ParticleType.INTEGER:
					return ByteUtil.BytesToNumber(buf, offset, len);

				case ParticleType.BOOL:
					return ByteUtil.BytesToBool(buf, offset, len);

				case ParticleType.DOUBLE:
					return ByteUtil.BytesToDouble(buf, offset);

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

				case ParticleType.GEOJSON:
					{
						// skip the flags
						int ncells = ByteUtil.BytesToShort(buf, offset + 1);
						int hdrsz = 1 + 2 + (ncells * 8);
						return new LuaGeoJSON(ByteUtil.Utf8ToString(buf, offset + hdrsz, len - hdrsz));
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
