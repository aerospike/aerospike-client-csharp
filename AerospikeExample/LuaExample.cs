/* 
 * Copyright 2012-2026 Aerospike, Inc.
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
using Aerospike.Client;

namespace Aerospike.Example;

public class LuaExample
{
	private static readonly string LuaDirectory = FindLuaDirectory();

	static LuaExample()
	{
		LuaConfig.PackagePath = LuaDirectory + "?.lua";
	}

	public static void Register(IAerospikeClient client, Policy policy, string packageName)
	{
		string path = LuaDirectory + packageName;
		RegisterTask task = client.Register(policy, path, packageName, Language.LUA);
		task.Wait();
	}

	private static string FindLuaDirectory()
	{
		string sep = Path.DirectorySeparatorChar.ToString();

		// Try to find the udf directory relative to the current executable.
		string baseDir = AppContext.BaseDirectory;
		string candidate = Path.Combine(baseDir, "udf" + sep);

		if (Directory.Exists(candidate))
		{
			return candidate;
		}

		// Walk up from executable location looking for the udf directory.
		string path = baseDir;

		for (int i = 0; i < 8; i++)
		{
			path = Path.GetDirectoryName(path);
			if (path == null) break;

			candidate = Path.Combine(path, "udf" + sep);
			if (Directory.Exists(candidate))
			{
				return candidate;
			}
		}

		// Default fallback.
		return Path.Combine(baseDir, "udf" + sep);
	}
}
