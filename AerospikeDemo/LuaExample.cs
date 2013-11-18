using System;
using System.IO;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class LuaExample
	{
		private const string LuaDirectory = @"..\..\..\udf";

		static LuaExample()
		{
 			LuaConfig.PackagePath = LuaDirectory + @"\?.lua";
		}

		public static void Register(AerospikeClient client, Policy policy, string packageName)
		{
			string path = LuaDirectory + Path.DirectorySeparatorChar + packageName;
			RegisterTask task = client.Register(policy, path, packageName, Language.LUA);
			task.Wait();
		}
	}
}
