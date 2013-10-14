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
			client.Register(policy, path, packageName, Language.LUA);

			// The server UDF distribution to other nodes is done asynchronously.  Therefore, the server
			// may return before the UDF is available on all nodes.  Hard code sleep for now.
			// TODO: Fix server so control is only returned when UDF registration is complete.
			Util.Sleep(1000);
		}
	}
}
