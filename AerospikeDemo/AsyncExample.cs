using System;
using System.Collections.Generic;
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public abstract class AsyncExample : Example
	{
		public AsyncExample(Console console)
			: base(console)
		{
		}

		public override void RunExample(Arguments args)
		{
			AsyncClientPolicy policy = new AsyncClientPolicy();
			policy.asyncMaxCommands = args.commandMax;

			AsyncClient client = new AsyncClient(policy, args.host, args.port);

			try
			{
				RunExample(client, args);
			}
			finally
			{
				client.Close();
			}
		}

		public abstract void RunExample(AsyncClient client, Arguments args);
	}
}