using System;
using System.Collections.Generic;
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public abstract class SyncExample : Example
	{
		public SyncExample(Console console)
			: base(console)
		{
		}

		public override void RunExample(Arguments args)
		{
			ClientPolicy policy = new ClientPolicy();
			AerospikeClient client = new AerospikeClient(policy, args.host, args.port);

			try
			{
				RunExample(client, args);
			}
			finally
			{
				client.Close();
			}
		}

		public abstract void RunExample(AerospikeClient client, Arguments args);
	}
}