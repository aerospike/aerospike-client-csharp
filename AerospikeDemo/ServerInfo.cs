using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class ServerInfo : SyncExample
	{
		public ServerInfo(Console console) : base(console)
		{
		}

		/// <summary>
		/// Query server configuration, cluster status and namespace configuration.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			GetServerConfig(args);
			console.Write("");
			GetNamespaceConfig(args);
		}

		/// <summary>
		/// Query server configuration and cluster status.
		/// </summary>
		private void GetServerConfig(Arguments args)
		{
			console.Write("Server Configuration");
			Dictionary<string, string> map = Info.Request(args.host, args.port);

			if (map == null)
			{
				throw new Exception(string.Format("Failed to get server info: host={0} port={1:D}", 
					args.host, args.port));
			}

			foreach (KeyValuePair<string, string> entry in map)
			{
				string key = entry.Key;

				if (key.Equals("statistics") || key.Equals("query-stat"))
				{
					LogNameValueTokens(entry.Value);
				}
				else
				{
					console.Write(key + '=' + entry.Value);
				}
			}
		}

		/// <summary>
		/// Query namespace configuration.
		/// </summary>
		private void GetNamespaceConfig(Arguments args)
		{
			console.Write("Namespace Configuration");
			string filter = "namespace/" + args.ns;
			string tokens = Info.Request(args.host, args.port, filter);

			if (tokens == null)
			{
				throw new Exception(string.Format("Failed to get namespace info: host={0} port={1:D} namespace={2}", 
					args.host, args.port, args.ns));
			}

			LogNameValueTokens(tokens);
		}

		private void LogNameValueTokens(string tokens)
		{
			string[] values = tokens.Split(';');

			foreach (string value in values)
			{
				console.Write(value);
			}
		}
	}
}