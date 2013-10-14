/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System;
using System.Net;
using System.Text;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class NodeValidator
	{
		private const int DEFAULT_TIMEOUT = 2000;

		internal string name;
		internal Host[] aliases;
		internal IPEndPoint address;
		internal bool useNewInfo = true;

		public NodeValidator(Host host, int timeoutMillis)
		{
			IPAddress[] addresses = Connection.GetHostAddresses(host.name, DEFAULT_TIMEOUT);
			int count = 0;
			aliases = new Host[addresses.Length];

			foreach (IPAddress address in addresses)
			{
				aliases[count++] = new Host(address.ToString(), host.port);
			}

			foreach (IPAddress alias in addresses)
			{
				try
				{
					IPEndPoint address = new IPEndPoint(alias, host.port);
					Connection conn = new Connection(address, timeoutMillis);
    
					try
					{
						Dictionary<string, string> map = Info.Request(conn, "node", "build");
						string nodeName;

						if (map.TryGetValue("node", out nodeName))
						{
							this.name = nodeName;
							this.address = address;

							// Check new info protocol support for >= 2.6.6 build
							string buildVersion;

							if (map.TryGetValue("build", out buildVersion))
							{
								try
								{
									string[] vNumber = buildVersion.Split('.');
									int v1 = Convert.ToInt32(vNumber[0]);
									int v2 = Convert.ToInt32(vNumber[1]);
									int v3 = Convert.ToInt32(vNumber[2]);

									this.useNewInfo = v1 > 2 || (v1 == 2 && (v2 > 6 || (v2 == 6 && v3 >= 6)));
								}
								catch (Exception)
								{
									// Unexpected exception. Use default info protocol.
								}
							}
							return;
						}
					}
					finally
					{
						conn.Close();
					}
				}
				catch (Exception e)
				{
					// Try next address.
					if (Log.DebugEnabled())
					{
						Log.Debug("Alias " + alias + " failed: " + Util.GetErrorMessage(e));
					}
				}
			}
			throw new AerospikeException.Connection("Failed to connect to host aliases: " + Util.ArrayToString(addresses));
		}
	}
}