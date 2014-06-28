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

		public NodeValidator(Cluster cluster, Host host)
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
					Connection conn = new Connection(address, cluster.connectionTimeout);
    
					try
					{
						if (cluster.user.Length > 0)
						{
							AdminCommand command = new AdminCommand();
							command.Authenticate(conn, cluster.user, cluster.password);
						}
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
