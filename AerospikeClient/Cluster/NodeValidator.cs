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
			aliases = new Host[addresses.Length];

			for (int i = 0; i < addresses.Length; i++)
			{
				aliases[i] = new Host(addresses[i].ToString(), host.port);
			}

			Exception exception = null;

			for (int i = 0; i < addresses.Length; i++)
			{			
				try
				{
					IPEndPoint address = new IPEndPoint(addresses[i], host.port);
					Connection conn = new Connection(address, cluster.connectionTimeout);

					try
					{
						if (cluster.user != null)
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
						Log.Debug("Alias " + addresses[i] + " failed: " + Util.GetErrorMessage(e));
					}

					if (exception == null)
					{
						exception = e;
					}
				}
			}

			if (exception == null)
			{
				throw new AerospikeException.Connection("Failed to find addresses for " + host);
			}
			throw exception;
		}
	}
}
