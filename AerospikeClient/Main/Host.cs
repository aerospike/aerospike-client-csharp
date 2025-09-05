/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
namespace Aerospike.Client
{
	/// <summary>
	/// Host name/port of database server. 
	/// </summary>
	public sealed class Host
	{
		/// <summary>
		/// Host name or IP address of database server.
		/// </summary>
		public readonly string name;

		/// <summary>
		/// TLS certificate name used for secure connections.
		/// The certificate must exist in your Trusted Root Certification repository.
		/// See: <see href="https://technet.microsoft.com/en-us/library/cc754841%28v=ws.11%29.aspx#BKMK_adddomain">Trusted Root Certification</see>
		/// </summary>
		public readonly string tlsName;

		/// <summary>
		/// Port of database server.
		/// </summary>
		public readonly int port;

		/// <summary>
		/// Initialize host.
		/// </summary>
		public Host(string name, int port)
		{
			this.name = name;
			this.port = port;
		}

		/// <summary>
		/// Initialize host.
		/// </summary>
		public Host(string name, string tlsName, int port)
		{
			this.name = name;
			this.tlsName = tlsName;
			this.port = port;
		}

		/// <summary>
		/// Convert host name and port to string.
		/// </summary>
		public override string ToString()
		{
			// Ignore tlsName in string representation.
			// Use space separator to avoid confusion with IPv6 addresses that contain colons.
			return name + ' ' + port;
		}

		/// <summary>
		/// Return host address hash code.
		/// </summary>
		public override int GetHashCode()
		{
			// Ignore tlsName in default hash code.
			const int prime = 31;
			int result = prime + name.GetHashCode();
			return prime * result + port;
		}

		/// <summary>
		/// Return if hosts are equal.
		/// </summary>
		public override bool Equals(object obj)
		{
			// Ignore tlsName in default equality comparison.
			Host other = (Host)obj;
			return this.name.Equals(other.name) && this.port == other.port;
		}

		/// <summary>
		/// Parse hosts from string format: hostname1[:tlsname1][:port1],...
		/// <para>
		/// Hostname may also be an IP address in the following formats.
		/// </para>
		/// <ul>
		/// <li>IPv4: xxx.xxx.xxx.xxx</li>
		/// <li>IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]</li>
		/// <li>IPv6: [xxxx::xxxx]</li>
		/// </ul>
		/// <para>
		/// IPv6 addresses must be enclosed by brackets.
		/// tlsname and port are optional.
		/// </para>
		/// </summary>
		public static Host[] ParseHosts(string str, string defaultTlsName, int defaultPort)
		{
			try
			{
				return new HostParser(str).ParseHosts(defaultTlsName, defaultPort);
			}
			catch (Exception e)
			{
				throw new AerospikeException("Invalid hosts string: " + str, e);
			}
		}

		/// <summary>
		/// Parse server service hosts from string format: hostname1:port1,...
		/// <para>
		/// Hostname may also be an IP address in the following formats.
		/// <ul>
		/// <li>IPv4: xxx.xxx.xxx.xxx</li>
		/// <li>IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]</li>
		/// <li>IPv6: [xxxx::xxxx]</li>
		/// </ul>
		/// IPv6 addresses must be enclosed by brackets.
		/// </para>
		/// </summary>
		public static List<Host> ParseServiceHosts(string str)
		{
			try
			{
				return new HostParser(str).ParseServiceHosts();
			}
			catch (Exception e)
			{
				throw new AerospikeException("Invalid service hosts string: " + str, e);
			}
		}

		internal class HostParser
		{
			private readonly string str;
			private int offset;
			private int length;
			private char c;

			internal HostParser(string str)
			{
				this.str = str;
				this.length = str.Length;
				this.offset = 0;
				this.c = ',';
			}

			internal Host[] ParseHosts(string defaultTlsName, int defaultPort)
			{
				List<Host> list = new List<Host>();
				string hostname;
				string tlsname;
				int port;

				while (offset < length)
				{
					if (c != ',')
					{
						throw new Exception();
					}
					hostname = ParseHost();
					tlsname = defaultTlsName;
					port = defaultPort;

					if (offset < length && c == ':')
					{
						string s = ParseString();

						if (s.Length > 0)
						{
							if (char.IsDigit(s[0]))
							{
								// Found port.
								port = Convert.ToInt32(s);
							}
							else
							{
								// Found tls name.
								tlsname = s;

								// Parse port.
								s = ParseString();

								if (s.Length > 0)
								{
									port = Convert.ToInt32(s);
								}
							}
						}
					}
					list.Add(new Host(hostname, tlsname, port));
				}
				return list.ToArray();
			}

			internal List<Host> ParseServiceHosts()
			{
				List<Host> list = new List<Host>();
				String hostname;
				int port;

				while (offset < length)
				{
					if (c != ',')
					{
						throw new Exception();
					}
					hostname = ParseHost();

					if (c != ':')
					{
						throw new Exception();
					}

					String s = ParseString();
					port = Convert.ToInt32(s);

					list.Add(new Host(hostname, port));
				}
				return list;
			}

			private string ParseHost()
			{
				c = str[offset];

				if (c == '[')
				{
					// IPv6 addresses are enclosed by brackets.
					int begin = ++offset;

					while (offset < length)
					{
						c = str[offset];

						if (c == ']')
						{
							string s = str.Substring(begin, offset - begin);

							if (++offset < length)
							{
								c = str[offset++];
							}
							return s;
						}
						offset++;
					}
					throw new Exception("Unterminated bracket");
				}
				else
				{
					return ParseString();
				}
			}

			private string ParseString()
			{
				int begin = offset;

				while (offset < length)
				{
					c = str[offset];

					if (c == ':' || c == ',')
					{
						string s = str.Substring(begin, offset - begin);
						offset++;
						return s;
					}
					offset++;
				}
				return str.Substring(begin, offset - begin);
			}
		}
	}
}
