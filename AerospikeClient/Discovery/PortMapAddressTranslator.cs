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
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Address translator that replaces both the advertised host and port with the address mapped to
	/// the advertised endpoint or to the node that advertised it. Translator name <c>port-map</c>,
	/// YAML key <c>translation.port_map</c>.
	/// <para>
	/// Map grammar:
	/// <list type="bullet">
	/// <item>Key: an advertised <c>host:port</c>, or a node name. A key that contains <c>':'</c> is a
	/// <c>host:port</c>; any other key is a node name.</item>
	/// <item>Value: the <c>host:port</c> to connect to. The port is 1 to 65535.</item>
	/// <item>An IPv6 host is written in brackets, in keys and values: <c>[2001:db8::1]:3000</c>.</item>
	/// </list>
	/// </para>
	/// <para>
	/// Precedence: the advertised <c>host:port</c> is looked up first and the node name second.
	/// The advertised TLS name is kept (FR-40). The map is parsed and copied at construction.
	/// </para>
	/// </summary>
	public sealed class PortMapAddressTranslator : IAddressTranslator
	{
		private readonly Dictionary<string, Host> endpointMap;
		private readonly Dictionary<string, Host> nodeMap;

		/// <summary>
		/// Initialize translator with a map of advertised <c>host:port</c> or node name to the
		/// <c>host:port</c> to connect to.
		/// </summary>
		/// <exception cref="AerospikeException">if the map is null or an entry is invalid</exception>
		public PortMapAddressTranslator(Dictionary<string, string> portMap)
		{
			if (portMap == null)
			{
				throw new AerospikeException("Invalid port-map: map is null");
			}

			Dictionary<string, Host> endpoints = new Dictionary<string, Host>();
			Dictionary<string, Host> nodes = new Dictionary<string, Host>();

			foreach (KeyValuePair<string, string> entry in portMap)
			{
				string key = entry.Key;
				string value = entry.Value;

				if (key == null || key.Length == 0)
				{
					throw Invalid(key, value, "key is empty");
				}

				if (value == null)
				{
					throw Invalid(key, value, "value is null");
				}

				Host target = Parse(key, value, value);

				if (key.IndexOf(':') >= 0)
				{
					Host advertised = Parse(key, value, key);
					string mapKey = Key(advertised.name, advertised.port);

					if (endpoints.ContainsKey(mapKey))
					{
						throw Invalid(key, value, "duplicates another key");
					}

					endpoints[mapKey] = target;
				}
				else
				{
					nodes[key] = target;
				}
			}

			this.endpointMap = endpoints;
			this.nodeMap = nodes;
		}

		/// <summary>
		/// Return the host and port mapped to the advertised <c>host:port</c>, else to the advertised
		/// node name, with the advertised TLS name. Return null when neither is mapped.
		/// </summary>
		public Host Translate(Endpoint advertised)
		{
			Host target;

			if (!endpointMap.TryGetValue(Key(advertised.host, advertised.port), out target))
			{
				if (advertised.nodeName == null)
				{
					return null;
				}

				if (!nodeMap.TryGetValue(advertised.nodeName, out target))
				{
					return null;
				}
			}

			return new Host(target.name, advertised.tlsName, target.port);
		}

		private static string Key(string host, int port)
		{
			return (host.IndexOf(':') >= 0) ? "[" + host + "]:" + port : host + ':' + port;
		}

		private static Host Parse(string key, string value, string address)
		{
			int index = address.LastIndexOf(':');

			if (index < 0)
			{
				throw Invalid(key, value, address + " is not host:port");
			}

			string host = address.Substring(0, index);

			if (host.StartsWith("["))
			{
				if (!host.EndsWith("]") || host.Length < 3)
				{
					throw Invalid(key, value, address + " has an invalid bracketed IPv6 host");
				}

				host = host.Substring(1, host.Length - 2);
			}
			else if (host.IndexOf(':') >= 0)
			{
				throw Invalid(key, value, address + " has an IPv6 host that is not in brackets");
			}

			if (host.Length == 0)
			{
				throw Invalid(key, value, address + " has an empty host");
			}

			string portString = address.Substring(index + 1);
			int port;

			if (!int.TryParse(portString, out port))
			{
				throw Invalid(key, value, address + " has an invalid port " + portString);
			}

			if (port < 1 || port > 65535)
			{
				throw Invalid(key, value, address + " has port " + port + " out of range 1-65535");
			}

			return new Host(host, port);
		}

		private static AerospikeException Invalid(string key, string value, string reason)
		{
			return new AerospikeException("Invalid port-map entry " + key + ": " + value + ": " + reason);
		}
	}
}
