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
using Aerospike.Client;

namespace Aerospike.Example;

/// <summary>
/// Demonstrate connection patterns used in documentation snippets.
/// Each method exercises a progressively more complex connection scenario.
/// </summary>
public class Connect(Console console) : SyncExample(console)
{
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		RunBasicConnect(args);
		RunAuthConnect(args);
		RunTlsConnect(args);
		RunTlsPkiConnect(args);
		console.Info("Connect completed successfully.");
	}

	private void RunBasicConnect(Arguments args)
	{
		RequireBasic(args);
		string host = args.hosts[0].name;
		int port = args.port;

		// @@@SNIPSTART csharp-client-connect-basic
		AerospikeClient client = new(host, port);
		// @@@SNIPEND

		console.Info("Basic connect: host={0} port={1}", host, port);

		// @@@SNIPSTART csharp-client-connect-close
		client.Close();
		// @@@SNIPEND
	}

	private void RunAuthConnect(Arguments args)
	{
		RequireAuth(args);
		string host = args.hosts[0].name;
		int port = args.port;
		string user = args.user;
		string password = args.password;

		// @@@SNIPSTART csharp-client-connect-auth
		ClientPolicy policy = new()
		{
			user = user,
			password = password
		};

		AerospikeClient client = new(policy, host, port);
		// @@@SNIPEND

		console.Info("Auth connect: host={0} port={1} user={2}", host, port, user);
		client.Close();
	}

	private void RunTlsConnect(Arguments args)
	{
		RequireTls(args);
		string host = args.hosts[0].name;
		string tlsName = args.hosts[0].tlsName;
		int port = args.port;

		// @@@SNIPSTART csharp-client-connect-tls
		Host tlsHost = new(host, tlsName, port);

		TlsPolicy tlsPolicy = new();

		ClientPolicy policy = new()
		{
			tlsPolicy = tlsPolicy
		};

		AerospikeClient client = new(policy, tlsHost);
		// @@@SNIPEND

		console.Info("TLS connect: host={0} tlsName={1} port={2}", host, tlsName, port);
		client.Close();
	}

	private void RunTlsPkiConnect(Arguments args)
	{
		RequireTls(args);
		RequirePki(args);
		string host = args.hosts[0].name;
		string tlsName = args.hosts[0].tlsName;
		int port = args.port;

		// @@@SNIPSTART csharp-client-connect-tls-pki
		Host tlsHost = new(host, tlsName, port);

		TlsPolicy tlsPolicy = new();

		ClientPolicy policy = new()
		{
			tlsPolicy = tlsPolicy,
			authMode = AuthMode.PKI
		};

		AerospikeClient client = new(policy, tlsHost);
		// @@@SNIPEND

		console.Info("TLS+PKI connect: host={0} tlsName={1} port={2}", host, tlsName, port);
		client.Close();
	}
}
