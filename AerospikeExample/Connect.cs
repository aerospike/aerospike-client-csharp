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
/// Demonstrate connection patterns referenced from documentation snippets.
/// Each method exercises a progressively more complex connection scenario.
/// </summary>
public sealed class Connect : SyncExample
{
	public override void RunExample()
	{
		RunBasicConnect();
		RunAuthConnect();
		RunTlsConnect();
		RunTlsPkiConnect();
		console.Info("Connect completed successfully.");
	}

	private void RunBasicConnect()
	{
		RequireBasic();
		string host = args.hosts[0].name;
		int port = args.port;

		// @@@SNIPSTART csharp-client-connect-basic
		AerospikeClient client = new(host, port);
		// @@@SNIPEND

		console.Info($"Basic connect: host={host} port={port}");

		// @@@SNIPSTART csharp-client-connect-close
		client.Close();
		// @@@SNIPEND
	}

	private void RunAuthConnect()
	{
		RequireAuth();
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

		console.Info($"Auth connect: host={host} port={port} user={user}");
		client.Close();
	}

	private void RunTlsConnect()
	{
		RequireTls();
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

		console.Info($"TLS connect: host={host} tlsName={tlsName} port={port}");
		client.Close();
	}

	private void RunTlsPkiConnect()
	{
		RequireTls();
		RequirePki();
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

		console.Info($"TLS+PKI connect: host={host} tlsName={tlsName} port={port}");
		client.Close();
	}
}
