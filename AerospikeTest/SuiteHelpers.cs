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

internal static class SuiteHelpers
{
	public static IAerospikeClient client;
	public static IAsyncClient asyncClient;
	public static Host[] hosts;
	public static int port;
	public static string user;
	public static string password;
	public static int timeout;
	public static string clusterName;
	public static string ns;
	public static string set;
	public static bool useServicesAlternate;
	public static string tlsName;
	public static TlsPolicy tlsPolicy;
	public static AuthMode authMode;
	public static bool enterprise;
	public static bool hasTtl;
	public static bool scMode;
	/// <summary>
	/// True when QueryRoles succeeds and both <c>sys-admin</c> and <c>user-admin</c>
	/// are present. Aerospike Cloud omits those roles (and often returns ROLE_VIOLATION
	/// for admin APIs), so tests that create users/roles or assert those predefined
	/// roles should skip when this is false.
	/// </summary>
	public static bool fullSecurityAdmin;
	public static Version serverVersion;
}