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
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Pre-defined user roles.
	/// </summary>
	public sealed class Role
	{
		/// <summary>
		/// Manage users their roles.
		/// </summary>
		public const string UserAdmin = "user-admin";

		/// <summary>
		/// Manage indicies, user defined functions and server configuration. 
		/// </summary>
		public const string SysAdmin = "sys-admin";

		/// <summary>
		/// Allow read and write transactions with the database.
		/// </summary>
		public const string ReadWrite = "read-write";

		/// <summary>
		/// Allow read transactions with the database.
		/// </summary>
		public const string Read = "Read";
	}
}
