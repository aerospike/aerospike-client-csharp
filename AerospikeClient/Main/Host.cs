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
using System.Text;

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
		/// Convert host name and port to string.
		/// </summary>
		public override string ToString()
		{
			return name + ':' + port;
		}

		/// <summary>
		/// Return host address hash code.
		/// </summary>
		public override int GetHashCode()
		{
			const int prime = 31;
			int result = prime + name.GetHashCode();
			return prime * result + port;
		}

		/// <summary>
		/// Return if hosts are equal.
		/// </summary>
		public override bool Equals(object obj)
		{
			Host other = (Host) obj;
			return this.name.Equals(other.name) && this.port == other.port;
		}
	}
}
