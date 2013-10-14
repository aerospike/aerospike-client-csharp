/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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

		public override string ToString()
		{
			return name + ':' + port;
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = prime + name.GetHashCode();
			return prime * result + port;
		}

		public override bool Equals(object obj)
		{
			Host other = (Host) obj;
			return this.name.Equals(other.name) && this.port == other.port;
		}
	}
}