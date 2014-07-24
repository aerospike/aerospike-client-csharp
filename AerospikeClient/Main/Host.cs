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
