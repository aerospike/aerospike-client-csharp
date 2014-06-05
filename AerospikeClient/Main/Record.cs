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
using System.Collections.Generic;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Container object for records.  Records are equivalent to rows.
	/// </summary>
	public sealed class Record
	{
		/// <summary>
		/// Map of requested name/value bins.
		/// </summary>
		public readonly Dictionary<string,object> bins;

		/// <summary>
		/// Record modification count.
		/// </summary>
		public readonly int generation;

		/// <summary>
		/// Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
		/// </summary>
		public readonly int expiration;

		/// <summary>
		/// Initialize record.
		/// </summary>
		public Record(Dictionary<string,object> bins, int generation, int expiration)
		{
			this.bins = bins;
			this.generation = generation;
			this.expiration = expiration;
		}

		/// <summary>
		/// Get bin value given bin name.
		/// Enter empty string ("") for servers configured as single-bin.
		/// </summary>
		public object GetValue(string name)
		{
			if (bins == null)
			{
				return null;
			}

			object obj;
			bins.TryGetValue(name, out obj);
			return obj;
		}

		/// <summary>
		/// Return string representation of record.
		/// </summary>
		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(500);
			sb.Append("(gen:");
			sb.Append(generation);
			sb.Append("),(exp:");
			sb.Append(expiration);
			sb.Append("),(bins:");

			if (bins != null)
			{
				bool sep = false;

				foreach (KeyValuePair<string, object> entry in bins)
				{
					if (sep)
					{
						sb.Append(',');
					}
					else
					{
						sep = true;
					}
					sb.Append('(');
					sb.Append(entry.Key);
					sb.Append(':');
					sb.Append(entry.Value);
					sb.Append(')');

					if (sb.Length > 1000)
					{
						sb.Append("...");
						break;
					}
				}
			}
			else
			{
				sb.Append("null");
			}
			sb.Append(')');
			return sb.ToString();
		}
	}
}
