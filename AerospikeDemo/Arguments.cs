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
using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	/// <summary>
	/// Configuration data.
	/// </summary>
	public class Arguments
	{
		internal string host;
		internal int port;
		internal string user;
		internal string password;
		internal string ns;
		internal string set;
        internal string binName;
        internal WritePolicy writePolicy;
		internal Policy policy;
        internal int commandMax;
        internal bool singleBin;
		internal bool hasUdf;

		protected internal Arguments()
		{
			this.writePolicy = new WritePolicy();
			this.policy = new Policy();
		}

		/// <summary>
		/// Some database calls need to know how the server is configured.
		/// </summary>
		protected internal void SetServerSpecific(AerospikeClient client)
		{
			Node node = client.Nodes[0];
			string featuresFilter = "features";
			string namespaceFilter = "namespace/" + ns;
			Dictionary<string, string> tokens = Info.Request(null, node, featuresFilter, namespaceFilter);

			string features = tokens[featuresFilter];
			hasUdf = false;

			if (features != null)
			{
				string[] list = features.Split(';');

				foreach (string s in list)
				{
					if (s.Equals("udf"))
					{
						hasUdf = true;
						break;
					}
				}
			}

			string namespaceTokens = tokens[namespaceFilter];

			if (namespaceTokens == null)
			{
				throw new Exception(string.Format("Failed to get namespace info: host={0} namespace={1}", node, ns));
			}

			string name = "single-bin";
			string search = name + '=';
			int begin = namespaceTokens.IndexOf(search);

			if (begin < 0)
			{
				throw new Exception(string.Format("Failed to find namespace attribute: host={0} namespace={1} attribute={2}", node, ns, name));
			}

			begin += search.Length;
			int end = namespaceTokens.IndexOf(';', begin);

			if (end < 0)
			{
				end = namespaceTokens.Length;
			}

			string value = namespaceTokens.Substring(begin, end - begin);
			singleBin = Convert.ToBoolean(value);

            binName = singleBin ? "" : "demobin";  // Single bin servers don't need a bin name.
		}

		public override string ToString()
		{
			return "Parameters: host=" + host + " port=" + port + " ns=" + ns + " set=" + set + " single-bin=" + singleBin;
		}

		public virtual string GetBinName(string name)
		{
			// Single bin servers don't need a bin name.
			return singleBin ? "" : name;
		}
	}
}
