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
		internal string ns;
		internal string set;
		internal WritePolicy writePolicy;
		internal Policy policy;
		internal bool singleBin;
		internal bool hasUdf;
		internal bool debug = false;

		internal int threadMax;
		internal int commandMax;

		protected internal Arguments(string host, int port, string ns, string set)
		{
			this.host = host;
			this.port = port;
			this.ns = ns;
			this.set = set;
			this.writePolicy = new WritePolicy();
			this.policy = new Policy();
		}

		/// <summary>
		/// Some database calls need to know how the server is configured.
		/// </summary>
		protected internal virtual void SetServerSpecific()
		{
			string featuresFilter = "features";
			string namespaceFilter = "namespace/" + ns;
			Dictionary<string, string> tokens = Info.Request(host, port, featuresFilter, namespaceFilter);

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
				throw new Exception(string.Format("Failed to get namespace info: host={0} port={1:D} namespace={2}", host, port, ns));
			}

			string name = "single-bin";
			string search = name + '=';
			int begin = namespaceTokens.IndexOf(search);

			if (begin < 0)
			{
				throw new Exception(string.Format("Failed to find namespace attribute: host={0} port={1:D} namespace={2} attribute={3}", host, port, ns, name));
			}

			begin += search.Length;
			int end = namespaceTokens.IndexOf(';', begin);

			if (end < 0)
			{
				end = namespaceTokens.Length;
			}

			string value = namespaceTokens.Substring(begin, end - begin);
			singleBin = Convert.ToBoolean(value);
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