/* 
 * Copyright 2012-2025 Aerospike, Inc.
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
	public sealed class RegisterCommand
	{
		public static RegisterTask Register(Cluster cluster, Policy policy, string content, string serverPath, Language language)
		{
			StringBuilder sb = new StringBuilder(serverPath.Length + content.Length + 100);
			sb.Append("udf-put:filename=");
			sb.Append(serverPath);
			sb.Append(";content=");
			sb.Append(content);
			sb.Append(";content-len=");
			sb.Append(content.Length);
			sb.Append(";udf-type=");
			sb.Append(language);
			sb.Append(";");

			// Send UDF to one node. That node will distribute the UDF to other nodes.
			string command = sb.ToString();
			Node node = cluster.GetRandomNode();
			Connection conn = node.GetConnection(policy.socketTimeout);

			try
			{
				Info info = new(node, conn, command);
				Info.NameValueParser parser = info.GetNameValueParser();
				string error = null;
				string file = null;
				string line = null;
				string message = null;
				string messageNew = null;
				int errorCode = 0;

				while (parser.Next())
				{
					string name = parser.GetName();

					if (name.StartsWith("ERROR"))
					{
						// New error format: ERROR:<code>:<msg1>;file=<filename>;line=<line>;message=<base64 encoded msg2>
						int idx = name.IndexOf(';');
						string s = (idx > 0) ? name.Substring(0, idx) : name;
						Info.Error ie = new(s);
						messageNew = ie.Message;
						errorCode = ie.Code;
						file = parser.GetValue();
					}
					else if (name.Equals("error"))
					{
						// Old error format: error=<code>;file=<filename>;line=<line>;message=<base64 encoded msg>
						error = parser.GetValue();
					}
					else if (name.Equals("file"))
					{
						file = parser.GetValue();
					}
					else if (name.Equals("line"))
					{
						line = parser.GetValue();
					}
					else if (name.Equals("message"))
					{
						message = parser.GetStringBase64();
					}
				}

				if (errorCode != 0)
				{
					throw new AerospikeException(errorCode, "Registration failed: " + Environment.NewLine +
						"File: " + file + Environment.NewLine +
						"Line: " + line + Environment.NewLine +
						"Message: " + messageNew + ". " + message
						);
				}
				else if (error != null)
				{
					throw new AerospikeException("Registration failed: " + error + Environment.NewLine +
						"File: " + file + Environment.NewLine +
						"Line: " + line + Environment.NewLine +
						"Message: " + message
						);
				}
				node.PutConnection(conn);
				return new RegisterTask(cluster, policy, serverPath);
			}
			catch (Exception)
			{
				node.CloseConnectionOnError(conn);
				throw;
			}
		}
	}
}
