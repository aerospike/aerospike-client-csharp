/* 
 * Copyright 2012-2024 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestServerInfo : TestSync
	{
		[TestMethod]
		public void ServerInfo()
		{
			Node node = client.Nodes[0];
			GetServerConfig(node);
			GetNamespaceConfig(node);
		}

		[TestMethod]
		public void ErrorResponse()
		{
			Info.Error error;

			error = new Info.Error("FaIL:201:index not found");
			Assert.AreEqual(error.Code, 201);
			Assert.AreEqual(error.Message, "index not found");

			error = new Info.Error("ERRor:201:index not found");
			Assert.AreEqual(error.Code, 201);
			Assert.AreEqual(error.Message, "index not found");

			error = new Info.Error("error::index not found ");
			Assert.AreEqual(error.Code, ResultCode.CLIENT_ERROR);
			Assert.AreEqual(error.Message, "index not found");

			error = new Info.Error("error: index not found ");
			Assert.AreEqual(error.Code, ResultCode.CLIENT_ERROR);
			Assert.AreEqual(error.Message, "index not found");

			error = new Info.Error("error:99");
			Assert.AreEqual(error.Code, 99);
			Assert.AreEqual(error.Message, "error:99");

			error = new Info.Error("generic message");
			Assert.AreEqual(error.Code, ResultCode.CLIENT_ERROR);
			Assert.AreEqual(error.Message, "generic message");
		}

		private void GetServerConfig(Node node)
		{
			IDictionary<string, string> map = Info.Request(null, node);
			Assert.IsNotNull(map);

			foreach (KeyValuePair<string, string> entry in map)
			{
				string key = entry.Key;

				if (key.Equals("statistics") || key.Equals("query-stat"))
				{
					LogNameValueTokens(entry.Value);
				}
				else
				{
					if (!(key.Equals("services-alumni") || key.Equals("services") || key.Equals("dcs") || key.Equals("build_ee_sha")))
					{
						Assert.IsNotNull(entry.Value);
					}
				}
			}
		}

		private void GetNamespaceConfig(Node node)
		{
			string filter = "namespace/" + args.ns;
			string tokens = Info.Request(null, node, filter);
			Assert.IsNotNull(tokens);
			LogNameValueTokens(tokens);
		}

		private void LogNameValueTokens(string tokens)
		{
			string[] values = tokens.Split(';');

			foreach (string value in values)
			{
				Assert.IsNotNull(value);
			}
		}
	}
}
