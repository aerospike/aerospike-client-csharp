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
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml;

namespace Aerospike.Client
{
	/// <summary>
	/// Parser for EC2 and STS XML responses, with DTDs and external entities disabled. Used by one
	/// provider instance on one thread at a time.
	/// </summary>
	internal sealed class AwsXml
	{
		private readonly XmlReaderSettings settings;

		internal AwsXml()
		{
			settings = new XmlReaderSettings
			{
				DtdProcessing = DtdProcessing.Prohibit,
				XmlResolver = null,
				IgnoreComments = true,
				IgnoreWhitespace = false
			};
		}

		/// <summary>
		/// Parse a document and return its root element.
		/// </summary>
		internal XmlElement Parse(byte[] body)
		{
			XmlDocument document = new XmlDocument
			{
				XmlResolver = null
			};

			using (MemoryStream stream = new MemoryStream(body))
			using (XmlReader reader = XmlReader.Create(stream, settings))
			{
				document.Load(reader);
			}

			return document.DocumentElement;
		}

		/// <summary>
		/// Return the direct child elements with the given name.
		/// </summary>
		internal static List<XmlElement> Children(XmlElement parent, string name)
		{
			List<XmlElement> list = new List<XmlElement>();

			if (parent == null)
			{
				return list;
			}

			foreach (XmlNode node in parent.ChildNodes)
			{
				if (node.NodeType == XmlNodeType.Element && name.Equals(node.LocalName))
				{
					list.Add((XmlElement)node);
				}
			}

			return list;
		}

		/// <summary>
		/// Return the first direct child element with the given name, or null.
		/// </summary>
		internal static XmlElement Child(XmlElement parent, string name)
		{
			List<XmlElement> list = Children(parent, name);
			return list.Count == 0 ? null : list[0];
		}

		/// <summary>
		/// Return the trimmed text of the first direct child element with the given name, or null.
		/// </summary>
		internal static string Text(XmlElement parent, string name)
		{
			XmlElement e = Child(parent, name);
			return e == null ? null : e.InnerText.Trim();
		}

		/// <summary>
		/// Return the trimmed text of the first descendant element with the given name, or null.
		/// </summary>
		internal static string Find(XmlElement root, string name)
		{
			XmlNodeList list = root.GetElementsByTagName(name);
			return list.Count == 0 ? null : list[0].InnerText.Trim();
		}

		/// <summary>
		/// Return an exception for an AWS Query API error response.
		/// </summary>
		internal AerospikeException Error(string action, AwsHttp.Response response, params string[] secrets)
		{
			string code = null;
			string message = null;
			string requestId = null;

			try
			{
				XmlElement root = Parse(response.Body);
				code = Find(root, "Code");
				message = Find(root, "Message");
				requestId = Find(root, "RequestID");

				if (requestId == null)
				{
					requestId = Find(root, "RequestId");
				}
			}
			catch (Exception)
			{
				// Keep status-only message when the body is not XML.
			}

			if (requestId == null)
			{
				response.Headers.TryGetValue("x-amzn-requestid", out requestId);
			}

			StringBuilder sb = new StringBuilder(128);
			sb.Append("AWS EC2 seed provider: ").Append(action).Append(" failed: HTTP ").Append(response.Status);

			if (code != null)
			{
				sb.Append(' ').Append(code);
			}

			if (message != null)
			{
				sb.Append(": ").Append(message);
			}

			if (requestId != null)
			{
				sb.Append(" (request id ").Append(requestId).Append(')');
			}

			return new AerospikeException(Redact(sb.ToString(), secrets));
		}

		/// <summary>
		/// Replace each non-empty secret in the text.
		/// </summary>
		internal static string Redact(string text, params string[] secrets)
		{
			foreach (string secret in secrets)
			{
				if (!string.IsNullOrEmpty(secret))
				{
					text = text.Replace(secret, "<redacted>", StringComparison.Ordinal);
				}
			}

			return text;
		}
	}
}
