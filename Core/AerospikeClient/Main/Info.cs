/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Collections.Generic;

namespace Aerospike.Client
{
	/// <summary>
	/// Access server's info monitoring protocol.
	/// <para>
	/// The info protocol is a name/value pair based system, where an individual
	/// database server node is queried to determine its configuration and status.
	/// The list of supported names can be found at:
	/// </para>
	/// <para>
	/// <a href="https://docs.aerospike.com/display/AS2/Config+Parameters+Reference">https://docs.aerospike.com/display/AS2/Config+Parameters+Reference</a>
	/// </para>
	/// </summary>
	public sealed class Info
	{
		//-------------------------------------------------------
		// Static variables.
		//-------------------------------------------------------

		private const int DEFAULT_TIMEOUT = 1000;

		//-------------------------------------------------------
		// Member variables.
		//-------------------------------------------------------

		public byte[] buffer;
		public int length;
		public int offset;

		//-------------------------------------------------------
		// Constructor
		//-------------------------------------------------------

		/// <summary>
		/// Send single command to server and store results.
		/// This constructor is used internally.
		/// The static request methods should be used instead.
		/// </summary>
		/// <param name="conn">connection to server node</param>
		/// <param name="command">command sent to server</param>
		public Info(Connection conn, string command)
		{
			buffer = ThreadLocalData.GetBuffer();

			// If conservative estimate may be exceeded, get exact estimate
			// to preserve memory and resize buffer.
			if ((command.Length * 2 + 9) > buffer.Length)
			{
				offset = ByteUtil.EstimateSizeUtf8(command) + 9;
				ResizeBuffer(offset);
			}
			offset = 8; // Skip size field.

			// The command format is: <name1>\n<name2>\n...
			offset += ByteUtil.StringToUtf8(command, buffer, offset);
			buffer[offset++] = (byte)'\n';

			SendCommand(conn);
		}

		/// <summary>
		/// Send multiple commands to server and store results. 
		/// This constructor is used internally.
		/// The static request methods should be used instead.
		/// </summary>
		/// <param name="conn">connection to server node</param>
		/// <param name="commands">commands sent to server</param>
		public Info(Connection conn, params string[] commands)
		{
			buffer = ThreadLocalData.GetBuffer();

			// First, do quick conservative buffer size estimate.
			offset = 8;

			foreach (string command in commands)
			{
				offset += command.Length * 2 + 1;
			}

			// If conservative estimate may be exceeded, get exact estimate
			// to preserve memory and resize buffer.
			if (offset > buffer.Length)
			{
				offset = 8;

				foreach (string command in commands)
				{
					offset += ByteUtil.EstimateSizeUtf8(command) + 1;
				}
				ResizeBuffer(offset);
			}
			offset = 8; // Skip size field.

			// The command format is: <name1>\n<name2>\n...
			foreach (string command in commands)
			{
				offset += ByteUtil.StringToUtf8(command, buffer, offset);
				buffer[offset++] = (byte)'\n';
			}
			SendCommand(conn);
		}

		/// <summary>
		/// Send default empty command to server and store results. 
		/// This constructor is used internally.
		/// The static request methods should be used instead.
		/// </summary>
		/// <param name="conn">connection to server node</param>
		public Info(Connection conn)
		{
			buffer = ThreadLocalData.GetBuffer();
			offset = 8; // Skip size field.
			SendCommand(conn);
		}

		/// <summary>
		/// Parse response in name/value pair format:
		/// <para>
		/// &lt;command&gt;\t&lt;name1&gt;=&lt;value1&gt;;&lt;name2&gt;=&lt;value2&gt;;...\n
		/// </para>
		/// </summary>
		public NameValueParser GetNameValueParser()
		{
			SkipToValue();
			return new NameValueParser(this);
		}

		/// <summary>
		/// Return single value from response buffer.
		/// </summary>
		public string GetValue()
		{
			SkipToValue();
			return ByteUtil.Utf8ToString(buffer, offset, length - offset - 1);
		}

		public void SkipToValue()
		{
			// Skip past command.
			while (offset < length)
			{
				byte b = buffer[offset];

				if (b == '\t')
				{
					offset++;
					break;
				}

				if (b == '\n')
				{
					break;
				}
				offset++;
			}
		}

		/// <summary>
		/// Convert UTF8 numeric digits to an integer.  Negative integers are not supported.
		/// Input format: 1234
		/// </summary>
		public int ParseInt()
		{
			int begin = offset;
			int end = offset;
			byte b;

			// Skip to end of integer.
			while (offset < length)
			{
				b = buffer[offset];

				if (b < 48 || b > 57)
				{
					end = offset;
					break;
				}
				offset++;
			}

			// Convert digits into an integer.
			return (int)ByteUtil.Utf8DigitsToInt(buffer, begin, end);
		}

		public string ParseString(char stop)
		{
			int begin = offset;
			byte b;

			while (offset < length)
			{
				b = buffer[offset];

				if (b == stop)
				{
					break;
				}
				offset++;
			}
			return ByteUtil.Utf8ToString(buffer, begin, offset - begin);
		}

		public string ParseString(char stop1, char stop2, char stop3)
		{
			int begin = offset;
			byte b;

			while (offset < length)
			{
				b = buffer[offset];

				if (b == stop1 || b == stop2 || b == stop3)
				{
					break;
				}
				offset++;
			}
			return ByteUtil.Utf8ToString(buffer, begin, offset - begin);
		}

		public void Expect(char expected)
		{
			if (expected != buffer[offset])
			{
				throw new AerospikeException.Parse("Expected " + expected + " Received: " + (char)buffer[offset]);
			}
			offset++;
		}

		public string GetTruncatedResponse()
		{
			int max = (length > 200) ? 200 : length;
			return ByteUtil.Utf8ToString(buffer, 0, max);
		}

		//-------------------------------------------------------
		// Get Info via Node
		//-------------------------------------------------------

		/// <summary>
		/// Get one info value by name from the specified database server node.
		/// This method supports user authentication.
		/// </summary>
		/// <param name="node">server node</param>
		/// <param name="name">name of variable to retrieve</param>
		public static string Request(Node node, string name)
		{
			Connection conn = node.GetConnection(DEFAULT_TIMEOUT);

			try
			{
				string response = Info.Request(conn, name);
				node.PutConnection(conn);
				return response;
			}
			catch (Exception)
			{
				node.CloseConnection(conn);
				throw;
			}
		}
		
		/// <summary>
		/// Get one info value by name from the specified database server node.
		/// This method supports user authentication.
		/// </summary>
		/// <param name="policy">info command configuration parameters, pass in null for defaults</param>
		/// <param name="node">server node</param>
		/// <param name="name">name of variable to retrieve</param>
		public static string Request(InfoPolicy policy, Node node, string name)
		{
			int timeout = (policy == null) ? DEFAULT_TIMEOUT : policy.timeout;
			Connection conn = node.GetConnection(timeout);

			try
			{
				string result = Request(conn, name);
				node.PutConnection(conn);
				return result;
			}
			catch (Exception)
			{
				// Garbage may be in socket.  Do not put back into pool.
				node.CloseConnection(conn);
				throw;
			}
		}

		/// <summary>
		/// Get many info values by name from the specified database server node.
		/// This method supports user authentication.
		/// </summary>
		/// <param name="policy">info command configuration parameters, pass in null for defaults</param>
		/// <param name="node">server node</param>
		/// <param name="names">names of variables to retrieve</param>
		public static Dictionary<string, string> Request(InfoPolicy policy, Node node, params string[] names)
		{
			int timeout = (policy == null) ? DEFAULT_TIMEOUT : policy.timeout;
			Connection conn = node.GetConnection(timeout);

			try
			{
				Dictionary<string, string> result = Request(conn, names);
				node.PutConnection(conn);
				return result;
			}
			catch (Exception)
			{
				// Garbage may be in socket.  Do not put back into pool.
				node.CloseConnection(conn);
				throw;
			}
		}

		/// <summary>
		/// Get default info values from the specified database server node.
		/// This method supports user authentication.
		/// </summary>
		/// <param name="policy">info command configuration parameters, pass in null for defaults</param>
		/// <param name="node">server node</param>
		public static Dictionary<string, string> Request(InfoPolicy policy, Node node)
		{
			int timeout = (policy == null) ? DEFAULT_TIMEOUT : policy.timeout;
			Connection conn = node.GetConnection(timeout);

			try
			{
				Dictionary<string, string> result = Request(conn);
				node.PutConnection(conn);
				return result;
			}
			catch (Exception)
			{
				// Garbage may be in socket.  Do not put back into pool.
				node.CloseConnection(conn);
				throw;
			}
		}
		
		//-------------------------------------------------------
		// Get Info via Host Name and Port
		//-------------------------------------------------------

		/// <summary>
		/// Get one info value by name from the specified database server node, using
		/// host name and port.
		/// This method does not support user authentication.
		/// </summary>
		/// <param name="hostname">host name</param>
		/// <param name="port">host port</param>
		/// <param name="name">name of value to retrieve</param>
		public static string Request(string hostname, int port, string name)
		{
			IPAddress[] addresses = Connection.GetHostAddresses(hostname, DEFAULT_TIMEOUT);
			IPEndPoint ipe = new IPEndPoint(addresses[0], port);
			return Request(ipe, name);
		}

		/// <summary>
		/// Get many info values by name from the specified database server node,
		/// using host name and port.
		/// This method does not support user authentication.
		/// </summary>
		/// <param name="hostname">host name</param>
		/// <param name="port">host port</param>
		/// <param name="names">names of values to retrieve</param>
		public static Dictionary<string,string> Request(string hostname, int port, params string[] names)
		{
			IPAddress[] addresses = Connection.GetHostAddresses(hostname, DEFAULT_TIMEOUT);
			IPEndPoint ipe = new IPEndPoint(addresses[0], port);
			return Request(ipe, names);
		}

		/// <summary>
		/// Get default info from the specified database server node, using host name and port.
		/// This method does not support user authentication.
		/// </summary>
		/// <param name="hostname">host name</param>
		/// <param name="port">host port</param>
		public static Dictionary<string, string> Request(string hostname, int port)
		{
			IPAddress[] addresses = Connection.GetHostAddresses(hostname, DEFAULT_TIMEOUT);
			IPEndPoint ipe = new IPEndPoint(addresses[0], port);
			return Request(ipe);
		}

		//-------------------------------------------------------
		// Get Info via Socket Address
		//-------------------------------------------------------

		/// <summary>
		/// Get one info value by name from the specified database server node.
		/// This method does not support secure connections nor user authentication.
		/// </summary>
		/// <param name="socketAddress">InetSocketAddress of server node</param>
		/// <param name="name">name of value to retrieve</param>
		public static string Request(IPEndPoint socketAddress, string name)
		{
			Connection conn = new Connection(socketAddress, DEFAULT_TIMEOUT);

			try
			{
				return Request(conn, name);
			}
			finally
			{
				conn.Close();
			}
		}

		/// <summary>
		/// Get many info values by name from the specified database server node.
		/// This method does not support secure connections nor user authentication.
		/// </summary>
		/// <param name="socketAddress">InetSocketAddress of server node</param>
		/// <param name="names">names of values to retrieve</param>
		public static Dictionary<string, string> Request(IPEndPoint socketAddress, params string[] names)
		{
			Connection conn = new Connection(socketAddress, DEFAULT_TIMEOUT);

			try
			{
				return Request(conn, names);
			}
			finally
			{
				conn.Close();
			}
		}

		/// <summary>
		/// Get all the default info from the specified database server node.
		/// This method does not support secure connections nor user authentication.
		/// </summary>
		/// <param name="socketAddress">InetSocketAddress of server node</param>
		public static Dictionary<string, string> Request(IPEndPoint socketAddress)
		{
			Connection conn = new Connection(socketAddress, DEFAULT_TIMEOUT);

			try
			{
				return Request(conn);
			}
			finally
			{
				conn.Close();
			}
		}

		//-------------------------------------------------------
		// Get Info via Connection
		//-------------------------------------------------------

		/// <summary>
		/// Get one info value by name from the specified database server node.
		/// </summary>
		/// <param name="conn">socket connection to server node</param>
		/// <param name="name">name of value to retrieve</param>
		public static string Request(Connection conn, string name)
		{
			Info info = new Info(conn, name);
			return info.ParseSingleResponse(name);
		}

		/// <summary>
		/// Get many info values by name from the specified database server node.
		/// </summary>
		/// <param name="conn">socket connection to server node</param>
		/// <param name="names">names of values to retrieve</param>
		public static Dictionary<string,string> Request(Connection conn, params string[] names)
		{
			Info info = new Info(conn, names);
			return info.ParseMultiResponse();
		}

		/// <summary>
		/// Get all the default info from the specified database server node.
		/// </summary>
		/// <param name="conn">socket connection to server node</param>
		public static Dictionary<string,string> Request(Connection conn)
		{
			Info info = new Info(conn);
			return info.ParseMultiResponse();
		}

		//-------------------------------------------------------
		// Private methods.
		//-------------------------------------------------------

		/// <summary>
		/// Issue request and set results buffer. This method is used internally.
		/// The static request methods should be used instead.
		/// </summary>
		/// <param name="conn">socket connection to server node</param>
		/// <exception cref="AerospikeException">if socket send or receive fails</exception>
		private void SendCommand(Connection conn)
		{
			try
			{
				// Write size field.
				ulong size = ((ulong)offset - 8L) | (2L << 56) | (1L << 48);
				ByteUtil.LongToBytes(size, buffer, 0);

				// Write.
				conn.Write(buffer, offset);

				// Read - reuse input buffer.
				conn.ReadFully(buffer, 8);

				size = (ulong)ByteUtil.BytesToLong(buffer, 0);
				length = (int)(size & 0xFFFFFFFFFFFFL);
				ResizeBuffer(length);
				conn.ReadFully(buffer, length);
				offset = 0;
			}
			catch (SocketException se)
			{
				throw new AerospikeException(se);
			}
		}

		private void ResizeBuffer(int size)
		{
			if (size > buffer.Length)
			{
				buffer = ThreadLocalData.ResizeBuffer(size);
			}
		}

		private string ParseSingleResponse(string name)
		{
			// Convert the UTF8 byte array into a string.
			string response = ByteUtil.Utf8ToString(buffer, 0, length);

			if (response.StartsWith(name))
			{
				if (response.Length > name.Length + 1)
				{
					// Remove field name, tab and trailing newline from response.
					// This is faster than calling parseMultiResponse()
					return response.Substring(name.Length + 1, response.Length - 1 - (name.Length + 1));
				}
				else
				{
					return null;
				}
			}
			else
			{
				throw new AerospikeException.Parse("Info response does not include: " + name);
			}
		}

		private Dictionary<string, string> ParseMultiResponse()
		{
			Dictionary<string, string> responses = new Dictionary<string, string>();
			int offset = 0;
			int begin = 0;

			while (offset < length)
			{
				byte b = buffer[offset];

				if (b == '\t')
				{
					string name = ByteUtil.Utf8ToString(buffer, begin, offset - begin);
					CheckError(name);
					begin = ++offset;

					// Parse field value.
					while (offset < length)
					{
						if (buffer[offset] == '\n')
						{
							break;
						}
						offset++;
					}

					if (offset > begin)
					{
						string value = ByteUtil.Utf8ToString(buffer, begin, offset - begin);
						responses[name] = value;
					}
					else
					{
						responses[name] = null;
					}
					begin = ++offset;
				}
				else if (b == '\n')
				{
					if (offset > begin)
					{
						string name = ByteUtil.Utf8ToString(buffer, begin, offset - begin);
						CheckError(name);
						responses[name] = null;
					}
					begin = ++offset;
				}
				else
				{
					offset++;
				}
			}

			if (offset > begin)
			{
				string name = ByteUtil.Utf8ToString(buffer, begin, offset - begin);
				CheckError(name);
				responses[name] = null;
			}
			return responses;
		}

		private void CheckError(string str)
		{
			if (str.StartsWith("ERROR:"))
			{
				int begin = 6;
				int end = str.IndexOf(':', begin);
				int code = -1;
				string message = "";

				if (end >= 0)
				{
					code = int.Parse(str.Substring(begin, end - begin));

					if (str[str.Length-1] == '\n')
					{
						message = str.Substring(end + 1, str.Length - 2);
					}
					else
					{
						message = str.Substring(end + 1);
					}
				}
				throw new AerospikeException(code, message);
			}
		}

		/// <summary>
		/// Parser for responses in name/value pair format:
		/// <para>
		/// &lt;command&gt;\t&lt;name1&gt;=&lt;value1&gt;;&lt;name2&gt;=&lt;value2&gt;;...\n
		/// </para>
		/// </summary>
		public sealed class NameValueParser
		{
			private readonly Info parent;

			public NameValueParser(Info parent)
			{
				this.parent = parent;
			}

			internal int nameBegin;
			internal int nameEnd;
			internal int valueBegin;
			internal int valueEnd;

			/// <summary>
			/// Set pointers to next name/value pair.
			/// Return true if next name/value pair exists.
			/// Return false if at end.
			/// </summary>
			public bool Next()
			{
				nameBegin = parent.offset;

				while (parent.offset < parent.length)
				{
					byte b = parent.buffer[parent.offset];

					if (b == '=')
					{
						if (parent.offset <= nameBegin)
						{
							return false;
						}
						nameEnd = parent.offset;
						ParseValue();
						return true;
					}

					if (b == '\n')
					{
						break;
					}
					parent.offset++;
				}
				nameEnd = parent.offset;
				valueBegin = parent.offset;
				valueEnd = parent.offset;
				return parent.offset > nameBegin;
			}

			internal void ParseValue()
			{
				valueBegin = ++parent.offset;

				while (parent.offset < parent.length)
				{
					byte b = parent.buffer[parent.offset];

					if (b == ';')
					{
						valueEnd = parent.offset++;
						return;
					}

					if (b == '\n')
					{
						break;
					}
					parent.offset++;
				}
				valueEnd = parent.offset;
			}

			/// <summary>
			/// Get name.
			/// </summary>
			public string GetName()
			{
				int len = nameEnd - nameBegin;
				return ByteUtil.Utf8ToString(parent.buffer, nameBegin, len);
			}

			/// <summary>
			/// Get value.
			/// </summary>
			public string GetValue()
			{
				int len = valueEnd - valueBegin;
    
				if (len <= 0)
				{
					return null;
				}
				return ByteUtil.Utf8ToString(parent.buffer, valueBegin, len);
			}

			/// <summary>
			/// Get Base64 string value.
			/// </summary>
			public string GetStringBase64()
			{
				int len = valueEnd - valueBegin;

				if (len <= 0)
				{
					return null;
				}
				char[] chars = Encoding.ASCII.GetChars(parent.buffer, valueBegin, len);
				byte[] bytes = Convert.FromBase64CharArray(chars, 0, chars.Length);
				return ByteUtil.Utf8ToString(bytes, 0, bytes.Length);
			}
		}
	}
}
