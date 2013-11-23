/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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

		private const int DEFAULT_TIMEOUT = 2000;

		//-------------------------------------------------------
		// Member variables.
		//-------------------------------------------------------

		private byte[] buffer;
		private int length;
		private int offset;

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

		private void SkipToValue()
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

		//-------------------------------------------------------
		// Get Info via Host Name and Port
		//-------------------------------------------------------

		/// <summary>
		/// Get one info value by name from the specified database server node, using
		/// host name and port.
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
		// Get Info via Node
		//-------------------------------------------------------

		/// <summary>
		/// Get one info value by name from the specified database server node.
		/// </summary>
		/// <param name="node">server node</param>
		/// <param name="name">name of value to retrieve</param>
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
				conn.Close();
				throw;
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

		/// <summary>
		/// Get response buffer. For internal use only.
		/// </summary>
		public byte[] GetBuffer()
		{
			return buffer;
		}

		/// <summary>
		/// Get response length. For internal use only.
		/// </summary>
		public int GetLength()
		{
			return length;
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
				responses[name] = null;
			}
			return responses;
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
		}
	}
}