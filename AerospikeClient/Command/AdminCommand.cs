/* 
 * Copyright 2012-2014 Aerospike, Inc.
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

namespace Aerospike.Client
{
	public sealed class AdminCommand
	{
		// Commands
		private const byte AUTHENTICATE = 0;
		private const byte CREATE_USER = 1;
		private const byte DROP_USER = 2;
		private const byte SET_PASSWORD = 3;
		private const byte CHANGE_PASSWORD = 4;
		private const byte GRANT_ROLES = 5;
		private const byte REVOKE_ROLES = 6;
		private const byte REPLACE_ROLES = 7;
		private const byte CREATE_ROLE = 8;
		private const byte QUERY_USERS = 9;
		private const byte QUERY_ROLES = 10;

		// Field IDs
		private const byte USER = 0;
		private const byte PASSWORD = 1;
		private const byte OLD_PASSWORD = 2;
		private const byte CREDENTIAL = 3;
		private const byte ROLES = 10;
		private const byte PRIVILEGES = 11;

		// Misc
		private const ulong MSG_VERSION = 0L;
		private const ulong MSG_TYPE = 2L;
		private const int FIELD_HEADER_SIZE = 5;
		private const int HEADER_SIZE = 24;
		private const int HEADER_REMAINING = 16;
		private const int RESULT_CODE = 9;
		private const int QUERY_END = 50;

		private byte[] dataBuffer;
		private int dataOffset;

		public AdminCommand()
		{
			dataBuffer = ThreadLocalData.GetBuffer();
			dataOffset = 8;
		}

		public AdminCommand(byte[] dataBuffer)
		{
			this.dataBuffer = dataBuffer;
			dataOffset = 8;
		}

		public void Authenticate(Connection conn, byte[] user, byte[] password)
		{
			SetAuthenticate(user, password);
			conn.Write(dataBuffer, dataOffset);
			conn.ReadFully(dataBuffer, HEADER_SIZE);

			int result = dataBuffer[RESULT_CODE];
			if (result != 0)
			{
				throw new AerospikeException(result, "Authentication failed");
			}
		}

		public int SetAuthenticate(byte[] user, byte[] password)
		{
			WriteHeader(AUTHENTICATE, 2);
			WriteField(USER, user);
			WriteField(CREDENTIAL, password);
			WriteSize();
			return dataOffset;
		}

		public void CreateUser(Cluster cluster, AdminPolicy policy, string user, string password, List<string> roles)
		{
			WriteHeader(CREATE_USER, 3);
			WriteField(USER, user);
			WriteField(PASSWORD, password);
			WriteRoles(roles);
			ExecuteCommand(cluster, policy);
		}

		public void DropUser(Cluster cluster, AdminPolicy policy, string user)
		{
			WriteHeader(DROP_USER, 1);
			WriteField(USER, user);
			ExecuteCommand(cluster, policy);
		}

		public void SetPassword(Cluster cluster, AdminPolicy policy, byte[] user, string password)
		{
			WriteHeader(SET_PASSWORD, 2);
			WriteField(USER, user);
			WriteField(PASSWORD, password);
			ExecuteCommand(cluster, policy);
		}

		public void ChangePassword(Cluster cluster, AdminPolicy policy, byte[] user, string password)
		{
			WriteHeader(CHANGE_PASSWORD, 3);
			WriteField(USER, user);
			WriteField(OLD_PASSWORD, cluster.password);
			WriteField(PASSWORD, password);
			ExecuteCommand(cluster, policy);
		}

		public void GrantRoles(Cluster cluster, AdminPolicy policy, string user, List<string> roles)
		{
			WriteHeader(GRANT_ROLES, 2);
			WriteField(USER, user);
			WriteRoles(roles);
			ExecuteCommand(cluster, policy);
		}

		public void RevokeRoles(Cluster cluster, AdminPolicy policy, string user, List<string> roles)
		{
			WriteHeader(REVOKE_ROLES, 2);
			WriteField(USER, user);
			WriteRoles(roles);
			ExecuteCommand(cluster, policy);
		}

		public void ReplaceRoles(Cluster cluster, AdminPolicy policy, string user, List<string> roles)
		{
			WriteHeader(REPLACE_ROLES, 2);
			WriteField(USER, user);
			WriteRoles(roles);
			ExecuteCommand(cluster, policy);
		}

		public UserRoles QueryUser(Cluster cluster, AdminPolicy policy, string user)
		{
			List<UserRoles> list = new List<UserRoles>(1);
			WriteHeader(QUERY_USERS, 1);
			WriteField(USER, user);
			ReadUsers(cluster, policy, list);
			return (list.Count > 0) ? list[0] : null;
		}

		public List<UserRoles> QueryUsers(Cluster cluster, AdminPolicy policy)
		{
			List<UserRoles> list = new List<UserRoles>(100);
			WriteHeader(QUERY_USERS, 0);
			ReadUsers(cluster, policy, list);
			return list;
		}

		private void WriteRoles(List<string> roles)
		{
			int offset = dataOffset + FIELD_HEADER_SIZE;
			dataBuffer[offset++] = (byte)roles.Count;

			foreach (string role in roles)
			{
				int len = ByteUtil.StringToUtf8(role, dataBuffer, offset + 1);
				dataBuffer[offset] = (byte)len;
				offset += len + 1;
			}

			int size = offset - dataOffset - FIELD_HEADER_SIZE;
			WriteFieldHeader(ROLES, size);
			dataOffset = offset;
		}

		private void WriteSize()
		{
			// Write total size of message which is the current offset.
			ulong size = ((ulong)dataOffset - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);
		}

		private void WriteHeader(byte command, byte fieldCount)
		{
			// Authenticate header is almost all zeros
			Array.Clear(dataBuffer, dataOffset, 16);
			dataBuffer[dataOffset + 2] = command;
			dataBuffer[dataOffset + 3] = fieldCount;
			dataOffset += 16;
		}

		private void WriteField(byte id, string str)
		{
			int len = ByteUtil.StringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
			WriteFieldHeader(id, len);
			dataOffset += len;
		}

		private void WriteField(byte id, byte[] bytes)
		{
			Array.Copy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.Length);
			WriteFieldHeader(id, bytes.Length);
			dataOffset += bytes.Length;
		}

		private void WriteFieldHeader(byte id, int size)
		{
			ByteUtil.IntToBytes((uint)size + 1, dataBuffer, dataOffset);
			dataOffset += 4;
			dataBuffer[dataOffset++] = id;
		}

		private void ExecuteCommand(Cluster cluster, AdminPolicy policy)
		{
			WriteSize();
			Node node = cluster.GetRandomNode();
			int timeout = (policy == null) ? 1000 : policy.timeout;
			Connection conn = node.GetConnection(timeout);

			try
			{
				conn.Write(dataBuffer, dataOffset);
				conn.ReadFully(dataBuffer, HEADER_SIZE);
				node.PutConnection(conn);
			}
			catch (Exception)
			{
				// Garbage may be in socket.  Do not put back into pool.
				conn.Close();
				throw;
			}

			int result = dataBuffer[RESULT_CODE];

			if (result != 0)
			{
				throw new AerospikeException(result);
			}
		}

		public void ReadUsers(Cluster cluster, AdminPolicy policy, List<UserRoles> list)
		{
			WriteSize();
			Node node = cluster.GetRandomNode();
			int timeout = (policy == null) ? 1000 : policy.timeout;
			int status = 0;
			Connection conn = node.GetConnection(0);

			try
			{
				conn.Write(dataBuffer, dataOffset);
				status = ReadUserBlocks(conn, list);
				node.PutConnection(conn);
			}
			catch (Exception)
			{
				// Garbage may be in socket.  Do not put back into pool.
				conn.Close();
				throw;
			}

			if (status > 0)
			{
 				throw new AerospikeException(status, "Query users failed.");
			}
		}

		private int ReadUserBlocks(Connection conn, List<UserRoles> list)
		{
			int status = 0;

			while (status == 0)
			{
				conn.ReadFully(dataBuffer, 8);
				long size = ByteUtil.BytesToLong(dataBuffer, 0);
				int receiveSize = ((int)(size & 0xFFFFFFFFFFFFL));

				if (receiveSize > 0)
				{
					if (receiveSize > dataBuffer.Length)
					{
						dataBuffer = ThreadLocalData.ResizeBuffer(receiveSize);
					}
					conn.ReadFully(dataBuffer, receiveSize);
					status = ParseUsers(list, receiveSize);
				}
				else
				{
					break;
				}
			}
			return status;
		}

		private int ParseUsers(List<UserRoles> list, int receiveSize)
		{
			dataOffset = 0;

			while (dataOffset < receiveSize)
			{
				int resultCode = dataBuffer[dataOffset + 1];

				if (resultCode != 0)
				{
					if (resultCode == QUERY_END)
					{
						return -1;
					}
					return resultCode;
				}

				UserRoles userRoles = new UserRoles();
				int fieldCount = dataBuffer[dataOffset + 3];
				dataOffset += HEADER_REMAINING;

				for (int i = 0; i < fieldCount; i++)
				{
					int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
					dataOffset += 4;
					int id = dataBuffer[dataOffset++];
					len--;

					if (id == USER)
					{
						userRoles.user = ByteUtil.Utf8ToString(dataBuffer, dataOffset, len);
						dataOffset += len;
					}
					else if (id == ROLES)
					{
						ParseRoles(userRoles);
					}
					else
					{
						dataOffset += len;
					}
				}

				if (userRoles.user == null && userRoles.roles == null)
				{
					continue;
				}

				if (userRoles.roles == null)
				{
					userRoles.roles = new List<string>(0);
				}
				list.Add(userRoles);
			}
			return 0;
		}

		private void ParseRoles(UserRoles userRoles)
		{
			int size = dataBuffer[dataOffset++];
			userRoles.roles = new List<string>(size);

			for (int i = 0; i < size; i++)
			{
				int len = dataBuffer[dataOffset++];
				string role = ByteUtil.Utf8ToString(dataBuffer, dataOffset, len);
				dataOffset += len;
				userRoles.roles.Add(role);
			}
		}

		public static string HashPassword(string password)
		{
			return BCrypt.Net.BCrypt.HashPassword(password, "$2a$10$7EqJtq98hPqEX7fNZaFWoO");
		}
	}
}
