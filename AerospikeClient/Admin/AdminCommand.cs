/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
	public class AdminCommand
	{
		// Commands
		private const byte AUTHENTICATE = 0;
		private const byte CREATE_USER = 1;
		private const byte DROP_USER = 2;
		private const byte SET_PASSWORD = 3;
		private const byte CHANGE_PASSWORD = 4;
		private const byte GRANT_ROLES = 5;
		private const byte REVOKE_ROLES = 6;
		private const byte QUERY_USERS = 9;
		private const byte CREATE_ROLE = 10;
		private const byte DROP_ROLE = 11;
		private const byte GRANT_PRIVILEGES = 12;
		private const byte REVOKE_PRIVILEGES = 13; 
		private const byte QUERY_ROLES = 16;

		// Field IDs
		private const byte USER = 0;
		private const byte PASSWORD = 1;
		private const byte OLD_PASSWORD = 2;
		private const byte CREDENTIAL = 3;
		private const byte ROLES = 10;
		private const byte ROLE = 11;
		private const byte PRIVILEGES = 12;

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

		public void CreateUser(Cluster cluster, AdminPolicy policy, string user, string password, IList<string> roles)
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

		public void GrantRoles(Cluster cluster, AdminPolicy policy, string user, IList<string> roles)
		{
			WriteHeader(GRANT_ROLES, 2);
			WriteField(USER, user);
			WriteRoles(roles);
			ExecuteCommand(cluster, policy);
		}

		public void RevokeRoles(Cluster cluster, AdminPolicy policy, string user, IList<string> roles)
		{
			WriteHeader(REVOKE_ROLES, 2);
			WriteField(USER, user);
			WriteRoles(roles);
			ExecuteCommand(cluster, policy);
		}

		public void CreateRole(Cluster cluster, AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			WriteHeader(CREATE_ROLE, 2);
			WriteField(ROLE, roleName);
			WritePrivileges(privileges);
			ExecuteCommand(cluster, policy);
		}

		public void DropRole(Cluster cluster, AdminPolicy policy, string roleName)
		{
			WriteHeader(DROP_ROLE, 1);
			WriteField(ROLE, roleName);
			ExecuteCommand(cluster, policy);
		}

		public void GrantPrivileges(Cluster cluster, AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			WriteHeader(GRANT_PRIVILEGES, 2);
			WriteField(ROLE, roleName);
			WritePrivileges(privileges);
			ExecuteCommand(cluster, policy);
		}

		public void RevokePrivileges(Cluster cluster, AdminPolicy policy, string roleName, IList<Privilege> privileges)
		{
			WriteHeader(REVOKE_PRIVILEGES, 2);
			WriteField(ROLE, roleName);
			WritePrivileges(privileges);
			ExecuteCommand(cluster, policy);
		}

		private void WriteRoles(IList<string> roles)
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

		private void WritePrivileges(IList<Privilege> privileges)
		{
			int offset = dataOffset + FIELD_HEADER_SIZE;
			dataBuffer[offset++] = (byte)privileges.Count;

			foreach (Privilege privilege in privileges)
			{
				dataBuffer[offset++] = (byte)privilege.code;

				if (privilege.CanScope())
				{
					if (!(privilege.setName == null || privilege.setName.Length == 0) &&
						(privilege.ns == null || privilege.ns.Length == 0))
					{
						throw new AerospikeException(ResultCode.INVALID_PRIVILEGE, "Admin privilege '" + 
							privilege.PrivilegeCodeToString() + "' has a set scope with an empty namespace.");
					}

					int len = ByteUtil.StringToUtf8(privilege.ns, dataBuffer, offset + 1);
					dataBuffer[offset] = (byte)len;
					offset += len + 1;

					len = ByteUtil.StringToUtf8(privilege.setName, dataBuffer, offset + 1);
					dataBuffer[offset] = (byte)len;
					offset += len + 1;
				}
				else
				{
					if (! (privilege.ns == null || privilege.ns.Length == 0) ||
						! (privilege.setName == null || privilege.setName.Length == 0))
					{
						throw new AerospikeException(ResultCode.INVALID_PRIVILEGE, "Admin global privilege '" +
							privilege.PrivilegeCodeToString() + "' has namespace/set scope which is invalid.");
					}
				}
			}

			int size = offset - dataOffset - FIELD_HEADER_SIZE;
			WriteFieldHeader(PRIVILEGES, size);
			dataOffset = offset;
		}

		private void WriteSize()
		{
			// Write total size of message which is the current offset.
			ulong size = ((ulong)dataOffset - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, 0);
		}

		void WriteHeader(byte command, byte fieldCount)
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

		private void ExecuteQuery(Cluster cluster, AdminPolicy policy)
		{
			WriteSize();
			Node node = cluster.GetRandomNode();
			int timeout = (policy == null) ? 1000 : policy.timeout;
			int status = 0;
			Connection conn = node.GetConnection(timeout);

			try
			{
				conn.Write(dataBuffer, dataOffset);
				status = ReadBlocks(conn);
				node.PutConnection(conn);
			}
			catch (Exception e)
			{
				// Garbage may be in socket.  Do not put back into pool.
				conn.Close();
				throw new AerospikeException(e);
			}

			if (status != QUERY_END && status > 0)
			{
				throw new AerospikeException(status, "Query failed.");
			}
		}

		private int ReadBlocks(Connection conn)
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
					status = ParseBlock(receiveSize);
				}
			}
			return status;
		}

		public static string HashPassword(string password)
		{
			return BCrypt.Net.BCrypt.HashPassword(password, "$2a$10$7EqJtq98hPqEX7fNZaFWoO");
		}

		internal virtual int ParseBlock(int receiveSize)
		{
			return QUERY_END;
		}

		public sealed class UserCommand : AdminCommand
		{
			internal readonly List<User> list;

			public UserCommand(int capacity)
			{
				list = new List<User>(capacity);
			}

			public User QueryUser(Cluster cluster, AdminPolicy policy, string user)
			{
				base.WriteHeader(QUERY_USERS, 1);
				base.WriteField(USER, user);
				base.ExecuteQuery(cluster, policy);
				return (list.Count > 0) ? list[0] : null;
			}

			public List<User> QueryUsers(Cluster cluster, AdminPolicy policy)
			{
				base.WriteHeader(QUERY_USERS, 0);
				base.ExecuteQuery(cluster, policy);
				return list;
			}

			internal override int ParseBlock(int receiveSize)
			{
				base.dataOffset = 0;

				while (base.dataOffset < receiveSize)
				{
					int resultCode = base.dataBuffer[base.dataOffset + 1];

					if (resultCode != 0)
					{
						return resultCode;
					}

					User user = new User();
					int fieldCount = base.dataBuffer[base.dataOffset + 3];
					base.dataOffset += HEADER_REMAINING;

					for (int i = 0; i < fieldCount; i++)
					{
						int len = ByteUtil.BytesToInt(base.dataBuffer, base.dataOffset);
						base.dataOffset += 4;
						int id = base.dataBuffer[base.dataOffset++];
						len--;

						if (id == USER)
						{
							user.name = ByteUtil.Utf8ToString(base.dataBuffer, base.dataOffset, len);
							base.dataOffset += len;
						}
						else if (id == ROLES)
						{
							ParseRoles(user);
						}
						else
						{
							base.dataOffset += len;
						}
					}

					if (user.name == null && user.roles == null)
					{
						continue;
					}

					if (user.roles == null)
					{
						user.roles = new List<string>(0);
					}
					list.Add(user);
				}
				return 0;
			}

			internal void ParseRoles(User user)
			{
				int size = base.dataBuffer[base.dataOffset++];
				user.roles = new List<string>(size);

				for (int i = 0; i < size; i++)
				{
					int len = base.dataBuffer[base.dataOffset++];
					string role = ByteUtil.Utf8ToString(base.dataBuffer, base.dataOffset, len);
					base.dataOffset += len;
					user.roles.Add(role);
				}
			}
		}

		public sealed class RoleCommand : AdminCommand
		{
			internal readonly List<Role> list;

			public RoleCommand(int capacity)
			{
				list = new List<Role>(capacity);
			}

			public Role QueryRole(Cluster cluster, AdminPolicy policy, string roleName)
			{
				base.WriteHeader(QUERY_ROLES, 1);
				base.WriteField(ROLE, roleName);
				base.ExecuteQuery(cluster, policy);
				return (list.Count > 0) ? list[0] : null;
			}

			public List<Role> QueryRoles(Cluster cluster, AdminPolicy policy)
			{
				base.WriteHeader(QUERY_ROLES, 0);
				base.ExecuteQuery(cluster, policy);
				return list;
			}

			internal override int ParseBlock(int receiveSize)
			{
				base.dataOffset = 0;

				while (base.dataOffset < receiveSize)
				{
					int resultCode = base.dataBuffer[base.dataOffset + 1];

					if (resultCode != 0)
					{
						return resultCode;
					}

					Role role = new Role();
					int fieldCount = base.dataBuffer[base.dataOffset + 3];
					base.dataOffset += HEADER_REMAINING;

					for (int i = 0; i < fieldCount; i++)
					{
						int len = ByteUtil.BytesToInt(base.dataBuffer, base.dataOffset);
						base.dataOffset += 4;
						int id = base.dataBuffer[base.dataOffset++];
						len--;

						if (id == ROLE)
						{
							role.name = ByteUtil.Utf8ToString(base.dataBuffer, base.dataOffset, len);
							base.dataOffset += len;
						}
						else if (id == PRIVILEGES)
						{
							ParsePrivileges(role);
						}
						else
						{
							base.dataOffset += len;
						}
					}

					if (role.name == null && role.privileges == null)
					{
						continue;
					}

					if (role.privileges == null)
					{
						role.privileges = new List<Privilege>(0);
					}
					list.Add(role);
				}
				return 0;
			}

			internal void ParsePrivileges(Role role)
			{
				int size = base.dataBuffer[base.dataOffset++];
				role.privileges = new List<Privilege>(size);

				for (int i = 0; i < size; i++)
				{
					Privilege priv = new Privilege();
					priv.code = (PrivilegeCode)base.dataBuffer[base.dataOffset++];

					if (priv.CanScope())
					{
						int len = base.dataBuffer[base.dataOffset++];
						priv.ns = ByteUtil.Utf8ToString(base.dataBuffer, base.dataOffset, len);
						base.dataOffset += len;

						len = base.dataBuffer[base.dataOffset++];
						priv.setName = ByteUtil.Utf8ToString(base.dataBuffer, base.dataOffset, len);
						base.dataOffset += len;
					}
					role.privileges.Add(priv);
				}
			}
		}
	}
}
