/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
		private const byte SET_WHITELIST = 14;
		private const byte SET_QUOTAS = 15;
		private const byte QUERY_ROLES = 16;
		private const byte LOGIN = 20;

		// Field IDs
		private const byte USER = 0;
		private const byte PASSWORD = 1;
		private const byte OLD_PASSWORD = 2;
		private const byte CREDENTIAL = 3;
		private const byte CLEAR_PASSWORD = 4;
		private const byte SESSION_TOKEN = 5;
		private const byte SESSION_TTL = 6;
		private const byte ROLES = 10;
		private const byte ROLE = 11;
		private const byte PRIVILEGES = 12;
		private const byte WHITELIST = 13;
		private const byte READ_QUOTA = 14;
		private const byte WRITE_QUOTA = 15;
		private const byte READ_INFO = 16;
		private const byte WRITE_INFO = 17;
		private const byte CONNECTIONS = 18;

		// Misc
		private const ulong MSG_VERSION = 2UL;
		private const ulong MSG_TYPE = 2UL;
		private const int FIELD_HEADER_SIZE = 5;
		private const int HEADER_SIZE = 24;
		private const int HEADER_REMAINING = 16;
		private const int RESULT_CODE = 9;
		private const int QUERY_END = 50;

		private byte[] dataBuffer;
		private int dataOffset;
		private readonly int dataBegin;

		public AdminCommand()
		{
			dataBuffer = new byte[8096];
			dataBegin = 0;
			dataOffset = 8;
		}

		public AdminCommand(byte[] dataBuffer, int dataOffset)
		{
			this.dataBuffer = dataBuffer;
			this.dataBegin = dataOffset;
			this.dataOffset = dataOffset + 8;
		}

		public void Login(Cluster cluster, Connection conn, out byte[] sessionToken, out DateTime? sessionExpiration)
		{
			dataOffset = 8;

			conn.SetTimeout(cluster.loginTimeout);

			try
			{
				if (cluster.authMode == AuthMode.INTERNAL)
				{
					WriteHeader(LOGIN, 2);
					WriteField(USER, cluster.user);
					WriteField(CREDENTIAL, cluster.passwordHash);
				}
				else if (cluster.authMode == AuthMode.PKI)
				{
					WriteHeader(LOGIN, 0);
				}
				else
				{
					WriteHeader(LOGIN, 3);
					WriteField(USER, cluster.user);
					WriteField(CREDENTIAL, cluster.passwordHash);
					WriteField(CLEAR_PASSWORD, cluster.password);
				}
				WriteSize();
				conn.Write(dataBuffer, dataOffset);
				conn.ReadFully(dataBuffer, HEADER_SIZE);

				int result = dataBuffer[RESULT_CODE];

				if (result != 0)
				{
					if (result == ResultCode.SECURITY_NOT_ENABLED)
					{
						// Server does not require login.
						sessionToken = null;
						sessionExpiration = null;
						return;
					}
					throw new AerospikeException(result, "Login failed");
				}

				// Read session token.
				long size = ByteUtil.BytesToLong(dataBuffer, 0);
				int receiveSize = ((int)(size & 0xFFFFFFFFFFFFL)) - HEADER_REMAINING;
				int fieldCount = dataBuffer[11];

				if (receiveSize <= 0 || receiveSize > dataBuffer.Length || fieldCount <= 0)
				{
					throw new AerospikeException(result, "Failed to retrieve session token");
				}

				conn.ReadFully(dataBuffer, receiveSize);
				dataOffset = 0;

				byte[] token = null;
				DateTime? ttl = null; 

				for (int i = 0; i < fieldCount; i++)
				{
					int len = ByteUtil.BytesToInt(dataBuffer, dataOffset);
					dataOffset += 4;
					int id = dataBuffer[dataOffset++];
					len--;

					if (id == SESSION_TOKEN)
					{
						token = new byte[len];
						Array.Copy(dataBuffer, dataOffset, token, 0, len);
					}
					else if (id == SESSION_TTL)
					{
						// Subtract 60 seconds from ttl so client session expires before server session.
						long seconds = ByteUtil.BytesToUInt(dataBuffer, dataOffset) - 60;

						if (seconds > 0)
						{
							ttl = DateTime.UtcNow.AddSeconds(seconds);
						}
						else
						{
							throw new AerospikeException("Invalid session expiration: " + seconds);
						}
					}
					dataOffset += len;
				}

				if (token == null)
				{
					throw new AerospikeException("Failed to retrieve session token");
				}
				sessionToken = token;
				sessionExpiration = ttl;
			}
			finally
			{
				conn.SetTimeout(cluster.connectionTimeout);
			}
		}

		public static bool Authenticate(Cluster cluster, Connection conn, byte[] sessionToken)
		{
			AdminCommand command = new AdminCommand(ThreadLocalData.GetBuffer(), 0);
			return command.AuthenticateSession(cluster, conn, sessionToken);
		}

		public bool AuthenticateSession(Cluster cluster, Connection conn, byte[] sessionToken)
		{
			dataOffset = 8;
			SetAuthenticate(cluster, sessionToken);
			conn.Write(dataBuffer, dataOffset);
			conn.ReadFully(dataBuffer, HEADER_SIZE);

			int result = dataBuffer[RESULT_CODE];
			return result == 0 || result == ResultCode.SECURITY_NOT_ENABLED;
		}

		public int SetAuthenticate(Cluster cluster, byte[] sessionToken)
		{
			if (cluster.authMode != AuthMode.PKI)
			{
				WriteHeader(AUTHENTICATE, 2);
				WriteField(USER, cluster.user);
			}
			else
			{
				WriteHeader(AUTHENTICATE, 1);
			}
			WriteField(SESSION_TOKEN, sessionToken);
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
			WriteField(OLD_PASSWORD, cluster.passwordHash);
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

		public void CreateRole
		(
			Cluster cluster,
			AdminPolicy policy,
			string roleName,
			IList<Privilege> privileges,
			IList<string> whitelist,
			int readQuota,
			int writeQuota
		)
		{
			byte fieldCount = 1;

			if (privileges != null && privileges.Count > 0)
			{
				fieldCount++;
			}

			if (whitelist != null && whitelist.Count > 0)
			{
				fieldCount++;
			}

			if (readQuota > 0)
			{
				fieldCount++;
			}

			if (writeQuota > 0)
			{
				fieldCount++;
			}

			WriteHeader(CREATE_ROLE, fieldCount);
			WriteField(ROLE, roleName);

			if (privileges != null && privileges.Count > 0)
			{
				WritePrivileges(privileges);
			}

			if (whitelist != null && whitelist.Count > 0)
			{
				WriteWhitelist(whitelist);
			}

			if (readQuota > 0)
			{
				WriteField(READ_QUOTA, readQuota);
			}

			if (writeQuota > 0)
			{
				WriteField(WRITE_QUOTA, writeQuota);
			}
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

		public void SetWhitelist(Cluster cluster, AdminPolicy policy, string roleName, IList<string> whitelist)
		{
			byte fieldCount = (whitelist != null && whitelist.Count > 0) ? (byte)2 : (byte)1;

			WriteHeader(SET_WHITELIST, fieldCount);
			WriteField(ROLE, roleName);

			if (whitelist != null && whitelist.Count > 0)
			{
				WriteWhitelist(whitelist);
			}

			ExecuteCommand(cluster, policy);
		}

		public void setQuotas(Cluster cluster, AdminPolicy policy, String roleName, int readQuota, int writeQuota)
		{
			WriteHeader(SET_QUOTAS, 3);
			WriteField(ROLE, roleName);
			WriteField(READ_QUOTA, readQuota);
			WriteField(WRITE_QUOTA, writeQuota);
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

		private void WriteWhitelist(IList<string> whitelist)
		{
			int offset = dataOffset + FIELD_HEADER_SIZE;
			bool comma = false;

			foreach (string address in whitelist)
			{
				if (comma)
				{
					dataBuffer[offset++] = (byte)',';
				}
				else
				{
					comma = true;
				}
				offset += ByteUtil.StringToUtf8(address, dataBuffer, offset);
			}

			int size = offset - dataOffset - FIELD_HEADER_SIZE;
			WriteFieldHeader(WHITELIST, size);
			dataOffset = offset;
		}
		
		private void WriteSize()
		{
			// Write total size of message which is the current offset.
			ulong size = ((ulong)dataOffset - (ulong)dataBegin - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
			ByteUtil.LongToBytes(size, dataBuffer, dataBegin);
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

		private void WriteField(byte id, int val)
		{
			WriteFieldHeader(id, 4);
			ByteUtil.IntToBytes((uint)val, dataBuffer, dataOffset);
			dataOffset += 4;
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
				conn.UpdateLastUsed();
				node.PutConnection(conn);
			}
			catch (Exception)
			{
				// Garbage may be in socket.  Do not put back into pool.
				node.CloseConnectionOnError(conn);
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
				node.CloseConnectionOnError(conn);
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
					conn.UpdateLastUsed();
					status = ParseBlock(receiveSize);
				}
			}
			return status;
		}

		public static string HashPassword(string password)
		{
			return BCrypt.HashPassword(password, "$2a$10$7EqJtq98hPqEX7fNZaFWoO");
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

						switch (id)
						{
							case USER:
								user.name = ByteUtil.Utf8ToString(base.dataBuffer, base.dataOffset, len);
								base.dataOffset += len;
								break;

							case ROLES:
								ParseRoles(user);
								break;

							case READ_INFO:
								user.readInfo = ParseInfo();
								break;

							case WRITE_INFO:
								user.writeInfo = ParseInfo();
								break;

							case CONNECTIONS:
								user.connsInUse = ByteUtil.BytesToUInt(base.dataBuffer, base.dataOffset);
								base.dataOffset += len;
								break;

							default:
								base.dataOffset += len;
								break;
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

			private List<uint> ParseInfo()
			{
				int size = base.dataBuffer[base.dataOffset++] & 0xFF;
				List<uint> list = new List<uint>(size);

				for (int i = 0; i < size; i++)
				{
					uint val = ByteUtil.BytesToUInt(base.dataBuffer, base.dataOffset);
					base.dataOffset += 4;
					list.Add(val);
				}
				return list;
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

						switch (id)
						{
							case ROLE:
								role.name = ByteUtil.Utf8ToString(base.dataBuffer, base.dataOffset, len);
								base.dataOffset += len;
								break;

							case PRIVILEGES:
								ParsePrivileges(role);
								break;

							case WHITELIST:
								role.whitelist = ParseWhitelist(len);
								break;

							case READ_QUOTA:
								role.readQuota = ByteUtil.BytesToInt(base.dataBuffer, base.dataOffset);
								base.dataOffset += len;
								break;

							case WRITE_QUOTA:
								role.writeQuota = ByteUtil.BytesToInt(base.dataBuffer, base.dataOffset);
								base.dataOffset += len;
								break;

							default:
								base.dataOffset += len;
								break;
						}
					}

					if (role.name == null)
					{
						throw new AerospikeException(ResultCode.INVALID_ROLE);
					}

					if (role.privileges == null)
					{
						role.privileges = new List<Privilege>(0);
					}

					if (role.whitelist == null)
					{
						role.whitelist = new List<string>(0);
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

			private List<string> ParseWhitelist(int len)
			{
				List<string> list = new List<string>();
				int begin = base.dataOffset;
				int max = base.dataOffset + len;
				int l;

				while (base.dataOffset < max)
				{
					if (base.dataBuffer[base.dataOffset] == ',')
					{
						l = base.dataOffset - begin;

						if (l > 0)
						{
							string s = ByteUtil.Utf8ToString(base.dataBuffer, begin, l);
							list.Add(s);
						}
						begin = ++base.dataOffset;
					}
					else
					{
						base.dataOffset++;
					}
				}
				l = base.dataOffset - begin;

				if (l > 0)
				{
					string s = ByteUtil.Utf8ToString(base.dataBuffer, begin, l);
					list.Add(s);
				}
				return list;
			}
		}
	}
}
