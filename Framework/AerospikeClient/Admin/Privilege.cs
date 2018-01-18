/* 
 * Copyright 2012-2018 Aerospike, Inc.
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

namespace Aerospike.Client
{
	/// <summary>
	/// User privilege.
	/// </summary>
	public sealed class Privilege
	{
		/// <summary>
		/// Privilege code.
		/// </summary>
		public PrivilegeCode code;

		/// <summary>
		/// Namespace scope. Apply permission to this namespace only.
		/// If namespace is null, the privilege applies to all namespaces.
		/// </summary>
		public string ns;

		/// <summary>
		/// Set name scope. Apply permission to this set within namespace only.
		/// If set is null, the privilege applies to all sets within namespace.
		/// </summary>
		public string setName;

		/// <summary>
		/// Can privilege be scoped with namespace and set.
		/// </summary>
		public bool CanScope()
		{
			return code >= PrivilegeCode.READ;
		}

		/// <summary>
		/// Privilege code property.
		/// </summary>
		public PrivilegeCode Code
		{
			get {return code;}
			set {code = value;}
		}

		/// <summary>
		/// Privilege code property.
		/// </summary>
		public string CodeString
		{
			get { return PrivilegeCodeToString(); }
		}

		/// <summary>
		/// Namespace property.
		/// </summary>
		public string Namespace
		{
			get { return ns; }
			set { ns = value; }
		}

		/// <summary>
		/// SetName property.
		/// </summary>
		public string SetName
		{
			get { return setName; }
			set { setName = value; }
		}

		/// <summary>
		/// Return privilege shallow clone.
		/// </summary>
		public Privilege Clone()
		{
			Privilege priv = new Privilege();
			priv.code = this.code;
			priv.ns = this.ns;
			priv.setName = this.setName;
			return priv;
		}

		/// <summary>
		/// Return if privileges are equal.
		/// </summary>
		public override bool Equals(object obj)
		{
			Privilege other = (Privilege)obj;

			if (this.code != other.code)
			{
				return false;
			}

			if (this.ns == null)
			{
				if (other.ns != null)
				{
					return false;
				}
			}
			else if (other.ns == null)
			{
				return false;
			}
			else if (!this.ns.Equals(other.ns))
			{
				return false;
			}

			if (this.setName == null)
			{
				if (other.setName != null)
				{
					return false;
				}
			}
			else if (other.setName == null)
			{
				return false;
			}
			else if (!this.setName.Equals(other.setName))
			{
				return false;
			}

			return true;
		}

		/// <summary>
		/// Return privilege hashcode.
		/// </summary>
		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		/// <summary>
		/// Convert privilege to string.
		/// </summary>
		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(100);
			sb.Append(PrivilegeCodeToString());

			if (ns != null && ns.Length > 0)
			{
				sb.Append('.');
				sb.Append(ns);
			}

			if (setName != null && setName.Length > 0)
			{
				sb.Append('.');
				sb.Append(setName);
			}
			return sb.ToString();
		}

		/// <summary>
		/// Convert privilege code to string.
		/// </summary>
		public string PrivilegeCodeToString()
		{
			switch (code)
			{
				case PrivilegeCode.SYS_ADMIN:
					return Role.SysAdmin;

				case PrivilegeCode.USER_ADMIN:
					return Role.UserAdmin;

				case PrivilegeCode.DATA_ADMIN:
					return Role.DataAdmin;

				case PrivilegeCode.READ:
					return Role.Read;

				case PrivilegeCode.READ_WRITE:
					return Role.ReadWrite;

				case PrivilegeCode.READ_WRITE_UDF:
					return Role.ReadWriteUdf;

				default:
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid privilege code: " + code);
			}
		}
	}
}
