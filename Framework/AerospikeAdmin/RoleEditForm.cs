/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.ComponentModel;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Aerospike.Client;

namespace Aerospike.Admin
{
	public partial class RoleEditForm : Form
	{
		private readonly AerospikeClient client;
		private readonly EditType editType;
		private readonly List<Privilege> oldPrivileges;
		private readonly List<string> oldWhitelist;
		private readonly int oldReadQuota;
		private readonly int oldWriteQuota;

		public RoleEditForm(AerospikeClient client, EditType editType, RoleRow row)
		{
			this.client = client;
			this.editType = editType;
			InitializeComponent();

			grid.AutoGenerateColumns = false;
			PrivilegeCodeColumn.DataPropertyName = "Code";
			PrivilegeCodeColumn.ValueMember = "PrivilegeCode";
			PrivilegeCodeColumn.DisplayMember = "Label";
			PrivilegeCodeColumn.DataSource = GetPrivilegeTypeBinding();
			NamespaceColumn.DataPropertyName = "Namespace";
			SetNameColumn.DataPropertyName = "SetName";

			BindingSource bindingSource = new BindingSource();

			switch (editType)
			{
				case EditType.CREATE:
					bindingSource.DataSource = new BindingList<Privilege>();
					break;

				case EditType.EDIT:
					this.Text = "Edit Role";
					nameBox.Enabled = false;
					nameBox.Text = row.name;
					bindingSource.DataSource = LoadPrivileges(row.privileges);
					oldPrivileges = row.privileges;
					whiteListBox.Text = GetWhitelistString(row.whitelist);
					oldWhitelist = row.whitelist;
					readQuotaBox.Text = row.readQuota.ToString();
					oldReadQuota = row.readQuota;
					writeQuotaBox.Text = row.writeQuota.ToString();
					oldWriteQuota = row.writeQuota;
					break;
			}
			grid.DataSource = bindingSource;
		}

		public static string GetWhitelistString(List<string> list)
		{
			StringBuilder sb = new StringBuilder(256);
			bool comma = false;

			foreach (string wl in list)
			{
				if (comma)
				{
					sb.Append(',');
				}
				else
				{
					comma = true;
				}
				sb.Append(wl);
			}
			return sb.ToString();
		}

		private static BindingList<PrivilegeType> GetPrivilegeTypeBinding()
		{
			BindingList<PrivilegeType> bindingList = new BindingList<PrivilegeType>();
			bindingList.Add(new PrivilegeType(PrivilegeCode.READ, Role.Read));
			bindingList.Add(new PrivilegeType(PrivilegeCode.READ_WRITE, Role.ReadWrite));
			bindingList.Add(new PrivilegeType(PrivilegeCode.READ_WRITE_UDF, Role.ReadWriteUdf));
			bindingList.Add(new PrivilegeType(PrivilegeCode.WRITE, Role.Write));
			bindingList.Add(new PrivilegeType(PrivilegeCode.TRUNCATE, Role.Truncate));
			bindingList.Add(new PrivilegeType(PrivilegeCode.SYS_ADMIN, Role.SysAdmin));
			bindingList.Add(new PrivilegeType(PrivilegeCode.USER_ADMIN, Role.UserAdmin));
			bindingList.Add(new PrivilegeType(PrivilegeCode.DATA_ADMIN, Role.DataAdmin));
			bindingList.Add(new PrivilegeType(PrivilegeCode.UDF_ADMIN, Role.UDFAdmin));
			bindingList.Add(new PrivilegeType(PrivilegeCode.SINDEX_ADMIN, Role.SIndexAdmin));
			return bindingList;
		}

		private void CancelClicked(object sender, EventArgs e)
		{
			DialogResult = DialogResult.Cancel;
			this.Close();
		}

		private void SaveClicked(object sender, EventArgs e)
		{
			try
			{
				SaveRole();
				DialogResult = DialogResult.OK;
				this.Close();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void SaveRole()
		{
			BindingSource bindingSource = grid.DataSource as BindingSource;
			BindingList<Privilege> privileges = bindingSource.DataSource as BindingList<Privilege>;
			string name	= nameBox.Text.Trim();
			string[] whitelistArray = whiteListBox.Text.Trim().Split(',');
			List<string> whitelist = new List<string>(whitelistArray.Length);

			foreach (string wl in whitelistArray)
			{
				string s = wl.Trim();

				if (s.Length > 0)
				{
					whitelist.Add(s);
				}
			}

			int readQuota = int.Parse(readQuotaBox.Text);
			int writeQuota = int.Parse(writeQuotaBox.Text);

			switch (editType)
			{
				case EditType.CREATE:
					if (privileges.Count == 0 && whitelist.Count == 0 && readQuota == 0 && writeQuota == 0)
					{
						throw new AerospikeException("Privileges, whitelist, readQuota or writeQuota is required.");
					}
					client.CreateRole(null, name, privileges, whitelist, readQuota, writeQuota);
					break;

				case EditType.EDIT:
					ReplacePrivileges(name, privileges);
					ReplaceWhitelist(name, whitelist);
					ReplaceQuotas(name, readQuota, writeQuota);
					break;
			}
		}

		private BindingList<Privilege> LoadPrivileges(IList<Privilege> privileges)
		{
			BindingList<Privilege> list = new BindingList<Privilege>();

			foreach (Privilege privilege in privileges)
			{
				list.Add(privilege.Clone());
			}
			return list;
		}

		private void ReplacePrivileges(string name, IList<Privilege> privileges)
		{
			// Find grants.
			List<Privilege> grants = new List<Privilege>();

			foreach (Privilege privilege in privileges)
			{
				if (!FindRole(oldPrivileges, privilege))
				{
					grants.Add(privilege);
				}
			}

			// Find revokes.
			List<Privilege> revokes = new List<Privilege>();

			foreach (Privilege oldPrivilege in oldPrivileges)
			{
				if (!FindRole(privileges, oldPrivilege))
				{
					revokes.Add(oldPrivilege);
				}
			}

			if (grants.Count > 0)
			{
				client.GrantPrivileges(null, name, grants);
			}

			if (revokes.Count > 0)
			{
				client.RevokePrivileges(null, name, revokes);
			}
		}

		private static bool FindRole(IList<Privilege> privileges, Privilege search)
		{
			foreach (Privilege privilege in privileges)
			{
				if (privilege.Equals(search))
				{
					return true;
				}
			}
			return false;
		}

		private void ReplaceWhitelist(string name, IList<string> whitelist)
		{
			if (!IsWhiteListEqual(whitelist))
			{
				client.SetWhitelist(null, name, whitelist);
			}
		}

		private bool IsWhiteListEqual(IList<string> whitelist)
		{
			if (whitelist.Count != oldWhitelist.Count)
			{
				return false;
			}

			for (int i = 0; i < whitelist.Count; i++)
			{
				if (! whitelist[i].Equals(oldWhitelist[i]))
				{
					return false;
				}
			}
			return true;
		}

		private void ReplaceQuotas(string name, int readQuota, int writeQuota)
		{
			if (readQuota != oldReadQuota || writeQuota != oldWriteQuota)
			{
				client.SetQuotas(null, name, readQuota, writeQuota);
			}
		}

		public string RoleName { get { return nameBox.Text.Trim(); } }
	}

	class PrivilegeType
	{
		private PrivilegeCode code;
		private string label;

		public PrivilegeType(PrivilegeCode code, string label)
		{
			this.code = code;
			this.label = label;
		}

		public PrivilegeCode PrivilegeCode
		{
			get { return this.code; }
			set { this.code = value; }
		}

		public string Label
		{
			get { return this.label; }
			set { this.label = value; }
		}

		public override bool Equals(object obj)
		{
			if (obj == null)
			{
				return false;
			}

			if (obj == this)
			{
				return false;
			}
			PrivilegeType other = obj as PrivilegeType;

            if (other == null)
			{
				return false;
			}
			return this.code == other.code;
		}

		public override int GetHashCode()
		{
			return this.label.GetHashCode();
		}
	}
}
