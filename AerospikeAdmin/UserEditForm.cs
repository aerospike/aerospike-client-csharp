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
using System.ComponentModel;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Aerospike.Client;

namespace Aerospike.Admin
{
	public enum UserEditType
	{
		CREATE,
		EDIT_ROLES
	}

	public partial class UserEditForm : Form
	{
		private AerospikeClient client;
		private UserEditType editType;

		public UserEditForm(AerospikeClient client, UserEditType editType, UserRow row)
		{
			this.client = client;
			this.editType = editType;

			InitializeComponent();

			switch (editType)
			{
				case UserEditType.CREATE:
					break;

				case UserEditType.EDIT_ROLES:
					this.Text = "Edit Roles";
					userBox.Enabled = false;
					userBox.Text = row.user;
					passwordBox.Enabled = false;
					passwordVerifyBox.Enabled = false;
					SetRoles(row.roles);
					break;
			}
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
				SaveUser();
				DialogResult = DialogResult.OK;
				this.Close();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void SaveUser()
		{
			string user;
			string password;
			List<string> roles;

			switch (editType)
			{
				case UserEditType.CREATE:
					user = userBox.Text.Trim();
					password = VerifyPassword();
					roles = GetRoles();
					client.CreateUser(null, user, password, roles);
					break;

				case UserEditType.EDIT_ROLES:
					user = userBox.Text.Trim();
					roles = GetRoles();
					client.ReplaceRoles(null, user, roles);
					break;
			}
		}

		private string VerifyPassword()
		{
			string password = passwordBox.Text.Trim();
			string passwordVerify = passwordVerifyBox.Text.Trim();

			if (password.Length < 5 || password.Length > 30)
			{
				throw new Exception("Password must be between 5 and 30 characters in length.");
			}

			if (! password.Equals(passwordVerify))
			{
				throw new Exception("Passwords do not match.");
			}
			return password;
		}

		private void SetRoles(List<string> roles)
		{
			int max = rolesBox.Items.Count;

			foreach (string role in roles)
			{
				for (int i = 0; i < max; i++)
				{
					string text = rolesBox.GetItemText(rolesBox.Items[i]);

					if (text.Equals(role))
					{
						rolesBox.SetItemChecked(i, true);
						break;
					}
				}
			}
		}

		private List<string> GetRoles()
		{
			List<string> list = new List<string>();
			int max = rolesBox.Items.Count;

			for (int i = 0; i < max; i++)
			{
				if (rolesBox.GetItemChecked(i))
				{
					string text = rolesBox.GetItemText(rolesBox.Items[i]);
					list.Add(text);
				}
			}
			return list;
		}
	}
}
