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
using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Windows.Forms;

namespace Aerospike.Admin
{
	public partial class UserEditForm : Form
	{
		private readonly AerospikeClient client;
		private readonly EditType editType;
		private readonly List<string> oldRoles;

		public UserEditForm(AerospikeClient client, EditType editType, UserRow user)
		{
			this.client = client;
			this.editType = editType;

			InitializeComponent();

			switch (editType)
			{
				case EditType.CREATE:
					SetRoles(null);
					break;

				case EditType.EDIT:
					this.Text = "Edit User Roles";
					userBox.Enabled = false;
					userBox.Text = user.name;
					passwordBox.Enabled = false;
					passwordVerifyBox.Enabled = false;
					SetRoles(user.roles);
					oldRoles = user.roles;
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
				case EditType.CREATE:
					user = userBox.Text.Trim();
					password = VerifyPassword();
					roles = GetRoles();
					client.CreateUser(null, user, password, roles);
					break;

				case EditType.EDIT:
					user = userBox.Text.Trim();
					roles = GetRoles();
					ReplaceRoles(user, roles);
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

			if (!password.Equals(passwordVerify))
			{
				throw new Exception("Passwords do not match.");
			}
			return password;
		}

		private void SetRoles(List<string> rolesUser)
		{
			List<Role> rolesAll = Globals.GetAllRoles();

			if (rolesUser != null)
			{
				foreach (Role role in rolesAll)
				{
					bool found = FindRole(rolesUser, role.name);
					rolesBox.Items.Add(role.name, found);
				}
			}
			else
			{
				foreach (Role role in rolesAll)
				{
					rolesBox.Items.Add(role.name, false);
				}
			}

			int height = rolesBox.GetItemRectangle(0).Height * rolesBox.Items.Count;

			if (height > 600)
			{
				height = 600;
			}
			this.Height += height - rolesBox.ClientSize.Height;
			//rolesBox.ClientSize = new Size(rolesBox.ClientSize.Width, height);
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

		private void ReplaceRoles(string user, List<string> roles)
		{
			// Find grants.
			List<string> grantRoles = new List<string>();

			foreach (string role in roles)
			{
				if (!FindRole(oldRoles, role))
				{
					grantRoles.Add(role);
				}
			}

			// Find revokes.
			List<string> revokeRoles = new List<string>();

			foreach (string oldRole in oldRoles)
			{
				if (!FindRole(roles, oldRole))
				{
					revokeRoles.Add(oldRole);
				}
			}

			if (grantRoles.Count > 0)
			{
				client.GrantRoles(null, user, grantRoles);
			}

			if (revokeRoles.Count > 0)
			{
				client.RevokeRoles(null, user, revokeRoles);
			}
		}

		private static bool FindRole(List<string> roles, string search)
		{
			foreach (string role in roles)
			{
				if (role.Equals(search))
				{
					return true;
				}
			}
			return false;
		}

		public string UserName { get { return userBox.Text.Trim(); } }
	}
}
