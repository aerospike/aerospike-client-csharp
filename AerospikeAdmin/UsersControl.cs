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
using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Windows.Forms;

namespace Aerospike.Admin
{
	public partial class UsersControl : UserControl
	{
		private AerospikeClient client;
		private BindingList<UserRow> users;
		private BindingSource bindingSourceUsers;
		private
			ContextMenuStrip rightClickMenuUsers;
		private string userNameFilter;

		public UsersControl()
		{
			InitializeComponent();
		}

		public void Init(AerospikeClient client, User user, bool admin)
		{
			this.client = client;
			InitRightClickMenu(admin);

			if (!admin)
			{
				userNameFilter = user.name;
				createUserButton.Enabled = false;
				createUserMenuItem.Enabled = false;
				dropUserMenuItem.Enabled = false;
				editUserMenuItem.Enabled = false;
			}

			// Initialize user grid
			gridUsers.AutoGenerateColumns = false;
			NameColumn.DataPropertyName = "UserName";
			RolesColumn.DataPropertyName = "RolesString";
			ConnsInUseColumn.DataPropertyName = "ConnsInUse";
			ReadQuotaColumn.DataPropertyName = "ReadQuota";
			ReadTpsColumn.DataPropertyName = "ReadTps";
			ReadQueryColumn.DataPropertyName = "ReadQuery";
			ReadLimitlessColumn.DataPropertyName = "ReadLimitless";
			WriteQuotaColumn.DataPropertyName = "WriteQuota";
			WriteTpsColumn.DataPropertyName = "WriteTps";
			WriteQueryColumn.DataPropertyName = "WriteQuery";
			WriteLimitlessColumn.DataPropertyName = "WriteLimitless";

			bindingSourceUsers = new BindingSource();
			gridUsers.DataSource = bindingSourceUsers;

			// Load data
			ReadUsers(user);
		}

		private void InitRightClickMenu(bool admin)
		{
			rightClickMenuUsers = new ContextMenuStrip();

			if (admin)
			{
				ToolStripMenuItem drop = new ToolStripMenuItem("Drop User");
				drop.Click += new System.EventHandler(this.DropUserClicked);
				rightClickMenuUsers.Items.Add(drop);
			}

			ToolStripMenuItem editPass = new ToolStripMenuItem("Change Password");
			editPass.Click += new System.EventHandler(this.ChangePasswordClicked);
			rightClickMenuUsers.Items.Add(editPass);

			if (admin)
			{
				ToolStripMenuItem editRole = new ToolStripMenuItem("Edit Roles");
				editRole.Click += new System.EventHandler(this.EditRolesClicked);
				rightClickMenuUsers.Items.Add(editRole);
			}
		}

		private void ReadUsers(User currentUser)
		{
			List<UserRow> list;

			if (userNameFilter == null)
			{
				// Query all users
				List<User> source = client.QueryUsers(null);
				list = new List<UserRow>(source.Count);

				foreach (User user in source)
				{
					list.Add(new UserRow(user));
				}
			}
			else
			{
				// Query own user.
				if (currentUser == null)
				{
					currentUser = client.QueryUser(null, userNameFilter);
				}
				list = new List<UserRow>(1);

				if (currentUser != null)
				{
					list.Add(new UserRow(currentUser));
				}
			}

			users = new BindingList<UserRow>(list);
			bindingSourceUsers.DataSource = users;
		}

		private void UserClicked(object sender, MouseEventArgs e)
		{
			if (e.Button == MouseButtons.Right)
			{
				rightClickMenuUsers.Show(gridUsers, new Point(e.X, e.Y));
			}
		}

		private void CreateUserClicked(object sender, EventArgs e)
		{
			UserEditForm form = new UserEditForm(client, EditType.CREATE, null);
			ShowUserEditForm(form);
			SelectUser(form.UserName);
		}

		private void SelectUser(string userName)
		{
			// Use binary search to find row.
			int lower = 0;
			int upper = users.Count - 1;
			int mid;
			int cmp;

			while (lower <= upper)
			{
				mid = (lower + upper) / 2;
				cmp = users[mid].name.CompareTo(userName);

				if (cmp < 0)
				{
					lower = mid + 1;
					continue;
				}
				else if (cmp > 0)
				{
					upper = mid - 1;
					continue;
				}
				else
				{
					// Found user. Make current.
					gridUsers.ClearSelection();
					gridUsers.Rows[mid].Selected = true;
					break;
				}
			}
		}

		private void DropUserClicked(object sender, EventArgs e)
		{
			try
			{
				int row = gridUsers.CurrentRow.Index;

				if (row >= 0)
				{
					string username = users[row].name;
					DialogResult result = MessageBox.Show("Drop user " + username + "?", "Confirm Drop", MessageBoxButtons.YesNo);

					if (result == DialogResult.Yes)
					{
						client.DropUser(null, username);
						ReadUsers(null);
					}
				}
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void ChangePasswordClicked(object sender, EventArgs e)
		{
			try
			{
				int row = gridUsers.CurrentRow.Index;

				if (row >= 0)
				{
					Form form = new PasswordForm(client, users[row].name);
					ShowUserEditForm(form);
				}
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void EditRolesClicked(object sender, EventArgs e)
		{
			try
			{
				int row = gridUsers.CurrentRow.Index;

				if (row >= 0)
				{
					UserEditForm form = new UserEditForm(client, EditType.EDIT, users[row]);
					ShowUserEditForm(form);
				}
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void ShowUserEditForm(Form form)
		{
			if (form.ShowDialog() == DialogResult.OK)
			{
				ReadUsers(null);
			}
		}

		private void RefreshClicked(object sender, EventArgs e)
		{
			try
			{
				ReadUsers(null);
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}
	}
}
