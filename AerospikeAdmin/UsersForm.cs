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
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Aerospike.Client;

namespace Aerospike.Admin
{
	public partial class UsersForm : Form
	{
		private AerospikeClient client;
		private SortableBindingList<UserRow> users;
		private ContextMenu rightClickMenu;
		private readonly string userName;

		public UsersForm(AerospikeClient client, UserRoles user)
		{
			this.client = client;
			InitializeComponent();

			bool admin = user.roles.Contains("user-admin");
			FormInit(admin);

			if (! admin)
			{
				userName = user.user;
				createToolStripMenuItem.Enabled = false;
				dropToolStripMenuItem.Enabled = false;
				editRolesToolStripMenuItem.Enabled = false;
				createUserButton.Enabled = false;
			}
			ReadUsers(user);
		}

		private void FormInit(bool admin)
		{
			rightClickMenu = new ContextMenu();

			if (admin)
			{
				MenuItem drop = new MenuItem("Drop User");
				drop.Click += new System.EventHandler(this.DropUserClicked);
				rightClickMenu.MenuItems.Add(drop);
			}

			MenuItem editPass = new MenuItem("Change Password");
			editPass.Click += new System.EventHandler(this.ChangePasswordClicked);
			rightClickMenu.MenuItems.Add(editPass);

			if (admin)
			{
				MenuItem editRole = new MenuItem("Edit Roles");
				editRole.Click += new System.EventHandler(this.EditRolesClicked);
				rightClickMenu.MenuItems.Add(editRole);
			}
		}

		private void ReadUsers(UserRoles user)
		{
			List<UserRow> list;

			if (userName == null)
			{
				// Query all users
				List<UserRoles> source = client.QueryUsers(null);
				list = new List<UserRow>(source.Count);

				foreach (UserRoles userRoles in source)
				{
					UserRow row = new UserRow(userRoles);
					list.Add(row);
				}
			}
			else
			{
				// Query own user.
				if (user == null)
				{
					user = client.QueryUser(null, userName);
				}	
				list = new List<UserRow>(1);

				if (user != null)
				{
					UserRow row = new UserRow(user);
					list.Add(row);
				}
			}

			users = new SortableBindingList<UserRow>(list);
			grid.DataSource = users;
			grid.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.Fill;
			grid.AutoSizeRowsMode = DataGridViewAutoSizeRowsMode.AllCells;
			grid.Invalidate();
		}

		private void MouseClicked(object sender, MouseEventArgs e)
		{
			if (e.Button == MouseButtons.Right)
			{
				rightClickMenu.Show(grid, new Point(e.X, e.Y));
			}
		}

		private void CreateUserClicked(object sender, EventArgs e)
		{
			UserEditForm form = new UserEditForm(client, UserEditType.CREATE, null);
			ShowUserEditForm(form);			
		}

		private void DropUserClicked(object sender, EventArgs e)
		{
			try
			{
				int row = grid.CurrentCell.RowIndex;

				if (row >= 0)
				{
					string username = users[row].user;
					DialogResult result = MessageBox.Show("Delete user " + username + "?", "Confirm Delete", MessageBoxButtons.YesNo);

					if (result == DialogResult.Yes)
					{
						client.DropUser(null, users[row].user);
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
				int row = grid.CurrentCell.RowIndex;

				if (row >= 0)
				{
					Form form = new PasswordForm(client, users[row].user);
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
				int row = grid.CurrentCell.RowIndex;

				if (row >= 0)
				{
					UserEditForm form = new UserEditForm(client, UserEditType.EDIT_ROLES, users[row]);
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

		private void FormClose(object sender, FormClosingEventArgs e)
		{
			client.Close();
		}
	}
}
