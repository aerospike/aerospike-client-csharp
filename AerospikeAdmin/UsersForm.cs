/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
