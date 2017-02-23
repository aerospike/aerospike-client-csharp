/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
	public partial class RolesControl : UserControl
	{
		private AerospikeClient client;
		private User user;
		private bool admin;

		private BindingList<RoleRow> roles;
		private BindingSource bindingSourceRoles;
		private BindingSource bindingSourcePrivs;
		private ContextMenu rightClickMenuRoles;
		private List<string> rolesFilter;

		public RolesControl()
		{
			InitializeComponent();
		}

		public void Init(AerospikeClient client, User user, bool admin)
		{
			this.client = client;
			this.user = user;
			this.admin = admin;

			InitRightClickMenu(admin);

			if (!admin)
			{
				rolesFilter = user.roles;
				createMenuItem.Enabled = false;
				dropMenuItem.Enabled = false;
				editMenuItem.Enabled = false;
				createButton.Enabled = false;
			}

			// Initialize role grid
			gridRoles.AutoGenerateColumns = false;
			RoleNameColumn.DataPropertyName = "RoleName";

			bindingSourceRoles = new BindingSource();
			gridRoles.DataSource = bindingSourceRoles;

			// Initialize privilege grid
			gridPrivs.AutoGenerateColumns = false;
			PrivilegeCodeColumn.DataPropertyName = "CodeString";
			NamespaceColumn.DataPropertyName = "Namespace";
			SetNameColumn.DataPropertyName = "SetName";

			bindingSourcePrivs = new BindingSource();
			gridPrivs.DataSource = bindingSourcePrivs;

			// Load data
			ReadRoles();

			if (roles.Count > 0)
			{
				bindingSourcePrivs.DataSource = roles[0].privilegeRows;
			}
		}

		private void InitRightClickMenu(bool admin)
		{
			rightClickMenuRoles = new ContextMenu();

			if (admin)
			{
				MenuItem drop = new MenuItem("Drop Role");
				drop.Click += new System.EventHandler(this.DropClicked);
				rightClickMenuRoles.MenuItems.Add(drop);

				MenuItem edit = new MenuItem("Edit Role");
				edit.Click += new System.EventHandler(this.EditClicked);
				rightClickMenuRoles.MenuItems.Add(edit);
			}
		}

		private void ReadRoles()
		{
			List<Role> source;

			if (admin)
			{
				// Query all roles
				Globals.RefreshRoles(client, user, admin);
				source = Globals.GetCustomRoles();
			}
			else
			{
				// Query user's own roles.
				source = Globals.FindRoles(rolesFilter);
			}

			List<RoleRow> list = new List<RoleRow>(source.Count);

			foreach (Role role in source)
			{
				list.Add(new RoleRow(role));
			}
			roles = new BindingList<RoleRow>(list);
			bindingSourceRoles.DataSource = roles;
		}

		private void RoleRowEnter(object sender, DataGridViewCellEventArgs e)
		{
			RoleRow role = roles[e.RowIndex];

			if (bindingSourcePrivs.DataSource != role.privilegeRows)
			{
				bindingSourcePrivs.DataSource = role.privilegeRows;
			}
		}

		private void RoleClicked(object sender, MouseEventArgs e)
		{
			if (e.Button == MouseButtons.Right)
			{
				rightClickMenuRoles.Show(gridRoles, new Point(e.X, e.Y));
			}
		}

		private void CreateRoleClicked(object sender, EventArgs e)
		{
			RoleEditForm form = new RoleEditForm(client, EditType.CREATE, null);
			ShowRoleEditForm(form);
			SelectRole(form.RoleName);
		}

		private void SelectRole(string roleName)
		{
			// Use binary search to find row.
			int lower = 0;
			int upper = roles.Count - 1;
			int mid;
			int cmp;

			while (lower <= upper)
			{
				mid = (lower + upper) / 2;
				cmp = roles[mid].name.CompareTo(roleName);

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
					gridRoles.ClearSelection();
					gridRoles.Rows[mid].Selected = true;
					bindingSourcePrivs.DataSource = roles[mid].privilegeRows;
					break;
				}
			}
		}

		private void DropClicked(object sender, EventArgs e)
		{
			try
			{
				int row = gridRoles.CurrentRow.Index;

				if (row >= 0)
				{
					string name = roles[row].name;
					DialogResult result = MessageBox.Show("Drop role " + name + "?", "Confirm Drop", MessageBoxButtons.YesNo);

					if (result == DialogResult.Yes)
					{
						client.DropRole(null, name);
						ReadRoles();
					}
				}
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void EditClicked(object sender, EventArgs e)
		{
			try
			{
				int row = gridRoles.CurrentRow.Index;

				if (row >= 0)
				{
					RoleEditForm form = new RoleEditForm(client, EditType.EDIT, roles[row]);
					ShowRoleEditForm(form);
				}
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void ShowRoleEditForm(Form form)
		{
			if (form.ShowDialog() == DialogResult.OK)
			{
				ReadRoles();
			}
		}

		private void PrivSelectionChanged(object sender, EventArgs e)
		{
			gridPrivs.ClearSelection();
		}

		private void RefreshClicked(object sender, EventArgs e)
		{
			try
			{
				ReadRoles();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}
	}
}
