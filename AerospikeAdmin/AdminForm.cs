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
using System.ComponentModel;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Aerospike.Client;

namespace Aerospike.Admin
{
	public partial class AdminForm : Form
	{
		private AerospikeClient client;

		public AdminForm(AerospikeClient client, User user, bool admin)
		{
			this.client = client;
			InitializeComponent();
			this.Text += " by " + user.name;
			usersControl.Init(client, user, admin);
			rolesControl.Init(client, user, admin);
			rolesControl.Visible = false;

			TreeNode usersNode = new TreeNode("Users");

			TreeNode rootNode = new TreeNode("Manage", new TreeNode[] {
                    usersNode,
                    new TreeNode("Roles"),
                });

			sideView.Nodes.Add(rootNode);
			sideView.ExpandAll();
			sideView.SelectedNode = usersNode;
		}

		private void ExampleSelected(object sender, TreeViewEventArgs e)
		{
			try
			{
				TreeNode node = sideView.SelectedNode;

				if (node == null)
				{
					return;
				}

				if (node.Text.Equals("Users"))
				{
					usersControl.Visible = true;
					rolesControl.Visible = false;
				}
				else if (node.Text.Equals("Roles"))
				{
					rolesControl.Visible = true;
					usersControl.Visible = false;
				}
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
