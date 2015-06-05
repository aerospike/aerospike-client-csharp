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
	public partial class PasswordForm : Form
	{
		private AerospikeClient client;

		public PasswordForm(AerospikeClient client, string user)
		{
			this.client = client;
			InitializeComponent();
			userBox.Text = user;
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
			string user = userBox.Text.Trim();
			string password = VerifyPassword();
			client.ChangePassword(null, user, password);
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
	}
}
