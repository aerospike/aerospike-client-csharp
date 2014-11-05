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
	public partial class LoginForm : Form
	{
		public LoginForm()
		{
			InitializeComponent();
			FormInit();
		}

		private void FormInit()
		{
			try
			{
				//Log.SetCallback(LogCallback);
				//Log.SetLevel(Log.Level.DEBUG);
				ReadDefaults();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void FormClose(object sender, FormClosingEventArgs e)
		{
			try
			{
				WriteDefaults();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void ReadDefaults()
		{
			hostBox.Text = Properties.Settings.Default.Host;
			portBox.Text = Properties.Settings.Default.Port.ToString();
			userBox.Text = Properties.Settings.Default.User;
		}

		private void WriteDefaults()
		{
			Properties.Settings.Default.Host = hostBox.Text.Trim();
			Properties.Settings.Default.Port = int.Parse(portBox.Text);
			Properties.Settings.Default.User = userBox.Text.Trim();

			Properties.Settings.Default.Save();
		}

		private void LoginClicked(object sender, EventArgs e)
		{
			try
			{
				Login();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message, "Aerospike Login Error");
			}
		}

		private void Login()
		{
			string server = hostBox.Text.Trim();
			int port = int.Parse(portBox.Text.Trim());
			string user = userBox.Text.Trim();
			string password = passwordBox.Text.Trim();

			ClientPolicy policy = new ClientPolicy();
			policy.user = user;
			policy.password = password;
			policy.failIfNotConnected = true;
			policy.timeout = 600000;

			AerospikeClient client = new AerospikeClient(policy, server, port);

			try
			{
				if (user.Equals("admin") && password.Equals("admin"))
				{
					Form form = new PasswordForm(client, user);
					form.ShowDialog();
				}

				// Query own user.
				UserRoles userRoles = client.QueryUser(null, user);

				if (userRoles != null)
				{
					Form form = new UsersForm(client, userRoles);
					form.Show();
				}
				else
				{
					throw new Exception("Failed to find user: " + user);
				}
			}
			catch (Exception)
			{
				client.Close();
				throw;
			}
		}

		private void LogCallback(Log.Level level, string message)
		{
			MessageBox.Show(message, "Log Message");
		}

		private void KeyDownClicked(object sender, KeyEventArgs e)
		{
			if (e.KeyCode == Keys.Enter)
			{
				LoginClicked(sender, e);
			}
		}
	}
}
