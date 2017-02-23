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
using System.Security.Authentication;
using System.Text;
using System.Windows.Forms;
using Aerospike.Client;

namespace Aerospike.Admin
{
	public partial class LoginForm : Form
	{
		private string clusterName;
		private string tlsName;
		private TlsPolicy tlsPolicy;

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
			clusterName = Properties.Settings.Default.ClusterName.Trim();
			userBox.Text = Properties.Settings.Default.User;

			if (Properties.Settings.Default.TlsEnable)
			{
				tlsName = Properties.Settings.Default.TlsName.Trim();
				tlsPolicy = new TlsPolicy(
					Properties.Settings.Default.TlsProtocols,
					Properties.Settings.Default.TlsRevoke,
					Properties.Settings.Default.TlsClientCertFile
					);
			}
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
			int port = int.Parse(portBox.Text.Trim());
			Host[] hosts = Host.ParseHosts(hostBox.Text.Trim(), tlsName, port);
			string userName = userBox.Text.Trim();
			string password = passwordBox.Text.Trim();

			ClientPolicy policy = new ClientPolicy();
			policy.user = userName;
			policy.password = password;
			policy.clusterName = clusterName;
			policy.failIfNotConnected = true;
			policy.timeout = 600000;
			policy.tlsPolicy = tlsPolicy;

			AerospikeClient client = new AerospikeClient(policy, hosts);

			try
			{
				if (userName.Equals("admin") && password.Equals("admin"))
				{
					Form form = new PasswordForm(client, userName);
					form.ShowDialog();
				}

				// Query own user.
				User user = client.QueryUser(null, userName);				

				if (user != null)
				{
					bool admin = user.roles.Contains("user-admin");

					// Initialize Global Data
					Globals.RefreshRoles(client, user, admin);
					
					Form form = new AdminForm(client, user, admin);
					form.Show();
				}
				else
				{
					throw new Exception("Failed to find user: " + userName);
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
