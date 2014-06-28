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
