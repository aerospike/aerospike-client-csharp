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
