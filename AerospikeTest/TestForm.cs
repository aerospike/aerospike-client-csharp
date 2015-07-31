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
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace Aerospike.Test
{
	public partial class TestForm : Form
	{
		public TestForm()
		{
			InitializeComponent();
			FormInit();
		}

		private void FormInit()
		{
			try
			{
				ReadDefaults();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void ReadDefaults()
		{
			Args args = Args.Instance;

			hostBox.Text = args.host;
			portBox.Text = args.port.ToString();
			userBox.Text = args.user;
			nsBox.Text = args.ns;
			setBox.Text = args.set;
		}

		private void TestClicked(object sender, EventArgs e)
		{
			try
			{
				Connect();
				DialogResult = DialogResult.OK;
				this.Close();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void Connect()
		{
			Args args = Args.Instance;

			args.host = hostBox.Text.Trim();
			args.port = int.Parse(portBox.Text);
			args.user = userBox.Text.Trim();
			args.password = passwordBox.Text.Trim();
			args.ns = nsBox.Text.Trim();
			args.set = setBox.Text.Trim();

			args.Save();
			args.Connect();
		}

		private void KeyDownClick(object sender, KeyEventArgs e)
		{
			if (e.KeyCode == Keys.Enter)
			{
				runButton.PerformClick();
				e.SuppressKeyPress = true;
				e.Handled = true;
			}
		}
	}
}
