/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using Aerospike.Client;
using System;
using System.Drawing;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Forms;

namespace Aerospike.Demo
{
	public partial class DemoForm : Form
	{
		public static readonly string RelativeDirectory = FindRelativeDirectory();

		private Thread thread;
		private volatile ExampleTreeNode currentExample;
		private Console console;
		private string clusterName;
		private string tlsName;
		private TlsPolicy tlsPolicy;
		private AuthMode authMode;

		public DemoForm()
		{
			InitializeComponent();
			FormInit();
		}

		private void FormInit()
		{
			try
			{
				codeBox.Font = new Font("Consolas", 10.0f);
				codeBox.SelectionTabs = new int[] { 25, 50, 75, 100, 125 };

				ReadDefaults();

				console = new ConsoleBox(consoleBox);
				Log.SetLevel(Log.Level.INFO);
				Log.SetCallback(LogCallback);

				TreeNode info = new ExampleTreeNode("Server Info", new ServerInfo(console));

				TreeNode examples = new TreeNode("Examples", new TreeNode[] {
					info,
					new ExampleTreeNode("Put/Get", new PutGet(console)),
					new ExampleTreeNode("Replace", new Replace(console)),
					new ExampleTreeNode("Add", new Add(console)),
					new ExampleTreeNode("Append", new Append(console)),
					new ExampleTreeNode("Prepend", new Prepend(console)),
					new ExampleTreeNode("Batch Read", new Batch(console)),
					new ExampleTreeNode("Batch Operate", new BatchOperate(console)),
					new ExampleTreeNode("Generation", new Generation(console)),
#if BINARY_FORMATTER
                    new ExampleTreeNode("Serialize", new Serialize(console)),
#endif   
                    new ExampleTreeNode("Expire", new Expire(console)),
					new ExampleTreeNode("Touch", new Touch(console)),
					new ExampleTreeNode("Transaction", new Transaction(console)),
					new ExampleTreeNode("Operate", new Operate(console)),
					new ExampleTreeNode("OperateBit", new OperateBit(console)),
					new ExampleTreeNode("OperateList", new OperateList(console)),
					new ExampleTreeNode("OperateMap", new OperateMap(console)),
					new ExampleTreeNode("Delete Bin", new DeleteBin(console)),
					new ExampleTreeNode("Join", new GetAndJoin(console)),
					new ExampleTreeNode("Scan Parallel", new ScanParallel(console)),
					new ExampleTreeNode("Scan Series", new ScanSeries(console)),
					new ExampleTreeNode("Scan Page", new ScanPage(console)),
					new ExampleTreeNode("Scan Resume", new ScanResume(console)),
					new ExampleTreeNode("Async PutGet", new AsyncPutGet(console)),
					new ExampleTreeNode("Async Batch", new AsyncBatch(console)),
					new ExampleTreeNode("Async Scan", new AsyncScan(console)),
					new ExampleTreeNode("Async Scan Page", new AsyncScanPage(console)),
					new ExampleTreeNode("Async Transaction", new AsyncTransaction(console)),
					new ExampleTreeNode("Async Transaction with Task", new AsyncTransactionWithTask(console)),
					new ExampleTreeNode("Async Query", new AsyncQuery(console)),
					new ExampleTreeNode("Async UDF", new AsyncUserDefinedFunction(console)),
					new ExampleTreeNode("List/Map", new ListMap(console)),
					new ExampleTreeNode("User Defined Function", new UserDefinedFunction(console)),
					new ExampleTreeNode("Query Integer", new QueryInteger(console)),
					new ExampleTreeNode("Query String", new QueryString(console)),
					new ExampleTreeNode("Query List", new QueryList(console)),
					new ExampleTreeNode("Query Region", new QueryRegion(console)),
					new ExampleTreeNode("Query Region Filter", new QueryRegionFilter(console)),
					new ExampleTreeNode("Query Filter", new QueryFilter(console)),
					new ExampleTreeNode("Query Expression", new QueryExp(console)),
					new ExampleTreeNode("Query Page", new QueryPage(console)),
					new ExampleTreeNode("Query Resume", new QueryResume(console)),
					new ExampleTreeNode("Query Sum", new QuerySum(console)),
					new ExampleTreeNode("Query Average", new QueryAverage(console)),
					new ExampleTreeNode("Query Execute", new QueryExecute(console)),
					new ExampleTreeNode("Query Geo Collection", new QueryGeoCollection(console))
				});

				examplesView.Nodes.Add(examples);
				examplesView.SelectedNode = info;
				examplesView.ExpandAll();
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
			nsBox.Text = Properties.Settings.Default.Namespace;
			setBox.Text = Properties.Settings.Default.Set;
			authMode = (AuthMode)Enum.Parse(typeof(AuthMode), Properties.Settings.Default.AuthMode.Trim(), true);

			if (Properties.Settings.Default.TlsEnable)
			{
				tlsName = Properties.Settings.Default.TlsName.Trim();
				tlsPolicy = new TlsPolicy(
					Properties.Settings.Default.TlsProtocols,
					Properties.Settings.Default.TlsRevoke,
					Properties.Settings.Default.TlsClientCertFile,
					Properties.Settings.Default.TlsLoginOnly
					);
			}
		}

		private void WriteDefaults()
		{
			Properties.Settings.Default.Host = hostBox.Text.Trim();
			Properties.Settings.Default.Port = int.Parse(portBox.Text);
			Properties.Settings.Default.Namespace = nsBox.Text.Trim();
			Properties.Settings.Default.Set = setBox.Text.Trim();
			Properties.Settings.Default.Save();
		}

		private void RunExample(object sender, MouseEventArgs e)
		{
			try
			{
				TreeNode node = examplesView.SelectedNode;

				if (node == null || !(node is ExampleTreeNode))
					throw new Exception("Please select an example program.");

				if (currentExample != null)
					throw new Exception("Stop the current example before running a new example.");

				currentExample = (ExampleTreeNode)node;
				Arguments args = ParseArguments();
				thread = new Thread(RunExampleThread);
				thread.Start(args);
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private Arguments ParseArguments()
		{
			Arguments args = new Arguments();
			args.commandMax = 40;
			args.port = int.Parse(portBox.Text);
			args.hosts = Host.ParseHosts(hostBox.Text.Trim(), tlsName, args.port);
			args.user = userBox.Text.Trim();
			args.password = passwordBox.Text;
			args.clusterName = clusterName;
			args.ns = nsBox.Text.Trim();
			args.set = setBox.Text.Trim();
			args.tlsPolicy = tlsPolicy;
			args.authMode = authMode;
			return args;
		}

		private void RunExampleThread(object data)
		{
			try
			{
				currentExample.Run((Arguments)data);
			}
			catch (Exception ex)
			{
				console.Error(Util.GetErrorMessage(ex));
				//MessageBox.Show(ex.Message);
			}
			finally
			{
				currentExample = null;
			}
		}

		private void StopExample(object sender, EventArgs e)
		{
			try
			{
				if (currentExample != null)
				{
					currentExample.Stop();
				}
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		private void ExampleSelected(object sender, TreeViewEventArgs e)
		{
			try
			{
				TreeNode node = examplesView.SelectedNode;

				if (node == null || !(node is ExampleTreeNode))
					return;

				codeBox.Clear();
				ExampleTreeNode example = (ExampleTreeNode)node;

				codeBox.Visible = true;
				codeBox.Text = example.Read();
				HighlightSourceCode();
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message);
			}
		}

		/// <summary>
		/// Extremely simple syntax highlighter.
		/// </summary>
		private void HighlightSourceCode()
		{
			string tokens = @"(using|namespace|class|public|base|override|void|string|int|bool|\stry\s|null|if|throw|new|\sobject\s|uint|out|else|catch|finally|private|foreach|\sin\s)";
			HighlightSegments(tokens, Color.Blue);

			tokens = "(\".*\"|'.?')";
			HighlightSegments(tokens, Color.DarkRed);

			tokens = "//.*";
			HighlightSegments(tokens, Color.Green);

			int offset = codeBox.Text.IndexOf("RunExample");
			codeBox.SelectionStart = offset;
			codeBox.ScrollToCaret();
			codeBox.Refresh();
		}

		private void HighlightSegments(string tokens, Color color)
		{
			Regex regex = new Regex(tokens);
			MatchCollection mc = regex.Matches(codeBox.Text);

			foreach (Match m in mc)
			{
				int startIndex = m.Index;
				int stopIndex = m.Length;
				codeBox.Select(startIndex, stopIndex);
				codeBox.SelectionColor = color;
			}
		}

		private void LogCallback(Log.Level level, string message)
		{
			console.Write(level, message);
		}

		private void ConsoleKeyDown(object sender, KeyEventArgs e)
		{
			if (e.Control && e.KeyCode == Keys.A)
			{
				consoleBox.SelectAll();
				e.Handled = true;
			}
		}

		private void PercentKeyDown(object sender, KeyEventArgs e)
		{
			// Allow digits and arrow keys only
			e.SuppressKeyPress = !((e.KeyValue >= 48 && e.KeyValue <= 57)
				|| e.KeyValue == 127 || e.KeyValue == 8 || e.KeyValue == 37 || e.KeyValue == 39);
		}

		private static string FindRelativeDirectory()
		{
			// Look for "udf" directory as relative path from executable.
			// First try relative path from AnyCPU executable default location.
			string dirname = "udf";
			string orig = ".." + Path.DirectorySeparatorChar + ".." + Path.DirectorySeparatorChar;
			string path = orig;

			for (int i = 0; i < 8; i++)
			{
				if (Directory.Exists(path + dirname))
				{
					// udf directory found.  Use corresponding directory path. 
					return path;
				}

				// Try next level up.
				path += ".." + Path.DirectorySeparatorChar;
			}

			// Failed to find path.  Just return original directory which means source code and lua files
			// can't be accessed.  Program can still run though.
			return orig;
		}
	}

	class ExampleTreeNode : TreeNode
	{
		private Example example;

		public ExampleTreeNode(String text, Example example)
			: base(text)
		{
			this.example = example;
		}

		public string Read()
		{
			string path = DemoForm.RelativeDirectory + example.GetType().Name + ".cs";
			return File.ReadAllText(path);
		}

		public void Run(Arguments args)
		{
			example.Run(args);
		}

		public void Stop()
		{
			Log.Info("Stop requested. Waiting for thread to end.");
			example.Stop();
		}
	}
}
