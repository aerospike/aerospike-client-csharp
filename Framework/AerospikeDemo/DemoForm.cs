/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
using System.Text;
using System.Windows.Forms;
using System.IO;
using System.Security.Authentication;
using System.Text.RegularExpressions;
using System.Threading;
using Aerospike.Client;

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

                binTypeBox.Items.Add(BinType.Integer);
                binTypeBox.Items.Add(BinType.String);
                binTypeBox.Items.Add(BinType.Byte);
                binTypeBox.SelectedItem = BinType.Integer;

				replicaBox.Items.Add(Replica.SEQUENCE);
				replicaBox.Items.Add(Replica.MASTER);
				replicaBox.Items.Add(Replica.MASTER_PROLES);
				replicaBox.Items.Add(Replica.RANDOM);
				replicaBox.SelectedItem = Replica.SEQUENCE;

				readModeAPBox.Items.Add(ReadModeAP.ONE);
				readModeAPBox.Items.Add(ReadModeAP.ALL);
				readModeAPBox.SelectedItem = ReadModeAP.ONE;

				readModeSCBox.Items.Add(ReadModeSC.SESSION);
				readModeSCBox.Items.Add(ReadModeSC.LINEARIZE);
				readModeSCBox.Items.Add(ReadModeSC.ALLOW_REPLICA);
				readModeSCBox.Items.Add(ReadModeSC.ALLOW_UNAVAILABLE);
				readModeSCBox.SelectedItem = ReadModeSC.SESSION;
				
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
                    new ExampleTreeNode("Batch", new Batch(console)),
                    new ExampleTreeNode("Generation", new Generation(console)),
                    new ExampleTreeNode("Serialize", new Serialize(console)),
                    new ExampleTreeNode("Expire", new Expire(console)),
                    new ExampleTreeNode("Touch", new Touch(console)),
                    new ExampleTreeNode("Operate", new Operate(console)),
                    new ExampleTreeNode("OperateList", new OperateList(console)),
                    new ExampleTreeNode("OperateMap", new OperateMap(console)),
                    new ExampleTreeNode("Delete Bin", new DeleteBin(console)),
                    new ExampleTreeNode("Join", new GetAndJoin(console)),
                    new ExampleTreeNode("Scan Parallel", new ScanParallel(console)),
                    new ExampleTreeNode("Scan Series", new ScanSeries(console)),
                    new ExampleTreeNode("Async PutGet", new AsyncPutGet(console)),
                    new ExampleTreeNode("Async Batch", new AsyncBatch(console)),
                    new ExampleTreeNode("Async Scan", new AsyncScan(console)),
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
                    new ExampleTreeNode("Query PredExp", new QueryPredExp(console)),
                    new ExampleTreeNode("Query Sum", new QuerySum(console)),
                    new ExampleTreeNode("Query Average", new QueryAverage(console)),
                    new ExampleTreeNode("Query Execute", new QueryExecute(console)),
                    new ExampleTreeNode("Query Geo Collection", new QueryGeoCollection(console))
                });
                TreeNode benchmarks = new TreeNode("Benchmarks", new TreeNode[] {
                    new ExampleTreeNode("Initialize", new BenchmarkInitialize(console)),
                    new ExampleTreeNode("Read/Write", new BenchmarkReadWrite(console))
                });

                examplesView.Nodes.Add(examples);
                examplesView.Nodes.Add(benchmarks);
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
            syncButton.Checked = Properties.Settings.Default.Sync;
            asyncButton.Checked = !syncButton.Checked;
            syncThreadBox.Text = Properties.Settings.Default.SyncThreads.ToString();
            asyncThreadBox.Text = Properties.Settings.Default.AsyncThreads.ToString();
            maxCommandBox.Text = Properties.Settings.Default.AsyncMaxCommands.ToString();
            recordsBox.Text = Properties.Settings.Default.Records.ToString();
			batchReadBox.Checked = Properties.Settings.Default.BatchRead;
			batchSizeBox.Text = Properties.Settings.Default.BatchSize.ToString();
            binTypeBox.SelectedIndex = Properties.Settings.Default.BinType;
            binSizeBox.Text = Properties.Settings.Default.BinSize.ToString();
            fixedValueButton.Checked = Properties.Settings.Default.FixedValue;
            dynamicValueButton.Checked = !fixedValueButton.Checked;
            initPctBox.Text = Properties.Settings.Default.InitPct.ToString();
            readBox.Text = Properties.Settings.Default.ReadPct.ToString();
            writeBox.Text = Properties.Settings.Default.WritePct.ToString();
            timeoutBox.Text = Properties.Settings.Default.Timeout.ToString();
            maxRetriesBox.Text = Properties.Settings.Default.MaxRetries.ToString();
            sleepBox.Text = Properties.Settings.Default.SleepBetweenRetries.ToString();
			replicaBox.SelectedIndex = Properties.Settings.Default.Replica;
			latencyBox.Checked = Properties.Settings.Default.Latency;
			latencyAltFormatBox.Checked = Properties.Settings.Default.LatencyAltFormat;
			latencyColumnsBox.Text = Properties.Settings.Default.LatencyColumns.ToString();
            latencyShiftBox.Text = Properties.Settings.Default.LatencyShift.ToString();
            debugBox.Checked = Properties.Settings.Default.Debug;
			limitTpsBox.Checked = Properties.Settings.Default.LimitTPS;
			throughputBox.Text = Properties.Settings.Default.Throughput.ToString();
			authMode = (AuthMode)Enum.Parse(typeof(AuthMode), Properties.Settings.Default.AuthMode.Trim(), true);
			readModeAPBox.SelectedIndex = Properties.Settings.Default.ReadModeAP;
			readModeSCBox.SelectedIndex = Properties.Settings.Default.ReadModeSC;

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
            Properties.Settings.Default.Sync = syncButton.Checked;
            Properties.Settings.Default.SyncThreads = int.Parse(syncThreadBox.Text);
            Properties.Settings.Default.AsyncThreads = int.Parse(asyncThreadBox.Text);
            Properties.Settings.Default.AsyncMaxCommands = int.Parse(maxCommandBox.Text);
            Properties.Settings.Default.Records = int.Parse(recordsBox.Text);
			Properties.Settings.Default.BatchRead = batchReadBox.Checked;
			Properties.Settings.Default.BatchSize = int.Parse(batchSizeBox.Text);
			Properties.Settings.Default.BinType = binTypeBox.SelectedIndex;
            Properties.Settings.Default.BinSize = int.Parse(binSizeBox.Text);
            Properties.Settings.Default.FixedValue = fixedValueButton.Checked;
            Properties.Settings.Default.InitPct = int.Parse(initPctBox.Text);
            Properties.Settings.Default.ReadPct = int.Parse(readBox.Text);
            Properties.Settings.Default.WritePct = int.Parse(writeBox.Text);
            Properties.Settings.Default.Timeout = int.Parse(timeoutBox.Text);
            Properties.Settings.Default.MaxRetries = int.Parse(maxRetriesBox.Text);
            Properties.Settings.Default.SleepBetweenRetries = int.Parse(sleepBox.Text);
			Properties.Settings.Default.Replica = replicaBox.SelectedIndex;
			Properties.Settings.Default.Latency = latencyBox.Checked;
			Properties.Settings.Default.LatencyAltFormat = latencyAltFormatBox.Checked;
			Properties.Settings.Default.LatencyColumns = int.Parse(latencyColumnsBox.Text);
            Properties.Settings.Default.LatencyShift = int.Parse(latencyShiftBox.Text);
            Properties.Settings.Default.Debug = debugBox.Checked;
			Properties.Settings.Default.LimitTPS = limitTpsBox.Checked;
			Properties.Settings.Default.Throughput = int.Parse(throughputBox.Text);
			Properties.Settings.Default.ReadModeAP = readModeAPBox.SelectedIndex;
			Properties.Settings.Default.ReadModeSC = readModeSCBox.SelectedIndex;
			Properties.Settings.Default.Save();
        }

        private void RunExample(object sender, MouseEventArgs e)
        {
			try
            {
                TreeNode node = examplesView.SelectedNode;

                if (node == null || ! (node is ExampleTreeNode))
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
            Arguments args;

            if (currentExample.IsBenchmark())
            {
                BenchmarkArguments bargs = new BenchmarkArguments();
                bargs.sync = syncButton.Checked;

                if (bargs.sync)
                {
                    bargs.threadMax = int.Parse(syncThreadBox.Text);
                }
                else
                {
                    bargs.threadMax = int.Parse(asyncThreadBox.Text);
                }
                bargs.commandMax = int.Parse(maxCommandBox.Text);
                
                bargs.records = int.Parse(recordsBox.Text);

				if (batchReadBox.Checked)
				{
					bargs.batchSize = int.Parse(batchSizeBox.Text);
				}
                bargs.binType = (BinType)binTypeBox.SelectedItem;
                bargs.binSize = int.Parse(binSizeBox.Text);

                bargs.readPct = int.Parse(readBox.Text);
                int writePct = int.Parse(writeBox.Text);

                if (!(bargs.readPct >= 0 && bargs.readPct <= 100 &&
                    writePct >= 0 && writePct <= 100 &&
                    bargs.readPct + writePct == 100))
                {
                    throw new Exception("Read + Write percentage must equal 100");
                }

                int recordsInitPct = int.Parse(initPctBox.Text);
                bargs.recordsInit = bargs.records / 100 * recordsInitPct;

                if (fixedValueButton.Checked)
                {
                    bargs.SetFixedValue();
                }

                int timeout = int.Parse(timeoutBox.Text);
                int maxRetries = int.Parse(maxRetriesBox.Text);
                int sleepBetweenRetries = int.Parse(sleepBox.Text);
				Replica replica = (Replica)replicaBox.SelectedItem;
				ReadModeAP readModeAP = (ReadModeAP)readModeAPBox.SelectedItem;
				ReadModeSC readModeSC = (ReadModeSC)readModeSCBox.SelectedItem;

				bargs.policy.totalTimeout = timeout;
                bargs.policy.maxRetries = maxRetries;
                bargs.policy.sleepBetweenRetries = sleepBetweenRetries;
				bargs.policy.replica = replica;
				bargs.policy.readModeAP = readModeAP;
				bargs.policy.readModeSC = readModeSC;

				bargs.writePolicy.totalTimeout = timeout;
                bargs.writePolicy.maxRetries = maxRetries;
                bargs.writePolicy.sleepBetweenRetries = sleepBetweenRetries;
				bargs.writePolicy.replica = replica;

				bargs.batchPolicy.totalTimeout = timeout;
				bargs.batchPolicy.maxRetries = maxRetries;
				bargs.batchPolicy.sleepBetweenRetries = sleepBetweenRetries;
				bargs.batchPolicy.replica = replica;
				bargs.batchPolicy.readModeAP = readModeAP;
				bargs.batchPolicy.readModeSC = readModeSC;
				
				bargs.debug = debugBox.Checked;

				if (limitTpsBox.Checked)
				{
					bargs.throughput = int.Parse(throughputBox.Text);
				}
                
				bargs.latency = latencyBox.Checked;
				bargs.altLatencyFormat = latencyAltFormatBox.Checked;

                if (latencyBox.Checked)
                {
                    bargs.latencyColumns = int.Parse(latencyColumnsBox.Text);
                    bargs.latencyShift = int.Parse(latencyShiftBox.Text);

                    if (!(bargs.latencyColumns >= 2 && bargs.latencyColumns <= 10))
                    {
                        throw new Exception("Latency columns must be between 2 and 10 inclusive.");
                    }

                    if (!(bargs.latencyShift >= 1 && bargs.latencyShift <= 5))
                    {
                        throw new Exception("Latency exponent shift must be between 1 and 5 inclusive.");
                    }
                }

                args = bargs;
            }
            else
            {
                args = new Arguments();
                args.commandMax = 40;
            }

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

                if (example.IsBenchmark())
                {
                    if (example.IsBenchmarkInitialize())
                    {
                        initializePanel.Visible = true;
                        workloadPanel.Visible = false;
                    }
                    else
                    {
                        initializePanel.Visible = false;
                        workloadPanel.Visible = true;
                    }
                    benchmarkPanel.Visible = true;
                    codeBox.Visible = false;
                }
                else
                {
                    codeBox.Visible = true;
                    benchmarkPanel.Visible = false;
                    codeBox.Text = example.Read();
                    HighlightSourceCode();
                }
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

        private void SyncCheckChanged(object sender, EventArgs e)
        {
            if (syncButton.Checked)
            {
                threadPanel.Visible = true;
                maxCommandPanel.Visible = false;
            }
            else
            {
                maxCommandPanel.Visible = true;
                threadPanel.Visible = false;
            }
        }

		private void BatchReadChanged(object sender, EventArgs e)
		{
			if (batchReadBox.Checked)
			{
				batchSizeBox.Visible = true;
				batchSizeLabel.Visible = true;
			}
			else
			{
				batchSizeBox.Visible = false;
				batchSizeLabel.Visible = false;
			}
		}
		
		private void BinTypeChanged(object sender, EventArgs e)
        {
            if (binTypeBox.SelectedIndex == 0)
            {
                binSizeBox.Visible = false;
                binSizeLabel.Visible = false;
            }
            else
            {
                binSizeBox.Visible = true;
                binSizeLabel.Visible = true;
            }
        }

		private void LimitTPSChanged(object sender, EventArgs e)
		{
			if (limitTpsBox.Checked)
			{
				throughputBox.Visible = true;
			}
			else
			{
				throughputBox.Visible = false;
			}
		}
		
		private void LatencyChanged(object sender, EventArgs e)
        {
            if (latencyBox.Checked)
            {
                latencyGroup.Visible = true;
            }
            else
            {
                latencyGroup.Visible = false;
            }
        }

        private void LatencyValueChanged(object sender, EventArgs e)
        {
            try
            {
                if (latencyColumnsBox.Text.Length > 0 && latencyShiftBox.Text.Length > 0)
                {
                    int columns = int.Parse(latencyColumnsBox.Text);
                    int bitShift = int.Parse(latencyShiftBox.Text);
					latencyDisplayLabel.Text = latencyAltFormatBox.Checked ? 
						printLatencyLayoutAlt(columns, bitShift) : 
						printLatencyLayout(columns, bitShift);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private static string printLatencyLayout(int columns, int bitShift)
        {
            StringBuilder sb = new StringBuilder(200);
            int limit = 1;
            sb.Append("<=1ms >1ms");
            int i;
            int max = columns;

            for (i = 2; i < columns; i++)
            {
                limit <<= bitShift;
                String s = " >" + limit + "ms";
                sb.Append(s);
            }
            sb.AppendLine();

            sb.Append("   x%   x%");
            String val = "x%";
            int size = val.Length;
            limit = 1;

            for (i = 2; i < max; i++)
            {
                limit <<= bitShift;
                int spaces = limit.ToString().Length + 4 - size;

                for (int j = 0; j < spaces; j++)
                {
                    sb.Append(' ');
                }
                sb.Append(val);
            }
            sb.AppendLine();

            return sb.ToString();
        }

		private static string printLatencyLayoutAlt(int columns, int bitShift)
		{
			StringBuilder sb = new StringBuilder(200);
			int limit = 1;
			sb.Append("<=1ms(count,%) >1ms(count,%)");
			int i;
			int max = columns;

			for (i = 2; i < columns; i++)
			{
				limit <<= bitShift;
				String s = " >" + limit + "ms";
				sb.Append(s);
				sb.Append("(count,%)");
			}
			sb.AppendLine();
			return sb.ToString();
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

        public bool IsBenchmark()
        {
            return example is BenchmarkExample;
        }

        public bool IsBenchmarkInitialize()
        {
            return example is BenchmarkInitialize;
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
