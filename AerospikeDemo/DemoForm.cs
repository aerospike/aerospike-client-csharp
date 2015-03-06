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
using System.Text;
using System.Windows.Forms;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using Aerospike.Client;

namespace Aerospike.Demo
{
    public partial class DemoForm : Form
    {
        private Thread thread;
        private volatile ExampleTreeNode currentExample;
        private Console console;

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
                    new ExampleTreeNode("Delete Bin", new DeleteBin(console)),
                    new ExampleTreeNode("Join", new GetAndJoin(console)),
                    new ExampleTreeNode("Scan Parallel", new ScanParallel(console)),
                    new ExampleTreeNode("Scan Series", new ScanSeries(console)),
                    new ExampleTreeNode("Async PutGet", new AsyncPutGet(console)),
                    new ExampleTreeNode("Async Batch", new AsyncBatch(console)),
                    new ExampleTreeNode("Async Scan", new AsyncScan(console)),
                    new ExampleTreeNode("List/Map", new ListMap(console)),
                    new ExampleTreeNode("User Defined Function", new UserDefinedFunction(console)),
                    new ExampleTreeNode("Large List", new LargeList(console)),
                    new ExampleTreeNode("Large Set", new LargeSet(console)),
                    new ExampleTreeNode("Large Stack", new LargeStack(console)),
                    new ExampleTreeNode("Query Integer", new QueryInteger(console)),
                    new ExampleTreeNode("Query String", new QueryString(console)),
                    #if (! LITE)
                    new ExampleTreeNode("Query Filter", new QueryFilter(console)),
                    new ExampleTreeNode("Query Sum", new QuerySum(console)),
                    new ExampleTreeNode("Query Average", new QueryAverage(console)),
                    #endif
                    new ExampleTreeNode("Query Execute", new QueryExecute(console))
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
            nsBox.Text = Properties.Settings.Default.Namespace;
            setBox.Text = Properties.Settings.Default.Set;
            syncButton.Checked = Properties.Settings.Default.Sync;
            asyncButton.Checked = !syncButton.Checked;
            syncThreadBox.Text = Properties.Settings.Default.SyncThreads.ToString();
            asyncThreadBox.Text = Properties.Settings.Default.AsyncThreads.ToString();
            maxCommandBox.Text = Properties.Settings.Default.AsyncMaxCommands.ToString();
            recordsBox.Text = Properties.Settings.Default.Records.ToString();
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
            latencyBox.Checked = Properties.Settings.Default.Latency;
            latencyColumnsBox.Text = Properties.Settings.Default.LatencyColumns.ToString();
            latencyShiftBox.Text = Properties.Settings.Default.LatencyShift.ToString();
            debugBox.Checked = Properties.Settings.Default.Debug;
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
            Properties.Settings.Default.BinType = binTypeBox.SelectedIndex;
            Properties.Settings.Default.BinSize = int.Parse(binSizeBox.Text);
            Properties.Settings.Default.FixedValue = fixedValueButton.Checked;
            Properties.Settings.Default.InitPct = int.Parse(initPctBox.Text);
            Properties.Settings.Default.ReadPct = int.Parse(readBox.Text);
            Properties.Settings.Default.WritePct = int.Parse(writeBox.Text);
            Properties.Settings.Default.Timeout = int.Parse(timeoutBox.Text);
            Properties.Settings.Default.MaxRetries = int.Parse(maxRetriesBox.Text);
            Properties.Settings.Default.SleepBetweenRetries = int.Parse(sleepBox.Text);
            Properties.Settings.Default.Latency = latencyBox.Checked;
            Properties.Settings.Default.LatencyColumns = int.Parse(latencyColumnsBox.Text);
            Properties.Settings.Default.LatencyShift = int.Parse(latencyShiftBox.Text);
            Properties.Settings.Default.Debug = debugBox.Checked;

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
                currentExample = null;
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

                bargs.policy.timeout = timeout;
                bargs.policy.maxRetries = maxRetries;
                bargs.policy.sleepBetweenRetries = sleepBetweenRetries;

                bargs.writePolicy.timeout = timeout;
                bargs.writePolicy.maxRetries = maxRetries;
                bargs.writePolicy.sleepBetweenRetries = sleepBetweenRetries;

                bargs.debug = debugBox.Checked;
                bargs.latency = latencyBox.Checked;

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

            args.host = hostBox.Text.Trim();
            args.port = int.Parse(portBox.Text);
            args.user = userBox.Text.Trim();
            args.password = passwordBox.Text;
            args.ns = nsBox.Text.Trim();
            args.set = setBox.Text.Trim();
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
                MessageBox.Show(ex.Message);
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
                    latencyDisplayLabel.Text = printLatencyLayout(columns, bitShift);
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

                if (sb.Length >= 65)
                {
                    max = i + 1;
                    break;
                }
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

        private void PercentKeyDown(object sender, KeyEventArgs e)
        {
            // Allow digits and arrow keys only
            e.SuppressKeyPress = !((e.KeyValue >= 48 && e.KeyValue <= 57)
                || e.KeyValue == 127 || e.KeyValue == 8 || e.KeyValue == 37 || e.KeyValue == 39);
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
            // Adjust path for whether using x64/x86 or AnyCPU compile target.
            string filename = example.GetType().Name + ".cs";

            //  Look for source files up directory tree
            string path = ".." + Path.DirectorySeparatorChar + ".." + Path.DirectorySeparatorChar;
            int i=8;
            do 
            {
                if (File.Exists(path+filename)) break;
                path += ".." + Path.DirectorySeparatorChar;
                i--;
            } while (i > 0);

            return File.ReadAllText(path+filename);
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
