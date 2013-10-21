using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
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
                hostBox.Text = Properties.Settings.Default.Host;
                portBox.Text = Properties.Settings.Default.Port.ToString();
                nsBox.Text = Properties.Settings.Default.Namespace;
                setBox.Text = Properties.Settings.Default.Set;

                codeBox.Font = new Font("Consolas", 10.0f);
				codeBox.SelectionTabs = new int[] { 25, 50, 75, 100, 125 };

                console = new ConsoleBox(consoleBox);

				Log.SetLevel(Log.Level.INFO);
				Log.SetCallback(LogCallback);

                TreeNode info = new ExampleTreeNode("Server Info", new ServerInfo(console));

                TreeNode examples = new TreeNode("Examples", new TreeNode[] {
                    info,
                    new ExampleTreeNode("Put/Get", new PutGet(console)),
                    new ExampleTreeNode("Add", new Add(console)),
                    new ExampleTreeNode("Append", new Append(console)),
                    new ExampleTreeNode("Prepend", new Prepend(console)),
                    new ExampleTreeNode("Batch", new Batch(console)),
                    new ExampleTreeNode("Generation", new Generation(console)),
                    new ExampleTreeNode("Serialize", new Serialize(console)),
                    new ExampleTreeNode("Expire", new Expire(console)),
                    new ExampleTreeNode("Touch", new Touch(console)),
                    new ExampleTreeNode("Delete Bin", new DeleteBin(console)),
                    new ExampleTreeNode("Scan Parallel", new ScanParallel(console)),
                    new ExampleTreeNode("Scan Series", new ScanSeries(console)),
                    new ExampleTreeNode("Async PutGet", new AsyncPutGet(console)),
                    new ExampleTreeNode("Async Batch", new AsyncBatch(console)),
                    new ExampleTreeNode("Async Scan", new AsyncScan(console)),
                    new ExampleTreeNode("List/Map", new ListMap(console)),
                    new ExampleTreeNode("User Defined Function", new UserDefinedFunction(console)),
                    new ExampleTreeNode("Large Set", new LargeSet(console)),
                    new ExampleTreeNode("Large Stack", new LargeStack(console)),
                    new ExampleTreeNode("Query Integer", new QueryInteger(console)),
                    new ExampleTreeNode("Query String", new QueryString(console)),
                    new ExampleTreeNode("Query Sum", new QuerySum(console)),
                    new ExampleTreeNode("Query Average", new QueryAverage(console)),
                    new ExampleTreeNode("Query Execute", new QueryExecute(console))
                });
                TreeNode benchmarks = new TreeNode("Benchmarks", new TreeNode[] {
                    new ExampleTreeNode("Linear Put/Get", new LinearPutGet(console)),
                    new ExampleTreeNode("Synchronous Load", new BenchmarkSync(console)),
                    new ExampleTreeNode("Asynchronous Load", new BenchmarkAsync(console))
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
                Properties.Settings.Default.Host = hostBox.Text.Trim();
                Properties.Settings.Default.Port = int.Parse(portBox.Text);
                Properties.Settings.Default.Namespace = nsBox.Text.Trim();
                Properties.Settings.Default.Set = setBox.Text.Trim();
                Properties.Settings.Default.Save();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
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
                thread = new Thread(RunExampleThread);
                thread.Start();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void RunExampleThread()
        {
            try
            {
                int port = int.Parse(portBox.Text);
				Arguments args = new Arguments(hostBox.Text.Trim(), port, nsBox.Text.Trim(), setBox.Text.Trim());
				args.SetServerSpecific();
				args.threadMax = int.Parse(threadBox.Text);
				args.commandMax = int.Parse(maxCommandBox.Text);
				currentExample.Run(args);
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

				if (example.Text.Equals("Synchronous Load"))
				{
					threadPanel.Visible = true;
					maxCommandPanel.Visible = false;
				}
				else if (example.Text.Equals("Asynchronous Load"))
				{
					threadPanel.Visible = false;
					maxCommandPanel.Visible = true;				
				}
				else
				{
					threadPanel.Visible = false;
					maxCommandPanel.Visible = false;
				}

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
			string path = @"..\..\..\" + example.GetType().Name + ".cs";
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
