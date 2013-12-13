namespace Aerospike.Demo
{
    partial class DemoForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
			this.splitContainer1 = new System.Windows.Forms.SplitContainer();
			this.codeBox = new System.Windows.Forms.RichTextBox();
			this.maxCommandPanel = new System.Windows.Forms.Panel();
			this.maxCommandBox = new System.Windows.Forms.TextBox();
			this.label6 = new System.Windows.Forms.Label();
			this.threadPanel = new System.Windows.Forms.Panel();
			this.threadBox = new System.Windows.Forms.TextBox();
			this.label5 = new System.Windows.Forms.Label();
			this.consoleBox = new System.Windows.Forms.TextBox();
			this.stopButton = new System.Windows.Forms.Button();
			this.runButton = new System.Windows.Forms.Button();
			this.nsBox = new System.Windows.Forms.TextBox();
			this.label3 = new System.Windows.Forms.Label();
			this.portBox = new System.Windows.Forms.TextBox();
			this.label2 = new System.Windows.Forms.Label();
			this.label1 = new System.Windows.Forms.Label();
			this.hostBox = new System.Windows.Forms.TextBox();
			this.setBox = new System.Windows.Forms.TextBox();
			this.label4 = new System.Windows.Forms.Label();
			this.examplesView = new System.Windows.Forms.TreeView();
			((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).BeginInit();
			this.splitContainer1.Panel1.SuspendLayout();
			this.splitContainer1.Panel2.SuspendLayout();
			this.splitContainer1.SuspendLayout();
			this.maxCommandPanel.SuspendLayout();
			this.threadPanel.SuspendLayout();
			this.SuspendLayout();
			// 
			// splitContainer1
			// 
			this.splitContainer1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.splitContainer1.Location = new System.Drawing.Point(155, 32);
			this.splitContainer1.Name = "splitContainer1";
			this.splitContainer1.Orientation = System.Windows.Forms.Orientation.Horizontal;
			// 
			// splitContainer1.Panel1
			// 
			this.splitContainer1.Panel1.Controls.Add(this.codeBox);
			// 
			// splitContainer1.Panel2
			// 
			this.splitContainer1.Panel2.Controls.Add(this.maxCommandPanel);
			this.splitContainer1.Panel2.Controls.Add(this.threadPanel);
			this.splitContainer1.Panel2.Controls.Add(this.consoleBox);
			this.splitContainer1.Panel2.Controls.Add(this.stopButton);
			this.splitContainer1.Panel2.Controls.Add(this.runButton);
			this.splitContainer1.Size = new System.Drawing.Size(879, 753);
			this.splitContainer1.SplitterDistance = 496;
			this.splitContainer1.TabIndex = 0;
			// 
			// codeBox
			// 
			this.codeBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.codeBox.BackColor = System.Drawing.SystemColors.Window;
			this.codeBox.Location = new System.Drawing.Point(0, 0);
			this.codeBox.Name = "codeBox";
			this.codeBox.ReadOnly = true;
			this.codeBox.Size = new System.Drawing.Size(876, 501);
			this.codeBox.TabIndex = 8;
			this.codeBox.Text = "";
			// 
			// maxCommandPanel
			// 
			this.maxCommandPanel.Controls.Add(this.maxCommandBox);
			this.maxCommandPanel.Controls.Add(this.label6);
			this.maxCommandPanel.Location = new System.Drawing.Point(177, 4);
			this.maxCommandPanel.Name = "maxCommandPanel";
			this.maxCommandPanel.Size = new System.Drawing.Size(213, 26);
			this.maxCommandPanel.TabIndex = 17;
			this.maxCommandPanel.Visible = false;
			// 
			// maxCommandBox
			// 
			this.maxCommandBox.Location = new System.Drawing.Point(147, 3);
			this.maxCommandBox.Name = "maxCommandBox";
			this.maxCommandBox.Size = new System.Drawing.Size(50, 20);
			this.maxCommandBox.TabIndex = 14;
			this.maxCommandBox.Text = "40";
			// 
			// label6
			// 
			this.label6.AutoSize = true;
			this.label6.Location = new System.Drawing.Point(4, 6);
			this.label6.Name = "label6";
			this.label6.Size = new System.Drawing.Size(137, 13);
			this.label6.TabIndex = 15;
			this.label6.Text = "Max Concurrent Commands";
			// 
			// threadPanel
			// 
			this.threadPanel.Controls.Add(this.threadBox);
			this.threadPanel.Controls.Add(this.label5);
			this.threadPanel.Location = new System.Drawing.Point(177, 4);
			this.threadPanel.Name = "threadPanel";
			this.threadPanel.Size = new System.Drawing.Size(115, 26);
			this.threadPanel.TabIndex = 16;
			this.threadPanel.Visible = false;
			// 
			// threadBox
			// 
			this.threadBox.Location = new System.Drawing.Point(52, 3);
			this.threadBox.Name = "threadBox";
			this.threadBox.Size = new System.Drawing.Size(50, 20);
			this.threadBox.TabIndex = 14;
			this.threadBox.Text = "8";
			// 
			// label5
			// 
			this.label5.AutoSize = true;
			this.label5.Location = new System.Drawing.Point(4, 6);
			this.label5.Name = "label5";
			this.label5.Size = new System.Drawing.Size(46, 13);
			this.label5.TabIndex = 15;
			this.label5.Text = "Threads";
			// 
			// consoleBox
			// 
			this.consoleBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.consoleBox.BackColor = System.Drawing.SystemColors.Window;
			this.consoleBox.Location = new System.Drawing.Point(0, 34);
			this.consoleBox.Multiline = true;
			this.consoleBox.Name = "consoleBox";
			this.consoleBox.ReadOnly = true;
			this.consoleBox.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.consoleBox.Size = new System.Drawing.Size(876, 216);
			this.consoleBox.TabIndex = 11;
			this.consoleBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.ConsoleKeyDown);
			// 
			// stopButton
			// 
			this.stopButton.Location = new System.Drawing.Point(82, 5);
			this.stopButton.Name = "stopButton";
			this.stopButton.Size = new System.Drawing.Size(75, 23);
			this.stopButton.TabIndex = 10;
			this.stopButton.Text = "Stop";
			this.stopButton.UseVisualStyleBackColor = true;
			this.stopButton.Click += new System.EventHandler(this.StopExample);
			// 
			// runButton
			// 
			this.runButton.Location = new System.Drawing.Point(1, 5);
			this.runButton.Name = "runButton";
			this.runButton.Size = new System.Drawing.Size(75, 23);
			this.runButton.TabIndex = 9;
			this.runButton.Text = "Start";
			this.runButton.UseVisualStyleBackColor = true;
			this.runButton.MouseClick += new System.Windows.Forms.MouseEventHandler(this.RunExample);
			// 
			// nsBox
			// 
			this.nsBox.Location = new System.Drawing.Point(329, 6);
			this.nsBox.Name = "nsBox";
			this.nsBox.Size = new System.Drawing.Size(100, 20);
			this.nsBox.TabIndex = 3;
			this.nsBox.Text = "test";
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Location = new System.Drawing.Point(265, 9);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(64, 13);
			this.label3.TabIndex = 11;
			this.label3.Text = "Namespace";
			// 
			// portBox
			// 
			this.portBox.Location = new System.Drawing.Point(200, 6);
			this.portBox.Name = "portBox";
			this.portBox.Size = new System.Drawing.Size(50, 20);
			this.portBox.TabIndex = 2;
			this.portBox.Text = "3000";
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Location = new System.Drawing.Point(174, 9);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(26, 13);
			this.label2.TabIndex = 9;
			this.label2.Text = "Port";
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(2, 9);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(63, 13);
			this.label1.TabIndex = 8;
			this.label1.Text = "Server Host";
			// 
			// hostBox
			// 
			this.hostBox.Location = new System.Drawing.Point(65, 6);
			this.hostBox.Name = "hostBox";
			this.hostBox.Size = new System.Drawing.Size(100, 20);
			this.hostBox.TabIndex = 1;
			this.hostBox.Text = "localhost";
			// 
			// setBox
			// 
			this.setBox.Location = new System.Drawing.Point(469, 6);
			this.setBox.Name = "setBox";
			this.setBox.Size = new System.Drawing.Size(100, 20);
			this.setBox.TabIndex = 4;
			this.setBox.Text = "demoset";
			// 
			// label4
			// 
			this.label4.AutoSize = true;
			this.label4.Location = new System.Drawing.Point(445, 9);
			this.label4.Name = "label4";
			this.label4.Size = new System.Drawing.Size(23, 13);
			this.label4.TabIndex = 13;
			this.label4.Text = "Set";
			// 
			// examplesView
			// 
			this.examplesView.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
			this.examplesView.BackColor = System.Drawing.SystemColors.ControlLight;
			this.examplesView.FullRowSelect = true;
			this.examplesView.HideSelection = false;
			this.examplesView.Indent = 5;
			this.examplesView.Location = new System.Drawing.Point(5, 32);
			this.examplesView.Name = "examplesView";
			this.examplesView.ShowLines = false;
			this.examplesView.Size = new System.Drawing.Size(144, 753);
			this.examplesView.TabIndex = 14;
			this.examplesView.AfterSelect += new System.Windows.Forms.TreeViewEventHandler(this.ExampleSelected);
			// 
			// DemoForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(1037, 789);
			this.Controls.Add(this.examplesView);
			this.Controls.Add(this.setBox);
			this.Controls.Add(this.label4);
			this.Controls.Add(this.nsBox);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.portBox);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.label1);
			this.Controls.Add(this.hostBox);
			this.Controls.Add(this.splitContainer1);
			this.Name = "DemoForm";
			this.Text = "Aerospike Database Client Demo";
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FormClose);
			this.splitContainer1.Panel1.ResumeLayout(false);
			this.splitContainer1.Panel2.ResumeLayout(false);
			this.splitContainer1.Panel2.PerformLayout();
			((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).EndInit();
			this.splitContainer1.ResumeLayout(false);
			this.maxCommandPanel.ResumeLayout(false);
			this.maxCommandPanel.PerformLayout();
			this.threadPanel.ResumeLayout(false);
			this.threadPanel.PerformLayout();
			this.ResumeLayout(false);
			this.PerformLayout();

        }

        #endregion

		private System.Windows.Forms.SplitContainer splitContainer1;
        private System.Windows.Forms.RichTextBox codeBox;
        private System.Windows.Forms.TextBox nsBox;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox portBox;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox hostBox;
        private System.Windows.Forms.Button stopButton;
        private System.Windows.Forms.Button runButton;
        private System.Windows.Forms.TextBox setBox;
        private System.Windows.Forms.Label label4;
		private System.Windows.Forms.TextBox consoleBox;
		private System.Windows.Forms.Panel threadPanel;
		private System.Windows.Forms.TextBox threadBox;
		private System.Windows.Forms.Label label5;
		private System.Windows.Forms.Panel maxCommandPanel;
		private System.Windows.Forms.TextBox maxCommandBox;
		private System.Windows.Forms.Label label6;
		private System.Windows.Forms.TreeView examplesView;
    }
}