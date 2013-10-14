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
			this.examplesView = new System.Windows.Forms.TreeView();
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
			((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).BeginInit();
			this.splitContainer1.Panel1.SuspendLayout();
			this.splitContainer1.Panel2.SuspendLayout();
			this.splitContainer1.SuspendLayout();
			this.SuspendLayout();
			// 
			// splitContainer1
			// 
			this.splitContainer1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.splitContainer1.Location = new System.Drawing.Point(3, 32);
			this.splitContainer1.Name = "splitContainer1";
			this.splitContainer1.Orientation = System.Windows.Forms.Orientation.Horizontal;
			// 
			// splitContainer1.Panel1
			// 
			this.splitContainer1.Panel1.Controls.Add(this.codeBox);
			this.splitContainer1.Panel1.Controls.Add(this.examplesView);
			// 
			// splitContainer1.Panel2
			// 
			this.splitContainer1.Panel2.Controls.Add(this.consoleBox);
			this.splitContainer1.Panel2.Controls.Add(this.stopButton);
			this.splitContainer1.Panel2.Controls.Add(this.runButton);
			this.splitContainer1.Size = new System.Drawing.Size(1031, 728);
			this.splitContainer1.SplitterDistance = 480;
			this.splitContainer1.TabIndex = 0;
			// 
			// codeBox
			// 
			this.codeBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.codeBox.BackColor = System.Drawing.SystemColors.Window;
			this.codeBox.Location = new System.Drawing.Point(137, 3);
			this.codeBox.Name = "codeBox";
			this.codeBox.ReadOnly = true;
			this.codeBox.Size = new System.Drawing.Size(891, 474);
			this.codeBox.TabIndex = 8;
			this.codeBox.Text = "";
			// 
			// examplesView
			// 
			this.examplesView.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
			this.examplesView.FullRowSelect = true;
			this.examplesView.HideSelection = false;
			this.examplesView.Indent = 5;
			this.examplesView.Location = new System.Drawing.Point(3, 3);
			this.examplesView.Name = "examplesView";
			this.examplesView.ShowLines = false;
			this.examplesView.Size = new System.Drawing.Size(128, 474);
			this.examplesView.TabIndex = 7;
			this.examplesView.AfterSelect += new System.Windows.Forms.TreeViewEventHandler(this.ExampleSelected);
			// 
			// consoleBox
			// 
			this.consoleBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.consoleBox.Location = new System.Drawing.Point(3, 34);
			this.consoleBox.Multiline = true;
			this.consoleBox.Name = "consoleBox";
			this.consoleBox.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.consoleBox.Size = new System.Drawing.Size(1025, 207);
			this.consoleBox.TabIndex = 11;
			// 
			// stopButton
			// 
			this.stopButton.Location = new System.Drawing.Point(86, 5);
			this.stopButton.Name = "stopButton";
			this.stopButton.Size = new System.Drawing.Size(75, 23);
			this.stopButton.TabIndex = 10;
			this.stopButton.Text = "Stop";
			this.stopButton.UseVisualStyleBackColor = true;
			this.stopButton.Click += new System.EventHandler(this.StopExample);
			// 
			// runButton
			// 
			this.runButton.Location = new System.Drawing.Point(5, 5);
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
			// DemoForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(1037, 764);
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
			this.ResumeLayout(false);
			this.PerformLayout();

        }

        #endregion

		private System.Windows.Forms.SplitContainer splitContainer1;
        private System.Windows.Forms.TreeView examplesView;
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
    }
}