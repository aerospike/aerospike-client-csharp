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
            this.passwordBox = new System.Windows.Forms.TextBox();
            this.label12 = new System.Windows.Forms.Label();
            this.userBox = new System.Windows.Forms.TextBox();
            this.label13 = new System.Windows.Forms.Label();
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
            this.splitContainer1.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.splitContainer1.Location = new System.Drawing.Point(336, 78);
            this.splitContainer1.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.splitContainer1.Name = "splitContainer1";
            this.splitContainer1.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer1.Panel1
            // 
            this.splitContainer1.Panel1.Controls.Add(this.codeBox);
            // 
            // splitContainer1.Panel2
            // 
            this.splitContainer1.Panel2.Controls.Add(this.consoleBox);
            this.splitContainer1.Panel2.Controls.Add(this.stopButton);
            this.splitContainer1.Panel2.Controls.Add(this.runButton);
            this.splitContainer1.Size = new System.Drawing.Size(1531, 1491);
            this.splitContainer1.SplitterDistance = 782;
            this.splitContainer1.SplitterWidth = 14;
            this.splitContainer1.TabIndex = 0;
            // 
            // codeBox
            // 
            this.codeBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.codeBox.BackColor = System.Drawing.SystemColors.Window;
            this.codeBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.codeBox.Location = new System.Drawing.Point(0, 0);
            this.codeBox.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.codeBox.Name = "codeBox";
            this.codeBox.ReadOnly = true;
            this.codeBox.Size = new System.Drawing.Size(1521, 776);
            this.codeBox.TabIndex = 20;
            this.codeBox.Text = "";
            // 
            // consoleBox
            // 
            this.consoleBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.consoleBox.BackColor = System.Drawing.SystemColors.Window;
            this.consoleBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.consoleBox.Font = new System.Drawing.Font("Courier New", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
            this.consoleBox.Location = new System.Drawing.Point(0, 83);
            this.consoleBox.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.consoleBox.Multiline = true;
            this.consoleBox.Name = "consoleBox";
            this.consoleBox.ReadOnly = true;
            this.consoleBox.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.consoleBox.Size = new System.Drawing.Size(1521, 600);
            this.consoleBox.TabIndex = 21;
            this.consoleBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.ConsoleKeyDown);
            // 
            // stopButton
            // 
            this.stopButton.Location = new System.Drawing.Point(177, 13);
            this.stopButton.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.stopButton.Name = "stopButton";
            this.stopButton.Size = new System.Drawing.Size(162, 56);
            this.stopButton.TabIndex = 12;
            this.stopButton.Text = "Stop";
            this.stopButton.UseVisualStyleBackColor = true;
            this.stopButton.Click += new System.EventHandler(this.StopExample);
            // 
            // runButton
            // 
            this.runButton.Location = new System.Drawing.Point(2, 13);
            this.runButton.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.runButton.Name = "runButton";
            this.runButton.Size = new System.Drawing.Size(162, 56);
            this.runButton.TabIndex = 11;
            this.runButton.Text = "Start";
            this.runButton.UseVisualStyleBackColor = true;
            this.runButton.MouseClick += new System.Windows.Forms.MouseEventHandler(this.RunExample);
            // 
            // nsBox
            // 
            this.nsBox.Location = new System.Drawing.Point(1346, 14);
            this.nsBox.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.nsBox.Name = "nsBox";
            this.nsBox.Size = new System.Drawing.Size(212, 39);
            this.nsBox.TabIndex = 5;
            this.nsBox.Text = "test";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(1209, 22);
            this.label3.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(138, 32);
            this.label3.TabIndex = 11;
            this.label3.Text = "Namespace";
            // 
            // portBox
            // 
            this.portBox.Location = new System.Drawing.Point(434, 14);
            this.portBox.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.portBox.Name = "portBox";
            this.portBox.Size = new System.Drawing.Size(103, 39);
            this.portBox.TabIndex = 2;
            this.portBox.Text = "3000";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(377, 22);
            this.label2.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(56, 32);
            this.label2.TabIndex = 9;
            this.label2.Text = "Port";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(5, 22);
            this.label1.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(137, 32);
            this.label1.TabIndex = 8;
            this.label1.Text = "Server Host";
            // 
            // hostBox
            // 
            this.hostBox.Location = new System.Drawing.Point(141, 14);
            this.hostBox.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.hostBox.Name = "hostBox";
            this.hostBox.Size = new System.Drawing.Size(212, 39);
            this.hostBox.TabIndex = 1;
            this.hostBox.Text = "localhost";
            // 
            // setBox
            // 
            this.setBox.Location = new System.Drawing.Point(1633, 14);
            this.setBox.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.setBox.Name = "setBox";
            this.setBox.Size = new System.Drawing.Size(212, 39);
            this.setBox.TabIndex = 6;
            this.setBox.Text = "demoset";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(1584, 22);
            this.label4.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(48, 32);
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
            this.examplesView.Location = new System.Drawing.Point(11, 78);
            this.examplesView.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.examplesView.Name = "examplesView";
            this.examplesView.ShowLines = false;
            this.examplesView.Size = new System.Drawing.Size(308, 1489);
            this.examplesView.TabIndex = 10;
            this.examplesView.AfterSelect += new System.Windows.Forms.TreeViewEventHandler(this.ExampleSelected);
            // 
            // passwordBox
            // 
            this.passwordBox.Location = new System.Drawing.Point(975, 14);
            this.passwordBox.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.passwordBox.Name = "passwordBox";
            this.passwordBox.Size = new System.Drawing.Size(212, 39);
            this.passwordBox.TabIndex = 4;
            this.passwordBox.UseSystemPasswordChar = true;
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Location = new System.Drawing.Point(860, 22);
            this.label12.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(111, 32);
            this.label12.TabIndex = 18;
            this.label12.Text = "Password";
            // 
            // userBox
            // 
            this.userBox.Location = new System.Drawing.Point(624, 14);
            this.userBox.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
            this.userBox.Name = "userBox";
            this.userBox.Size = new System.Drawing.Size(212, 39);
            this.userBox.TabIndex = 3;
            // 
            // label13
            // 
            this.label13.AutoSize = true;
            this.label13.Location = new System.Drawing.Point(557, 22);
            this.label13.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label13.Name = "label13";
            this.label13.Size = new System.Drawing.Size(61, 32);
            this.label13.TabIndex = 17;
            this.label13.Text = "User";
            // 
            // DemoForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(13F, 32F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1874, 1579);
            this.Controls.Add(this.splitContainer1);
            this.Controls.Add(this.label12);
            this.Controls.Add(this.userBox);
            this.Controls.Add(this.label13);
            this.Controls.Add(this.examplesView);
            this.Controls.Add(this.setBox);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.nsBox);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.portBox);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.hostBox);
            this.Controls.Add(this.passwordBox);
            this.Margin = new System.Windows.Forms.Padding(6, 8, 6, 8);
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
		private System.Windows.Forms.TreeView examplesView;
		private System.Windows.Forms.TextBox passwordBox;
		private System.Windows.Forms.Label label12;
		private System.Windows.Forms.TextBox userBox;
		private System.Windows.Forms.Label label13;
    }
}