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
			splitContainer1 = new System.Windows.Forms.SplitContainer();
			codeBox = new System.Windows.Forms.RichTextBox();
			consoleBox = new System.Windows.Forms.TextBox();
			stopButton = new System.Windows.Forms.Button();
			runButton = new System.Windows.Forms.Button();
			nsBox = new System.Windows.Forms.TextBox();
			label3 = new System.Windows.Forms.Label();
			portBox = new System.Windows.Forms.TextBox();
			label2 = new System.Windows.Forms.Label();
			label1 = new System.Windows.Forms.Label();
			hostBox = new System.Windows.Forms.TextBox();
			setBox = new System.Windows.Forms.TextBox();
			label4 = new System.Windows.Forms.Label();
			examplesView = new System.Windows.Forms.TreeView();
			passwordBox = new System.Windows.Forms.TextBox();
			label12 = new System.Windows.Forms.Label();
			userBox = new System.Windows.Forms.TextBox();
			label13 = new System.Windows.Forms.Label();
			((System.ComponentModel.ISupportInitialize)splitContainer1).BeginInit();
			splitContainer1.Panel1.SuspendLayout();
			splitContainer1.Panel2.SuspendLayout();
			splitContainer1.SuspendLayout();
			SuspendLayout();
			// 
			// splitContainer1
			// 
			splitContainer1.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;
			splitContainer1.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
			splitContainer1.Location = new System.Drawing.Point(207, 49);
			splitContainer1.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			splitContainer1.Name = "splitContainer1";
			splitContainer1.Orientation = System.Windows.Forms.Orientation.Horizontal;
			// 
			// splitContainer1.Panel1
			// 
			splitContainer1.Panel1.Controls.Add(codeBox);
			// 
			// splitContainer1.Panel2
			// 
			splitContainer1.Panel2.Controls.Add(consoleBox);
			splitContainer1.Panel2.Controls.Add(stopButton);
			splitContainer1.Panel2.Controls.Add(runButton);
			splitContainer1.Size = new System.Drawing.Size(942, 679);
			splitContainer1.SplitterDistance = 356;
			splitContainer1.SplitterWidth = 9;
			splitContainer1.TabIndex = 0;
			// 
			// codeBox
			// 
			codeBox.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;
			codeBox.BackColor = System.Drawing.SystemColors.Window;
			codeBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
			codeBox.Location = new System.Drawing.Point(0, 0);
			codeBox.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			codeBox.Name = "codeBox";
			codeBox.ReadOnly = true;
			codeBox.Size = new System.Drawing.Size(934, 351);
			codeBox.TabIndex = 20;
			codeBox.Text = "";
			// 
			// consoleBox
			// 
			consoleBox.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left | System.Windows.Forms.AnchorStyles.Right;
			consoleBox.BackColor = System.Drawing.SystemColors.Window;
			consoleBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
			consoleBox.Font = new System.Drawing.Font("Courier New", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
			consoleBox.Location = new System.Drawing.Point(0, 52);
			consoleBox.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			consoleBox.Multiline = true;
			consoleBox.Name = "consoleBox";
			consoleBox.ReadOnly = true;
			consoleBox.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			consoleBox.Size = new System.Drawing.Size(934, 238);
			consoleBox.TabIndex = 21;
			consoleBox.KeyDown += ConsoleKeyDown;
			// 
			// stopButton
			// 
			stopButton.Location = new System.Drawing.Point(109, 8);
			stopButton.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			stopButton.Name = "stopButton";
			stopButton.Size = new System.Drawing.Size(100, 35);
			stopButton.TabIndex = 12;
			stopButton.Text = "Stop";
			stopButton.UseVisualStyleBackColor = true;
			stopButton.Click += StopExample;
			// 
			// runButton
			// 
			runButton.Location = new System.Drawing.Point(1, 8);
			runButton.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			runButton.Name = "runButton";
			runButton.Size = new System.Drawing.Size(100, 35);
			runButton.TabIndex = 11;
			runButton.Text = "Start";
			runButton.UseVisualStyleBackColor = true;
			runButton.MouseClick += RunExample;
			// 
			// nsBox
			// 
			nsBox.Location = new System.Drawing.Point(828, 9);
			nsBox.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			nsBox.Name = "nsBox";
			nsBox.Size = new System.Drawing.Size(132, 27);
			nsBox.TabIndex = 5;
			nsBox.Text = "test";
			// 
			// label3
			// 
			label3.AutoSize = true;
			label3.Location = new System.Drawing.Point(744, 14);
			label3.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
			label3.Name = "label3";
			label3.Size = new System.Drawing.Size(87, 20);
			label3.TabIndex = 11;
			label3.Text = "Namespace";
			// 
			// portBox
			// 
			portBox.Location = new System.Drawing.Point(267, 9);
			portBox.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			portBox.Name = "portBox";
			portBox.Size = new System.Drawing.Size(65, 27);
			portBox.TabIndex = 2;
			portBox.Text = "3000";
			// 
			// label2
			// 
			label2.AutoSize = true;
			label2.Location = new System.Drawing.Point(232, 14);
			label2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
			label2.Name = "label2";
			label2.Size = new System.Drawing.Size(35, 20);
			label2.TabIndex = 9;
			label2.Text = "Port";
			// 
			// label1
			// 
			label1.AutoSize = true;
			label1.Location = new System.Drawing.Point(3, 14);
			label1.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
			label1.Name = "label1";
			label1.Size = new System.Drawing.Size(85, 20);
			label1.TabIndex = 8;
			label1.Text = "Server Host";
			// 
			// hostBox
			// 
			hostBox.Location = new System.Drawing.Point(87, 9);
			hostBox.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			hostBox.Name = "hostBox";
			hostBox.Size = new System.Drawing.Size(132, 27);
			hostBox.TabIndex = 1;
			hostBox.Text = "localhost";
			// 
			// setBox
			// 
			setBox.Location = new System.Drawing.Point(1005, 9);
			setBox.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			setBox.Name = "setBox";
			setBox.Size = new System.Drawing.Size(132, 27);
			setBox.TabIndex = 6;
			setBox.Text = "demoset";
			// 
			// label4
			// 
			label4.AutoSize = true;
			label4.Location = new System.Drawing.Point(975, 14);
			label4.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
			label4.Name = "label4";
			label4.Size = new System.Drawing.Size(30, 20);
			label4.TabIndex = 13;
			label4.Text = "Set";
			// 
			// examplesView
			// 
			examplesView.Anchor = System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left;
			examplesView.BackColor = System.Drawing.SystemColors.ControlLight;
			examplesView.FullRowSelect = true;
			examplesView.HideSelection = false;
			examplesView.Indent = 5;
			examplesView.Location = new System.Drawing.Point(7, 49);
			examplesView.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			examplesView.Name = "examplesView";
			examplesView.ShowLines = false;
			examplesView.Size = new System.Drawing.Size(191, 678);
			examplesView.TabIndex = 10;
			examplesView.AfterSelect += ExampleSelected;
			// 
			// passwordBox
			// 
			passwordBox.Location = new System.Drawing.Point(600, 9);
			passwordBox.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			passwordBox.Name = "passwordBox";
			passwordBox.Size = new System.Drawing.Size(132, 27);
			passwordBox.TabIndex = 4;
			passwordBox.UseSystemPasswordChar = true;
			// 
			// label12
			// 
			label12.AutoSize = true;
			label12.Location = new System.Drawing.Point(529, 14);
			label12.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
			label12.Name = "label12";
			label12.Size = new System.Drawing.Size(70, 20);
			label12.TabIndex = 18;
			label12.Text = "Password";
			// 
			// userBox
			// 
			userBox.Location = new System.Drawing.Point(384, 9);
			userBox.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			userBox.Name = "userBox";
			userBox.Size = new System.Drawing.Size(132, 27);
			userBox.TabIndex = 3;
			// 
			// label13
			// 
			label13.AutoSize = true;
			label13.Location = new System.Drawing.Point(343, 14);
			label13.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
			label13.Name = "label13";
			label13.Size = new System.Drawing.Size(38, 20);
			label13.TabIndex = 17;
			label13.Text = "User";
			// 
			// DemoForm
			// 
			AutoScaleDimensions = new System.Drawing.SizeF(8F, 20F);
			AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			ClientSize = new System.Drawing.Size(1153, 734);
			Controls.Add(splitContainer1);
			Controls.Add(label12);
			Controls.Add(userBox);
			Controls.Add(label13);
			Controls.Add(examplesView);
			Controls.Add(setBox);
			Controls.Add(label4);
			Controls.Add(nsBox);
			Controls.Add(label3);
			Controls.Add(portBox);
			Controls.Add(label2);
			Controls.Add(label1);
			Controls.Add(hostBox);
			Controls.Add(passwordBox);
			Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
			Name = "DemoForm";
			Text = "Aerospike Database Client Demo";
			FormClosing += FormClose;
			splitContainer1.Panel1.ResumeLayout(false);
			splitContainer1.Panel2.ResumeLayout(false);
			splitContainer1.Panel2.PerformLayout();
			((System.ComponentModel.ISupportInitialize)splitContainer1).EndInit();
			splitContainer1.ResumeLayout(false);
			ResumeLayout(false);
			PerformLayout();
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