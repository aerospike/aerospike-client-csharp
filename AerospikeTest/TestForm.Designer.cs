namespace Aerospike.Test
{
	partial class TestForm
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
			this.label12 = new System.Windows.Forms.Label();
			this.userBox = new System.Windows.Forms.TextBox();
			this.label13 = new System.Windows.Forms.Label();
			this.setBox = new System.Windows.Forms.TextBox();
			this.label4 = new System.Windows.Forms.Label();
			this.nsBox = new System.Windows.Forms.TextBox();
			this.label3 = new System.Windows.Forms.Label();
			this.portBox = new System.Windows.Forms.TextBox();
			this.label2 = new System.Windows.Forms.Label();
			this.label1 = new System.Windows.Forms.Label();
			this.hostBox = new System.Windows.Forms.TextBox();
			this.passwordBox = new System.Windows.Forms.TextBox();
			this.runButton = new System.Windows.Forms.Button();
			this.SuspendLayout();
			// 
			// label12
			// 
			this.label12.AutoSize = true;
			this.label12.Location = new System.Drawing.Point(12, 87);
			this.label12.Name = "label12";
			this.label12.Size = new System.Drawing.Size(53, 13);
			this.label12.TabIndex = 30;
			this.label12.Text = "Password";
			// 
			// userBox
			// 
			this.userBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.userBox.Location = new System.Drawing.Point(90, 58);
			this.userBox.Name = "userBox";
			this.userBox.Size = new System.Drawing.Size(138, 20);
			this.userBox.TabIndex = 27;
			this.userBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.KeyDownClick);
			// 
			// label13
			// 
			this.label13.AutoSize = true;
			this.label13.Location = new System.Drawing.Point(12, 61);
			this.label13.Name = "label13";
			this.label13.Size = new System.Drawing.Size(29, 13);
			this.label13.TabIndex = 29;
			this.label13.Text = "User";
			// 
			// setBox
			// 
			this.setBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.setBox.Location = new System.Drawing.Point(90, 136);
			this.setBox.Name = "setBox";
			this.setBox.Size = new System.Drawing.Size(138, 20);
			this.setBox.TabIndex = 22;
			this.setBox.Text = "test";
			this.setBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.KeyDownClick);
			// 
			// label4
			// 
			this.label4.AutoSize = true;
			this.label4.Location = new System.Drawing.Point(12, 139);
			this.label4.Name = "label4";
			this.label4.Size = new System.Drawing.Size(23, 13);
			this.label4.TabIndex = 26;
			this.label4.Text = "Set";
			// 
			// nsBox
			// 
			this.nsBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.nsBox.Location = new System.Drawing.Point(90, 110);
			this.nsBox.Name = "nsBox";
			this.nsBox.Size = new System.Drawing.Size(138, 20);
			this.nsBox.TabIndex = 21;
			this.nsBox.Text = "test";
			this.nsBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.KeyDownClick);
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Location = new System.Drawing.Point(11, 113);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(64, 13);
			this.label3.TabIndex = 25;
			this.label3.Text = "Namespace";
			// 
			// portBox
			// 
			this.portBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.portBox.Location = new System.Drawing.Point(90, 32);
			this.portBox.Name = "portBox";
			this.portBox.Size = new System.Drawing.Size(138, 20);
			this.portBox.TabIndex = 20;
			this.portBox.Text = "3000";
			this.portBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.KeyDownClick);
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Location = new System.Drawing.Point(12, 35);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(26, 13);
			this.label2.TabIndex = 24;
			this.label2.Text = "Port";
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(12, 9);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(63, 13);
			this.label1.TabIndex = 23;
			this.label1.Text = "Server Host";
			// 
			// hostBox
			// 
			this.hostBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.hostBox.Location = new System.Drawing.Point(90, 6);
			this.hostBox.Name = "hostBox";
			this.hostBox.Size = new System.Drawing.Size(138, 20);
			this.hostBox.TabIndex = 19;
			this.hostBox.Text = "localhost";
			this.hostBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.KeyDownClick);
			// 
			// passwordBox
			// 
			this.passwordBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.passwordBox.Location = new System.Drawing.Point(90, 84);
			this.passwordBox.Name = "passwordBox";
			this.passwordBox.Size = new System.Drawing.Size(138, 20);
			this.passwordBox.TabIndex = 28;
			this.passwordBox.UseSystemPasswordChar = true;
			this.passwordBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.KeyDownClick);
			// 
			// runButton
			// 
			this.runButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
			this.runButton.Location = new System.Drawing.Point(153, 169);
			this.runButton.Name = "runButton";
			this.runButton.Size = new System.Drawing.Size(75, 23);
			this.runButton.TabIndex = 31;
			this.runButton.Text = "Test";
			this.runButton.UseVisualStyleBackColor = true;
			this.runButton.Click += new System.EventHandler(this.TestClicked);
			// 
			// TestForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(241, 204);
			this.Controls.Add(this.runButton);
			this.Controls.Add(this.label12);
			this.Controls.Add(this.userBox);
			this.Controls.Add(this.label13);
			this.Controls.Add(this.setBox);
			this.Controls.Add(this.label4);
			this.Controls.Add(this.nsBox);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.portBox);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.label1);
			this.Controls.Add(this.hostBox);
			this.Controls.Add(this.passwordBox);
			this.MaximizeBox = false;
			this.MinimizeBox = false;
			this.Name = "TestForm";
			this.Text = "Aerospike Test";
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Label label12;
		private System.Windows.Forms.TextBox userBox;
		private System.Windows.Forms.Label label13;
		private System.Windows.Forms.TextBox setBox;
		private System.Windows.Forms.Label label4;
		private System.Windows.Forms.TextBox nsBox;
		private System.Windows.Forms.Label label3;
		private System.Windows.Forms.TextBox portBox;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.TextBox hostBox;
		private System.Windows.Forms.TextBox passwordBox;
		private System.Windows.Forms.Button runButton;
	}
}