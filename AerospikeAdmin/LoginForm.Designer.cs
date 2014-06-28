namespace Aerospike.Admin
{
	partial class LoginForm
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
			this.label1 = new System.Windows.Forms.Label();
			this.label2 = new System.Windows.Forms.Label();
			this.label3 = new System.Windows.Forms.Label();
			this.label4 = new System.Windows.Forms.Label();
			this.hostBox = new System.Windows.Forms.TextBox();
			this.portBox = new System.Windows.Forms.TextBox();
			this.userBox = new System.Windows.Forms.TextBox();
			this.passwordBox = new System.Windows.Forms.TextBox();
			this.loginButton = new System.Windows.Forms.Button();
			this.SuspendLayout();
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(12, 14);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(38, 13);
			this.label1.TabIndex = 0;
			this.label1.Text = "Server";
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Location = new System.Drawing.Point(12, 40);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(26, 13);
			this.label2.TabIndex = 1;
			this.label2.Text = "Port";
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Location = new System.Drawing.Point(12, 66);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(29, 13);
			this.label3.TabIndex = 2;
			this.label3.Text = "User";
			// 
			// label4
			// 
			this.label4.AutoSize = true;
			this.label4.Location = new System.Drawing.Point(12, 92);
			this.label4.Name = "label4";
			this.label4.RightToLeft = System.Windows.Forms.RightToLeft.No;
			this.label4.Size = new System.Drawing.Size(53, 13);
			this.label4.TabIndex = 3;
			this.label4.Text = "Password";
			// 
			// hostBox
			// 
			this.hostBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.hostBox.Location = new System.Drawing.Point(84, 11);
			this.hostBox.Name = "hostBox";
			this.hostBox.Size = new System.Drawing.Size(201, 20);
			this.hostBox.TabIndex = 4;
			// 
			// portBox
			// 
			this.portBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.portBox.Location = new System.Drawing.Point(84, 37);
			this.portBox.Name = "portBox";
			this.portBox.Size = new System.Drawing.Size(201, 20);
			this.portBox.TabIndex = 5;
			// 
			// userBox
			// 
			this.userBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.userBox.Location = new System.Drawing.Point(84, 63);
			this.userBox.Name = "userBox";
			this.userBox.Size = new System.Drawing.Size(201, 20);
			this.userBox.TabIndex = 6;
			// 
			// passwordBox
			// 
			this.passwordBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.passwordBox.Location = new System.Drawing.Point(84, 89);
			this.passwordBox.Name = "passwordBox";
			this.passwordBox.Size = new System.Drawing.Size(201, 20);
			this.passwordBox.TabIndex = 7;
			this.passwordBox.UseSystemPasswordChar = true;
			this.passwordBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.KeyDownClicked);
			// 
			// loginButton
			// 
			this.loginButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
			this.loginButton.Location = new System.Drawing.Point(210, 118);
			this.loginButton.Name = "loginButton";
			this.loginButton.Size = new System.Drawing.Size(75, 23);
			this.loginButton.TabIndex = 8;
			this.loginButton.Text = "Login";
			this.loginButton.UseVisualStyleBackColor = true;
			this.loginButton.Click += new System.EventHandler(this.LoginClicked);
			// 
			// LoginForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(293, 150);
			this.Controls.Add(this.loginButton);
			this.Controls.Add(this.passwordBox);
			this.Controls.Add(this.userBox);
			this.Controls.Add(this.portBox);
			this.Controls.Add(this.hostBox);
			this.Controls.Add(this.label4);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.label1);
			this.Name = "LoginForm";
			this.Text = "Aerospike User Administration";
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FormClose);
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.Label label3;
		private System.Windows.Forms.Label label4;
		private System.Windows.Forms.TextBox hostBox;
		private System.Windows.Forms.TextBox portBox;
		private System.Windows.Forms.TextBox userBox;
		private System.Windows.Forms.TextBox passwordBox;
		private System.Windows.Forms.Button loginButton;
	}
}

