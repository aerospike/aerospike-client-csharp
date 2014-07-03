namespace Aerospike.Admin
{
	partial class UserEditForm
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
			this.userBox = new System.Windows.Forms.TextBox();
			this.label1 = new System.Windows.Forms.Label();
			this.passwordBox = new System.Windows.Forms.TextBox();
			this.label4 = new System.Windows.Forms.Label();
			this.passwordVerifyBox = new System.Windows.Forms.TextBox();
			this.label2 = new System.Windows.Forms.Label();
			this.label3 = new System.Windows.Forms.Label();
			this.rolesBox = new System.Windows.Forms.CheckedListBox();
			this.button1 = new System.Windows.Forms.Button();
			this.button2 = new System.Windows.Forms.Button();
			this.SuspendLayout();
			// 
			// userBox
			// 
			this.userBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.userBox.Location = new System.Drawing.Point(95, 12);
			this.userBox.Name = "userBox";
			this.userBox.Size = new System.Drawing.Size(210, 20);
			this.userBox.TabIndex = 0;
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(7, 15);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(29, 13);
			this.label1.TabIndex = 5;
			this.label1.Text = "User";
			// 
			// passwordBox
			// 
			this.passwordBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.passwordBox.Location = new System.Drawing.Point(95, 38);
			this.passwordBox.Name = "passwordBox";
			this.passwordBox.Size = new System.Drawing.Size(210, 20);
			this.passwordBox.TabIndex = 1;
			this.passwordBox.UseSystemPasswordChar = true;
			// 
			// label4
			// 
			this.label4.AutoSize = true;
			this.label4.Location = new System.Drawing.Point(7, 41);
			this.label4.Name = "label4";
			this.label4.RightToLeft = System.Windows.Forms.RightToLeft.No;
			this.label4.Size = new System.Drawing.Size(53, 13);
			this.label4.TabIndex = 8;
			this.label4.Text = "Password";
			// 
			// passwordVerifyBox
			// 
			this.passwordVerifyBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.passwordVerifyBox.Location = new System.Drawing.Point(95, 64);
			this.passwordVerifyBox.Name = "passwordVerifyBox";
			this.passwordVerifyBox.Size = new System.Drawing.Size(210, 20);
			this.passwordVerifyBox.TabIndex = 2;
			this.passwordVerifyBox.UseSystemPasswordChar = true;
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Location = new System.Drawing.Point(7, 67);
			this.label2.Name = "label2";
			this.label2.RightToLeft = System.Windows.Forms.RightToLeft.No;
			this.label2.Size = new System.Drawing.Size(82, 13);
			this.label2.TabIndex = 10;
			this.label2.Text = "Verify Password";
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Location = new System.Drawing.Point(7, 92);
			this.label3.Name = "label3";
			this.label3.RightToLeft = System.Windows.Forms.RightToLeft.No;
			this.label3.Size = new System.Drawing.Size(34, 13);
			this.label3.TabIndex = 12;
			this.label3.Text = "Roles";
			// 
			// rolesBox
			// 
			this.rolesBox.CheckOnClick = true;
			this.rolesBox.FormattingEnabled = true;
			this.rolesBox.Items.AddRange(new object[] {
            "user-admin",
            "sys-admin",
            "read-write",
            "read"});
			this.rolesBox.Location = new System.Drawing.Point(95, 92);
			this.rolesBox.Name = "rolesBox";
			this.rolesBox.Size = new System.Drawing.Size(98, 64);
			this.rolesBox.TabIndex = 3;
			// 
			// button1
			// 
			this.button1.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
			this.button1.Location = new System.Drawing.Point(143, 166);
			this.button1.Name = "button1";
			this.button1.Size = new System.Drawing.Size(75, 23);
			this.button1.TabIndex = 4;
			this.button1.Text = "Cancel";
			this.button1.UseVisualStyleBackColor = true;
			this.button1.Click += new System.EventHandler(this.CancelClicked);
			// 
			// button2
			// 
			this.button2.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
			this.button2.Location = new System.Drawing.Point(229, 166);
			this.button2.Name = "button2";
			this.button2.Size = new System.Drawing.Size(75, 23);
			this.button2.TabIndex = 5;
			this.button2.Text = "OK";
			this.button2.UseVisualStyleBackColor = true;
			this.button2.Click += new System.EventHandler(this.SaveClicked);
			// 
			// UserEditForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(317, 201);
			this.Controls.Add(this.button2);
			this.Controls.Add(this.button1);
			this.Controls.Add(this.rolesBox);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.passwordVerifyBox);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.passwordBox);
			this.Controls.Add(this.label4);
			this.Controls.Add(this.userBox);
			this.Controls.Add(this.label1);
			this.Name = "UserEditForm";
			this.Text = "Create User";
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.TextBox userBox;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.TextBox passwordBox;
		private System.Windows.Forms.Label label4;
		private System.Windows.Forms.TextBox passwordVerifyBox;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.Label label3;
		private System.Windows.Forms.CheckedListBox rolesBox;
		private System.Windows.Forms.Button button1;
		private System.Windows.Forms.Button button2;
	}
}