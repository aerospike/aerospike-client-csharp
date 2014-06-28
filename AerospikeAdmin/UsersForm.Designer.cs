namespace Aerospike.Admin
{
	partial class UsersForm
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
			this.grid = new System.Windows.Forms.DataGridView();
			this.menuStrip1 = new System.Windows.Forms.MenuStrip();
			this.userToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.createToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.dropToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.changePasswordToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.editRolesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.createUserButton = new System.Windows.Forms.Button();
			this.button2 = new System.Windows.Forms.Button();
			((System.ComponentModel.ISupportInitialize)(this.grid)).BeginInit();
			this.menuStrip1.SuspendLayout();
			this.SuspendLayout();
			// 
			// grid
			// 
			this.grid.AllowUserToDeleteRows = false;
			this.grid.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.grid.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.grid.Location = new System.Drawing.Point(0, 27);
			this.grid.Name = "grid";
			this.grid.Size = new System.Drawing.Size(419, 369);
			this.grid.TabIndex = 0;
			this.grid.MouseClick += new System.Windows.Forms.MouseEventHandler(this.MouseClicked);
			// 
			// menuStrip1
			// 
			this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.userToolStripMenuItem});
			this.menuStrip1.Location = new System.Drawing.Point(0, 0);
			this.menuStrip1.Name = "menuStrip1";
			this.menuStrip1.Size = new System.Drawing.Size(419, 24);
			this.menuStrip1.TabIndex = 3;
			this.menuStrip1.Text = "menuStrip1";
			// 
			// userToolStripMenuItem
			// 
			this.userToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.createToolStripMenuItem,
            this.dropToolStripMenuItem,
            this.changePasswordToolStripMenuItem,
            this.editRolesToolStripMenuItem});
			this.userToolStripMenuItem.Name = "userToolStripMenuItem";
			this.userToolStripMenuItem.Size = new System.Drawing.Size(42, 20);
			this.userToolStripMenuItem.Text = "User";
			// 
			// createToolStripMenuItem
			// 
			this.createToolStripMenuItem.Name = "createToolStripMenuItem";
			this.createToolStripMenuItem.Size = new System.Drawing.Size(168, 22);
			this.createToolStripMenuItem.Text = "Create";
			this.createToolStripMenuItem.Click += new System.EventHandler(this.CreateUserClicked);
			// 
			// dropToolStripMenuItem
			// 
			this.dropToolStripMenuItem.Name = "dropToolStripMenuItem";
			this.dropToolStripMenuItem.Size = new System.Drawing.Size(168, 22);
			this.dropToolStripMenuItem.Text = "Drop";
			this.dropToolStripMenuItem.Click += new System.EventHandler(this.DropUserClicked);
			// 
			// changePasswordToolStripMenuItem
			// 
			this.changePasswordToolStripMenuItem.Name = "changePasswordToolStripMenuItem";
			this.changePasswordToolStripMenuItem.Size = new System.Drawing.Size(168, 22);
			this.changePasswordToolStripMenuItem.Text = "Change Password";
			this.changePasswordToolStripMenuItem.Click += new System.EventHandler(this.ChangePasswordClicked);
			// 
			// editRolesToolStripMenuItem
			// 
			this.editRolesToolStripMenuItem.Name = "editRolesToolStripMenuItem";
			this.editRolesToolStripMenuItem.Size = new System.Drawing.Size(168, 22);
			this.editRolesToolStripMenuItem.Text = "Edit Roles";
			this.editRolesToolStripMenuItem.Click += new System.EventHandler(this.EditRolesClicked);
			// 
			// createUserButton
			// 
			this.createUserButton.Location = new System.Drawing.Point(12, 406);
			this.createUserButton.Name = "createUserButton";
			this.createUserButton.Size = new System.Drawing.Size(75, 23);
			this.createUserButton.TabIndex = 4;
			this.createUserButton.Text = "Create User";
			this.createUserButton.UseVisualStyleBackColor = true;
			this.createUserButton.Click += new System.EventHandler(this.CreateUserClicked);
			// 
			// button2
			// 
			this.button2.Location = new System.Drawing.Point(332, 406);
			this.button2.Name = "button2";
			this.button2.Size = new System.Drawing.Size(75, 23);
			this.button2.TabIndex = 5;
			this.button2.Text = "Refresh";
			this.button2.UseVisualStyleBackColor = true;
			this.button2.Click += new System.EventHandler(this.RefreshClicked);
			// 
			// UsersForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(419, 437);
			this.Controls.Add(this.button2);
			this.Controls.Add(this.createUserButton);
			this.Controls.Add(this.menuStrip1);
			this.Controls.Add(this.grid);
			this.MainMenuStrip = this.menuStrip1;
			this.Name = "UsersForm";
			this.Text = "Aerospike User Administration";
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FormClose);
			((System.ComponentModel.ISupportInitialize)(this.grid)).EndInit();
			this.menuStrip1.ResumeLayout(false);
			this.menuStrip1.PerformLayout();
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.DataGridView grid;
		private System.Windows.Forms.MenuStrip menuStrip1;
		private System.Windows.Forms.ToolStripMenuItem userToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem createToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem dropToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem changePasswordToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem editRolesToolStripMenuItem;
		private System.Windows.Forms.Button createUserButton;
		private System.Windows.Forms.Button button2;
	}
}