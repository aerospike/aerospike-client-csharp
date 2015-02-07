namespace Aerospike.Admin
{
	partial class UsersControl
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

		#region Component Designer generated code

		/// <summary> 
		/// Required method for Designer support - do not modify 
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			this.splitContainer1 = new System.Windows.Forms.SplitContainer();
			this.usersTitle = new System.Windows.Forms.Label();
			this.gridUsers = new System.Windows.Forms.DataGridView();
			this.UserNameColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.roleTitle = new System.Windows.Forms.Label();
			this.gridRoles = new System.Windows.Forms.DataGridView();
			this.RoleNameColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.refreshUserButton = new System.Windows.Forms.Button();
			this.createUserButton = new System.Windows.Forms.Button();
			this.menuStrip1 = new System.Windows.Forms.MenuStrip();
			this.userToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.createUserMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.dropUserMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.changePasswordMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.editUserMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).BeginInit();
			this.splitContainer1.Panel1.SuspendLayout();
			this.splitContainer1.Panel2.SuspendLayout();
			this.splitContainer1.SuspendLayout();
			((System.ComponentModel.ISupportInitialize)(this.gridUsers)).BeginInit();
			((System.ComponentModel.ISupportInitialize)(this.gridRoles)).BeginInit();
			this.menuStrip1.SuspendLayout();
			this.SuspendLayout();
			// 
			// splitContainer1
			// 
			this.splitContainer1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.splitContainer1.Location = new System.Drawing.Point(0, 27);
			this.splitContainer1.Name = "splitContainer1";
			// 
			// splitContainer1.Panel1
			// 
			this.splitContainer1.Panel1.Controls.Add(this.usersTitle);
			this.splitContainer1.Panel1.Controls.Add(this.gridUsers);
			// 
			// splitContainer1.Panel2
			// 
			this.splitContainer1.Panel2.Controls.Add(this.roleTitle);
			this.splitContainer1.Panel2.Controls.Add(this.gridRoles);
			this.splitContainer1.Size = new System.Drawing.Size(392, 334);
			this.splitContainer1.SplitterDistance = 193;
			this.splitContainer1.TabIndex = 9;
			// 
			// usersTitle
			// 
			this.usersTitle.AutoSize = true;
			this.usersTitle.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.usersTitle.Location = new System.Drawing.Point(2, 2);
			this.usersTitle.Name = "usersTitle";
			this.usersTitle.Size = new System.Drawing.Size(44, 15);
			this.usersTitle.TabIndex = 2;
			this.usersTitle.Text = "Users";
			// 
			// gridUsers
			// 
			this.gridUsers.AllowUserToDeleteRows = false;
			this.gridUsers.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.gridUsers.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.gridUsers.ColumnHeadersVisible = false;
			this.gridUsers.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.UserNameColumn});
			this.gridUsers.Location = new System.Drawing.Point(0, 20);
			this.gridUsers.MultiSelect = false;
			this.gridUsers.Name = "gridUsers";
			this.gridUsers.RowHeadersVisible = false;
			this.gridUsers.Size = new System.Drawing.Size(191, 314);
			this.gridUsers.TabIndex = 10;
			this.gridUsers.RowEnter += new System.Windows.Forms.DataGridViewCellEventHandler(this.UserRowEnter);
			this.gridUsers.MouseClick += new System.Windows.Forms.MouseEventHandler(this.UserClicked);
			// 
			// UserNameColumn
			// 
			this.UserNameColumn.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
			this.UserNameColumn.HeaderText = "User";
			this.UserNameColumn.Name = "UserNameColumn";
			// 
			// roleTitle
			// 
			this.roleTitle.AutoSize = true;
			this.roleTitle.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.roleTitle.Location = new System.Drawing.Point(2, 2);
			this.roleTitle.Name = "roleTitle";
			this.roleTitle.Size = new System.Drawing.Size(89, 15);
			this.roleTitle.TabIndex = 3;
			this.roleTitle.Text = "User\'s Roles";
			// 
			// gridRoles
			// 
			this.gridRoles.AllowUserToAddRows = false;
			this.gridRoles.AllowUserToDeleteRows = false;
			this.gridRoles.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.gridRoles.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.gridRoles.ColumnHeadersVisible = false;
			this.gridRoles.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.RoleNameColumn});
			this.gridRoles.Location = new System.Drawing.Point(0, 20);
			this.gridRoles.MultiSelect = false;
			this.gridRoles.Name = "gridRoles";
			this.gridRoles.ReadOnly = true;
			this.gridRoles.RowHeadersVisible = false;
			this.gridRoles.Size = new System.Drawing.Size(195, 314);
			this.gridRoles.TabIndex = 11;
			this.gridRoles.SelectionChanged += new System.EventHandler(this.RoleSelectionChanged);
			// 
			// RoleNameColumn
			// 
			this.RoleNameColumn.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
			this.RoleNameColumn.HeaderText = "Roles";
			this.RoleNameColumn.Name = "RoleNameColumn";
			this.RoleNameColumn.ReadOnly = true;
			// 
			// refreshUserButton
			// 
			this.refreshUserButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
			this.refreshUserButton.Location = new System.Drawing.Point(314, 369);
			this.refreshUserButton.Name = "refreshUserButton";
			this.refreshUserButton.Size = new System.Drawing.Size(75, 23);
			this.refreshUserButton.TabIndex = 2;
			this.refreshUserButton.Text = "Refresh";
			this.refreshUserButton.UseVisualStyleBackColor = true;
			this.refreshUserButton.Click += new System.EventHandler(this.RefreshClicked);
			// 
			// createUserButton
			// 
			this.createUserButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
			this.createUserButton.Location = new System.Drawing.Point(0, 369);
			this.createUserButton.Name = "createUserButton";
			this.createUserButton.Size = new System.Drawing.Size(75, 23);
			this.createUserButton.TabIndex = 1;
			this.createUserButton.Text = "Create User";
			this.createUserButton.UseVisualStyleBackColor = true;
			this.createUserButton.Click += new System.EventHandler(this.CreateUserClicked);
			// 
			// menuStrip1
			// 
			this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.userToolStripMenuItem});
			this.menuStrip1.Location = new System.Drawing.Point(0, 0);
			this.menuStrip1.Name = "menuStrip1";
			this.menuStrip1.Size = new System.Drawing.Size(392, 24);
			this.menuStrip1.TabIndex = 10;
			this.menuStrip1.Text = "menuStrip1";
			// 
			// userToolStripMenuItem
			// 
			this.userToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.createUserMenuItem,
            this.dropUserMenuItem,
            this.changePasswordMenuItem,
            this.editUserMenuItem});
			this.userToolStripMenuItem.Name = "userToolStripMenuItem";
			this.userToolStripMenuItem.Size = new System.Drawing.Size(42, 20);
			this.userToolStripMenuItem.Text = "User";
			// 
			// createUserMenuItem
			// 
			this.createUserMenuItem.Name = "createUserMenuItem";
			this.createUserMenuItem.Size = new System.Drawing.Size(168, 22);
			this.createUserMenuItem.Text = "Create";
			this.createUserMenuItem.Click += new System.EventHandler(this.CreateUserClicked);
			// 
			// dropUserMenuItem
			// 
			this.dropUserMenuItem.Name = "dropUserMenuItem";
			this.dropUserMenuItem.Size = new System.Drawing.Size(168, 22);
			this.dropUserMenuItem.Text = "Drop";
			this.dropUserMenuItem.Click += new System.EventHandler(this.DropUserClicked);
			// 
			// changePasswordMenuItem
			// 
			this.changePasswordMenuItem.Name = "changePasswordMenuItem";
			this.changePasswordMenuItem.Size = new System.Drawing.Size(168, 22);
			this.changePasswordMenuItem.Text = "Change Password";
			this.changePasswordMenuItem.Click += new System.EventHandler(this.ChangePasswordClicked);
			// 
			// editUserMenuItem
			// 
			this.editUserMenuItem.Name = "editUserMenuItem";
			this.editUserMenuItem.Size = new System.Drawing.Size(168, 22);
			this.editUserMenuItem.Text = "Edit";
			this.editUserMenuItem.Click += new System.EventHandler(this.EditRolesClicked);
			// 
			// UsersControl
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.Controls.Add(this.menuStrip1);
			this.Controls.Add(this.splitContainer1);
			this.Controls.Add(this.refreshUserButton);
			this.Controls.Add(this.createUserButton);
			this.Name = "UsersControl";
			this.Size = new System.Drawing.Size(392, 395);
			this.splitContainer1.Panel1.ResumeLayout(false);
			this.splitContainer1.Panel1.PerformLayout();
			this.splitContainer1.Panel2.ResumeLayout(false);
			this.splitContainer1.Panel2.PerformLayout();
			((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).EndInit();
			this.splitContainer1.ResumeLayout(false);
			((System.ComponentModel.ISupportInitialize)(this.gridUsers)).EndInit();
			((System.ComponentModel.ISupportInitialize)(this.gridRoles)).EndInit();
			this.menuStrip1.ResumeLayout(false);
			this.menuStrip1.PerformLayout();
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.SplitContainer splitContainer1;
		private System.Windows.Forms.Label usersTitle;
		private System.Windows.Forms.DataGridView gridUsers;
		private System.Windows.Forms.DataGridViewTextBoxColumn UserNameColumn;
		private System.Windows.Forms.Label roleTitle;
		private System.Windows.Forms.DataGridView gridRoles;
		private System.Windows.Forms.DataGridViewTextBoxColumn RoleNameColumn;
		private System.Windows.Forms.Button refreshUserButton;
		private System.Windows.Forms.Button createUserButton;
		private System.Windows.Forms.MenuStrip menuStrip1;
		private System.Windows.Forms.ToolStripMenuItem userToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem createUserMenuItem;
		private System.Windows.Forms.ToolStripMenuItem dropUserMenuItem;
		private System.Windows.Forms.ToolStripMenuItem changePasswordMenuItem;
		private System.Windows.Forms.ToolStripMenuItem editUserMenuItem;
	}
}
