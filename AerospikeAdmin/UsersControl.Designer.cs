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
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle19 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle20 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle21 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle22 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle23 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle24 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle25 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle26 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle27 = new System.Windows.Forms.DataGridViewCellStyle();
            this.refreshUserButton = new System.Windows.Forms.Button();
            this.createUserButton = new System.Windows.Forms.Button();
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.userToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.createUserMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.dropUserMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.changePasswordMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editUserMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.gridUsers = new System.Windows.Forms.DataGridView();
            this.NameColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.RolesColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.ConnsInUseColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.ReadQuotaColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.ReadTpsColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.ReadQueryColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.ReadLimitlessColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.WriteQuotaColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.WriteTpsColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.WriteQueryColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.WriteLimitlessColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.menuStrip1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.gridUsers)).BeginInit();
            this.SuspendLayout();
            // 
            // refreshUserButton
            // 
            this.refreshUserButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.refreshUserButton.Location = new System.Drawing.Point(174, 885);
            this.refreshUserButton.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.refreshUserButton.Name = "refreshUserButton";
            this.refreshUserButton.Size = new System.Drawing.Size(162, 57);
            this.refreshUserButton.TabIndex = 2;
            this.refreshUserButton.Text = "Refresh";
            this.refreshUserButton.UseVisualStyleBackColor = true;
            this.refreshUserButton.Click += new System.EventHandler(this.RefreshClicked);
            // 
            // createUserButton
            // 
            this.createUserButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.createUserButton.Location = new System.Drawing.Point(0, 885);
            this.createUserButton.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.createUserButton.Name = "createUserButton";
            this.createUserButton.Size = new System.Drawing.Size(162, 57);
            this.createUserButton.TabIndex = 1;
            this.createUserButton.Text = "Create User";
            this.createUserButton.UseVisualStyleBackColor = true;
            this.createUserButton.Click += new System.EventHandler(this.CreateUserClicked);
            // 
            // menuStrip1
            // 
            this.menuStrip1.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.userToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Padding = new System.Windows.Forms.Padding(13, 5, 0, 5);
            this.menuStrip1.Size = new System.Drawing.Size(1567, 46);
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
            this.userToolStripMenuItem.Size = new System.Drawing.Size(81, 36);
            this.userToolStripMenuItem.Text = "User";
            // 
            // createUserMenuItem
            // 
            this.createUserMenuItem.Name = "createUserMenuItem";
            this.createUserMenuItem.Size = new System.Drawing.Size(333, 44);
            this.createUserMenuItem.Text = "Create";
            this.createUserMenuItem.Click += new System.EventHandler(this.CreateUserClicked);
            // 
            // dropUserMenuItem
            // 
            this.dropUserMenuItem.Name = "dropUserMenuItem";
            this.dropUserMenuItem.Size = new System.Drawing.Size(333, 44);
            this.dropUserMenuItem.Text = "Drop";
            this.dropUserMenuItem.Click += new System.EventHandler(this.DropUserClicked);
            // 
            // changePasswordMenuItem
            // 
            this.changePasswordMenuItem.Name = "changePasswordMenuItem";
            this.changePasswordMenuItem.Size = new System.Drawing.Size(333, 44);
            this.changePasswordMenuItem.Text = "Change Password";
            this.changePasswordMenuItem.Click += new System.EventHandler(this.ChangePasswordClicked);
            // 
            // editUserMenuItem
            // 
            this.editUserMenuItem.Name = "editUserMenuItem";
            this.editUserMenuItem.Size = new System.Drawing.Size(333, 44);
            this.editUserMenuItem.Text = "Edit";
            this.editUserMenuItem.Click += new System.EventHandler(this.EditRolesClicked);
            // 
            // gridUsers
            // 
            this.gridUsers.AllowUserToAddRows = false;
            this.gridUsers.AllowUserToDeleteRows = false;
            this.gridUsers.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.gridUsers.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gridUsers.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.NameColumn,
            this.RolesColumn,
            this.ConnsInUseColumn,
            this.ReadQuotaColumn,
            this.ReadTpsColumn,
            this.ReadQueryColumn,
            this.ReadLimitlessColumn,
            this.WriteQuotaColumn,
            this.WriteTpsColumn,
            this.WriteQueryColumn,
            this.WriteLimitlessColumn});
            this.gridUsers.Location = new System.Drawing.Point(6, 67);
            this.gridUsers.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.gridUsers.MultiSelect = false;
            this.gridUsers.Name = "gridUsers";
            this.gridUsers.ReadOnly = true;
            this.gridUsers.RowHeadersVisible = false;
            this.gridUsers.RowHeadersWidth = 82;
            this.gridUsers.Size = new System.Drawing.Size(1545, 804);
            this.gridUsers.TabIndex = 12;
            this.gridUsers.MouseClick += new System.Windows.Forms.MouseEventHandler(this.UserClicked);
            // 
            // NameColumn
            // 
            this.NameColumn.HeaderText = "Name";
            this.NameColumn.MinimumWidth = 10;
            this.NameColumn.Name = "NameColumn";
            this.NameColumn.ReadOnly = true;
            this.NameColumn.Resizable = System.Windows.Forms.DataGridViewTriState.True;
            this.NameColumn.SortMode = System.Windows.Forms.DataGridViewColumnSortMode.NotSortable;
            this.NameColumn.Width = 200;
            // 
            // RolesColumn
            // 
            this.RolesColumn.HeaderText = "Roles";
            this.RolesColumn.MinimumWidth = 10;
            this.RolesColumn.Name = "RolesColumn";
            this.RolesColumn.ReadOnly = true;
            this.RolesColumn.Width = 300;
            // 
            // ConnsInUseColumn
            // 
            dataGridViewCellStyle19.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.ConnsInUseColumn.DefaultCellStyle = dataGridViewCellStyle19;
            this.ConnsInUseColumn.HeaderText = "Conns In Use";
            this.ConnsInUseColumn.MinimumWidth = 10;
            this.ConnsInUseColumn.Name = "ConnsInUseColumn";
            this.ConnsInUseColumn.ReadOnly = true;
            this.ConnsInUseColumn.Width = 90;
            // 
            // ReadQuotaColumn
            // 
            dataGridViewCellStyle20.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.ReadQuotaColumn.DefaultCellStyle = dataGridViewCellStyle20;
            this.ReadQuotaColumn.HeaderText = "Read Quota";
            this.ReadQuotaColumn.MinimumWidth = 10;
            this.ReadQuotaColumn.Name = "ReadQuotaColumn";
            this.ReadQuotaColumn.ReadOnly = true;
            this.ReadQuotaColumn.Width = 80;
            // 
            // ReadTpsColumn
            // 
            dataGridViewCellStyle21.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.ReadTpsColumn.DefaultCellStyle = dataGridViewCellStyle21;
            this.ReadTpsColumn.HeaderText = "Read TPS";
            this.ReadTpsColumn.MinimumWidth = 10;
            this.ReadTpsColumn.Name = "ReadTpsColumn";
            this.ReadTpsColumn.ReadOnly = true;
            this.ReadTpsColumn.Width = 80;
            // 
            // ReadQueryColumn
            // 
            dataGridViewCellStyle22.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.ReadQueryColumn.DefaultCellStyle = dataGridViewCellStyle22;
            this.ReadQueryColumn.HeaderText = "Query Read RPS";
            this.ReadQueryColumn.MinimumWidth = 10;
            this.ReadQueryColumn.Name = "ReadQueryColumn";
            this.ReadQueryColumn.ReadOnly = true;
            this.ReadQueryColumn.Width = 130;
            // 
            // ReadLimitlessColumn
            // 
            dataGridViewCellStyle23.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.ReadLimitlessColumn.DefaultCellStyle = dataGridViewCellStyle23;
            this.ReadLimitlessColumn.HeaderText = "Read Limitless";
            this.ReadLimitlessColumn.MinimumWidth = 10;
            this.ReadLimitlessColumn.Name = "ReadLimitlessColumn";
            this.ReadLimitlessColumn.ReadOnly = true;
            this.ReadLimitlessColumn.Width = 200;
            // 
            // WriteQuotaColumn
            // 
            dataGridViewCellStyle24.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.WriteQuotaColumn.DefaultCellStyle = dataGridViewCellStyle24;
            this.WriteQuotaColumn.HeaderText = "Write Quota";
            this.WriteQuotaColumn.MinimumWidth = 10;
            this.WriteQuotaColumn.Name = "WriteQuotaColumn";
            this.WriteQuotaColumn.ReadOnly = true;
            this.WriteQuotaColumn.Width = 80;
            // 
            // WriteTpsColumn
            // 
            dataGridViewCellStyle25.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.WriteTpsColumn.DefaultCellStyle = dataGridViewCellStyle25;
            this.WriteTpsColumn.HeaderText = "Write TPS";
            this.WriteTpsColumn.MinimumWidth = 10;
            this.WriteTpsColumn.Name = "WriteTpsColumn";
            this.WriteTpsColumn.ReadOnly = true;
            this.WriteTpsColumn.Width = 80;
            // 
            // WriteQueryColumn
            // 
            dataGridViewCellStyle26.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.WriteQueryColumn.DefaultCellStyle = dataGridViewCellStyle26;
            this.WriteQueryColumn.HeaderText = "Query Write RPS";
            this.WriteQueryColumn.MinimumWidth = 10;
            this.WriteQueryColumn.Name = "WriteQueryColumn";
            this.WriteQueryColumn.ReadOnly = true;
            this.WriteQueryColumn.Width = 130;
            // 
            // WriteLimitlessColumn
            // 
            dataGridViewCellStyle27.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleRight;
            this.WriteLimitlessColumn.DefaultCellStyle = dataGridViewCellStyle27;
            this.WriteLimitlessColumn.HeaderText = "Write Limitless";
            this.WriteLimitlessColumn.MinimumWidth = 10;
            this.WriteLimitlessColumn.Name = "WriteLimitlessColumn";
            this.WriteLimitlessColumn.ReadOnly = true;
            this.WriteLimitlessColumn.Width = 200;
            // 
            // UsersControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(13F, 32F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.gridUsers);
            this.Controls.Add(this.menuStrip1);
            this.Controls.Add(this.refreshUserButton);
            this.Controls.Add(this.createUserButton);
            this.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.Name = "UsersControl";
            this.Size = new System.Drawing.Size(1567, 949);
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.gridUsers)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

		}

		#endregion
		private System.Windows.Forms.Button refreshUserButton;
		private System.Windows.Forms.Button createUserButton;
		private System.Windows.Forms.MenuStrip menuStrip1;
		private System.Windows.Forms.ToolStripMenuItem userToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem createUserMenuItem;
		private System.Windows.Forms.ToolStripMenuItem dropUserMenuItem;
		private System.Windows.Forms.ToolStripMenuItem changePasswordMenuItem;
		private System.Windows.Forms.ToolStripMenuItem editUserMenuItem;
		private System.Windows.Forms.DataGridView gridUsers;
        private System.Windows.Forms.DataGridViewTextBoxColumn NameColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn RolesColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn ConnsInUseColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn ReadQuotaColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn ReadTpsColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn ReadQueryColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn ReadLimitlessColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn WriteQuotaColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn WriteTpsColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn WriteQueryColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn WriteLimitlessColumn;
    }
}
