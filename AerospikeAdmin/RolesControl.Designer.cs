namespace Aerospike.Admin
{
	partial class RolesControl
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
            this.label1 = new System.Windows.Forms.Label();
            this.gridRoles = new System.Windows.Forms.DataGridView();
            this.RoleNameColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.writeQuotaBox = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.readQuotaBox = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.whitelistBox = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.titleRole = new System.Windows.Forms.Label();
            this.gridPrivs = new System.Windows.Forms.DataGridView();
            this.PrivilegeCodeColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.NamespaceColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.SetNameColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.button2 = new System.Windows.Forms.Button();
            this.createButton = new System.Windows.Forms.Button();
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.userToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.createMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.dropMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).BeginInit();
            this.splitContainer1.Panel1.SuspendLayout();
            this.splitContainer1.Panel2.SuspendLayout();
            this.splitContainer1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.gridRoles)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.gridPrivs)).BeginInit();
            this.menuStrip1.SuspendLayout();
            this.SuspendLayout();
            // 
            // splitContainer1
            // 
            this.splitContainer1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.splitContainer1.Location = new System.Drawing.Point(6, 66);
            this.splitContainer1.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.splitContainer1.Name = "splitContainer1";
            // 
            // splitContainer1.Panel1
            // 
            this.splitContainer1.Panel1.Controls.Add(this.label1);
            this.splitContainer1.Panel1.Controls.Add(this.gridRoles);
            // 
            // splitContainer1.Panel2
            // 
            this.splitContainer1.Panel2.Controls.Add(this.writeQuotaBox);
            this.splitContainer1.Panel2.Controls.Add(this.label4);
            this.splitContainer1.Panel2.Controls.Add(this.readQuotaBox);
            this.splitContainer1.Panel2.Controls.Add(this.label3);
            this.splitContainer1.Panel2.Controls.Add(this.whitelistBox);
            this.splitContainer1.Panel2.Controls.Add(this.label2);
            this.splitContainer1.Panel2.Controls.Add(this.titleRole);
            this.splitContainer1.Panel2.Controls.Add(this.gridPrivs);
            this.splitContainer1.Size = new System.Drawing.Size(1164, 824);
            this.splitContainer1.SplitterDistance = 335;
            this.splitContainer1.SplitterWidth = 9;
            this.splitContainer1.TabIndex = 10;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
            this.label1.Location = new System.Drawing.Point(4, 5);
            this.label1.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(81, 29);
            this.label1.TabIndex = 2;
            this.label1.Text = "Roles";
            // 
            // gridRoles
            // 
            this.gridRoles.AllowUserToDeleteRows = false;
            this.gridRoles.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gridRoles.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.RoleNameColumn});
            this.gridRoles.Dock = System.Windows.Forms.DockStyle.Fill;
            this.gridRoles.Location = new System.Drawing.Point(0, 0);
            this.gridRoles.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.gridRoles.MultiSelect = false;
            this.gridRoles.Name = "gridRoles";
            this.gridRoles.RowHeadersVisible = false;
            this.gridRoles.RowHeadersWidth = 82;
            this.gridRoles.Size = new System.Drawing.Size(335, 824);
            this.gridRoles.TabIndex = 10;
            this.gridRoles.RowEnter += new System.Windows.Forms.DataGridViewCellEventHandler(this.RoleRowEnter);
            this.gridRoles.MouseClick += new System.Windows.Forms.MouseEventHandler(this.RoleClicked);
            // 
            // RoleNameColumn
            // 
            this.RoleNameColumn.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.RoleNameColumn.HeaderText = "Name";
            this.RoleNameColumn.MinimumWidth = 10;
            this.RoleNameColumn.Name = "RoleNameColumn";
            // 
            // writeQuotaBox
            // 
            this.writeQuotaBox.Enabled = false;
            this.writeQuotaBox.Location = new System.Drawing.Point(483, 124);
            this.writeQuotaBox.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.writeQuotaBox.Name = "writeQuotaBox";
            this.writeQuotaBox.RightToLeft = System.Windows.Forms.RightToLeft.Yes;
            this.writeQuotaBox.Size = new System.Drawing.Size(169, 39);
            this.writeQuotaBox.TabIndex = 15;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(340, 126);
            this.label4.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(144, 32);
            this.label4.TabIndex = 16;
            this.label4.Text = "Write Quota";
            // 
            // readQuotaBox
            // 
            this.readQuotaBox.Enabled = false;
            this.readQuotaBox.Location = new System.Drawing.Point(154, 124);
            this.readQuotaBox.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.readQuotaBox.Name = "readQuotaBox";
            this.readQuotaBox.RightToLeft = System.Windows.Forms.RightToLeft.Yes;
            this.readQuotaBox.Size = new System.Drawing.Size(169, 39);
            this.readQuotaBox.TabIndex = 13;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(11, 126);
            this.label3.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(139, 32);
            this.label3.TabIndex = 14;
            this.label3.Text = "Read Quota";
            // 
            // whitelistBox
            // 
            this.whitelistBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.whitelistBox.Enabled = false;
            this.whitelistBox.Location = new System.Drawing.Point(154, 55);
            this.whitelistBox.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.whitelistBox.Name = "whitelistBox";
            this.whitelistBox.Size = new System.Drawing.Size(599, 39);
            this.whitelistBox.TabIndex = 10;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(11, 62);
            this.label2.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(107, 32);
            this.label2.TabIndex = 12;
            this.label2.Text = "Whitelist";
            // 
            // titleRole
            // 
            this.titleRole.AutoSize = true;
            this.titleRole.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
            this.titleRole.Location = new System.Drawing.Point(4, 182);
            this.titleRole.Margin = new System.Windows.Forms.Padding(6, 0, 6, 0);
            this.titleRole.Name = "titleRole";
            this.titleRole.Size = new System.Drawing.Size(130, 29);
            this.titleRole.TabIndex = 3;
            this.titleRole.Text = "Privileges";
            // 
            // gridPrivs
            // 
            this.gridPrivs.AllowUserToAddRows = false;
            this.gridPrivs.AllowUserToDeleteRows = false;
            this.gridPrivs.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.gridPrivs.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gridPrivs.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.PrivilegeCodeColumn,
            this.NamespaceColumn,
            this.SetNameColumn});
            this.gridPrivs.Location = new System.Drawing.Point(6, 226);
            this.gridPrivs.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.gridPrivs.MultiSelect = false;
            this.gridPrivs.Name = "gridPrivs";
            this.gridPrivs.ReadOnly = true;
            this.gridPrivs.RowHeadersVisible = false;
            this.gridPrivs.RowHeadersWidth = 82;
            this.gridPrivs.Size = new System.Drawing.Size(793, 597);
            this.gridPrivs.TabIndex = 11;
            this.gridPrivs.SelectionChanged += new System.EventHandler(this.PrivSelectionChanged);
            // 
            // PrivilegeCodeColumn
            // 
            this.PrivilegeCodeColumn.HeaderText = "Privilege Code";
            this.PrivilegeCodeColumn.MinimumWidth = 10;
            this.PrivilegeCodeColumn.Name = "PrivilegeCodeColumn";
            this.PrivilegeCodeColumn.ReadOnly = true;
            this.PrivilegeCodeColumn.Resizable = System.Windows.Forms.DataGridViewTriState.True;
            this.PrivilegeCodeColumn.SortMode = System.Windows.Forms.DataGridViewColumnSortMode.NotSortable;
            this.PrivilegeCodeColumn.Width = 200;
            // 
            // NamespaceColumn
            // 
            this.NamespaceColumn.HeaderText = "Namespace";
            this.NamespaceColumn.MinimumWidth = 10;
            this.NamespaceColumn.Name = "NamespaceColumn";
            this.NamespaceColumn.ReadOnly = true;
            this.NamespaceColumn.Width = 135;
            // 
            // SetNameColumn
            // 
            this.SetNameColumn.HeaderText = "Set Name";
            this.SetNameColumn.MinimumWidth = 10;
            this.SetNameColumn.Name = "SetNameColumn";
            this.SetNameColumn.ReadOnly = true;
            this.SetNameColumn.Width = 135;
            // 
            // button2
            // 
            this.button2.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.button2.Location = new System.Drawing.Point(179, 905);
            this.button2.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(162, 57);
            this.button2.TabIndex = 8;
            this.button2.Text = "Refresh";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.RefreshClicked);
            // 
            // createButton
            // 
            this.createButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.createButton.Location = new System.Drawing.Point(0, 905);
            this.createButton.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.createButton.Name = "createButton";
            this.createButton.Size = new System.Drawing.Size(162, 57);
            this.createButton.TabIndex = 7;
            this.createButton.Text = "Create Role";
            this.createButton.UseVisualStyleBackColor = true;
            this.createButton.Click += new System.EventHandler(this.CreateRoleClicked);
            // 
            // menuStrip1
            // 
            this.menuStrip1.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.userToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Padding = new System.Windows.Forms.Padding(13, 5, 0, 5);
            this.menuStrip1.Size = new System.Drawing.Size(1176, 46);
            this.menuStrip1.TabIndex = 9;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // userToolStripMenuItem
            // 
            this.userToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.createMenuItem,
            this.dropMenuItem,
            this.editMenuItem});
            this.userToolStripMenuItem.Name = "userToolStripMenuItem";
            this.userToolStripMenuItem.Size = new System.Drawing.Size(80, 36);
            this.userToolStripMenuItem.Text = "Role";
            // 
            // createMenuItem
            // 
            this.createMenuItem.Name = "createMenuItem";
            this.createMenuItem.Size = new System.Drawing.Size(216, 44);
            this.createMenuItem.Text = "Create";
            this.createMenuItem.Click += new System.EventHandler(this.CreateRoleClicked);
            // 
            // dropMenuItem
            // 
            this.dropMenuItem.Name = "dropMenuItem";
            this.dropMenuItem.Size = new System.Drawing.Size(216, 44);
            this.dropMenuItem.Text = "Drop";
            this.dropMenuItem.Click += new System.EventHandler(this.DropClicked);
            // 
            // editMenuItem
            // 
            this.editMenuItem.Name = "editMenuItem";
            this.editMenuItem.Size = new System.Drawing.Size(216, 44);
            this.editMenuItem.Text = "Edit";
            this.editMenuItem.Click += new System.EventHandler(this.EditClicked);
            // 
            // RolesControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(13F, 32F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.splitContainer1);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.createButton);
            this.Controls.Add(this.menuStrip1);
            this.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.Name = "RolesControl";
            this.Size = new System.Drawing.Size(1176, 969);
            this.splitContainer1.Panel1.ResumeLayout(false);
            this.splitContainer1.Panel1.PerformLayout();
            this.splitContainer1.Panel2.ResumeLayout(false);
            this.splitContainer1.Panel2.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).EndInit();
            this.splitContainer1.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.gridRoles)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.gridPrivs)).EndInit();
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.SplitContainer splitContainer1;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.DataGridView gridRoles;
		private System.Windows.Forms.DataGridViewTextBoxColumn RoleNameColumn;
		private System.Windows.Forms.Label titleRole;
		private System.Windows.Forms.DataGridView gridPrivs;
		private System.Windows.Forms.DataGridViewTextBoxColumn PrivilegeCodeColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn NamespaceColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn SetNameColumn;
		private System.Windows.Forms.Button button2;
		private System.Windows.Forms.Button createButton;
		private System.Windows.Forms.MenuStrip menuStrip1;
		private System.Windows.Forms.ToolStripMenuItem userToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem createMenuItem;
		private System.Windows.Forms.ToolStripMenuItem dropMenuItem;
		private System.Windows.Forms.ToolStripMenuItem editMenuItem;
		private System.Windows.Forms.TextBox whitelistBox;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.TextBox writeQuotaBox;
		private System.Windows.Forms.Label label4;
		private System.Windows.Forms.TextBox readQuotaBox;
		private System.Windows.Forms.Label label3;
	}
}
