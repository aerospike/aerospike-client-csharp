namespace Aerospike.Admin
{
	partial class RoleEditForm
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
			this.nameBox = new System.Windows.Forms.TextBox();
			this.label1 = new System.Windows.Forms.Label();
			this.label3 = new System.Windows.Forms.Label();
			this.button1 = new System.Windows.Forms.Button();
			this.button2 = new System.Windows.Forms.Button();
			this.grid = new System.Windows.Forms.DataGridView();
			this.PrivilegeCodeColumn = new System.Windows.Forms.DataGridViewComboBoxColumn();
			this.NamespaceColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.SetNameColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.whiteListBox = new System.Windows.Forms.TextBox();
			this.label2 = new System.Windows.Forms.Label();
			((System.ComponentModel.ISupportInitialize)(this.grid)).BeginInit();
			this.SuspendLayout();
			// 
			// nameBox
			// 
			this.nameBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.nameBox.Location = new System.Drawing.Point(73, 12);
			this.nameBox.Name = "nameBox";
			this.nameBox.Size = new System.Drawing.Size(352, 20);
			this.nameBox.TabIndex = 0;
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(7, 15);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(60, 13);
			this.label1.TabIndex = 5;
			this.label1.Text = "Role Name";
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label3.Location = new System.Drawing.Point(7, 73);
			this.label3.Name = "label3";
			this.label3.RightToLeft = System.Windows.Forms.RightToLeft.No;
			this.label3.Size = new System.Drawing.Size(70, 15);
			this.label3.TabIndex = 12;
			this.label3.Text = "Privileges";
			// 
			// button1
			// 
			this.button1.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
			this.button1.Location = new System.Drawing.Point(260, 314);
			this.button1.Name = "button1";
			this.button1.Size = new System.Drawing.Size(75, 23);
			this.button1.TabIndex = 4;
			this.button1.Text = "Cancel";
			this.button1.UseVisualStyleBackColor = true;
			this.button1.Click += new System.EventHandler(this.CancelClicked);
			// 
			// button2
			// 
			this.button2.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
			this.button2.Location = new System.Drawing.Point(346, 314);
			this.button2.Name = "button2";
			this.button2.Size = new System.Drawing.Size(75, 23);
			this.button2.TabIndex = 5;
			this.button2.Text = "OK";
			this.button2.UseVisualStyleBackColor = true;
			this.button2.Click += new System.EventHandler(this.SaveClicked);
			// 
			// grid
			// 
			this.grid.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.grid.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.grid.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.PrivilegeCodeColumn,
            this.NamespaceColumn,
            this.SetNameColumn});
			this.grid.Location = new System.Drawing.Point(10, 95);
			this.grid.Name = "grid";
			this.grid.Size = new System.Drawing.Size(415, 212);
			this.grid.TabIndex = 3;
			// 
			// PrivilegeCodeColumn
			// 
			this.PrivilegeCodeColumn.HeaderText = "Privilege Code";
			this.PrivilegeCodeColumn.Name = "PrivilegeCodeColumn";
			// 
			// NamespaceColumn
			// 
			this.NamespaceColumn.HeaderText = "Namespace";
			this.NamespaceColumn.Name = "NamespaceColumn";
			this.NamespaceColumn.Width = 135;
			// 
			// SetNameColumn
			// 
			this.SetNameColumn.HeaderText = "Set Name";
			this.SetNameColumn.Name = "SetNameColumn";
			this.SetNameColumn.Width = 135;
			// 
			// whiteListBox
			// 
			this.whiteListBox.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.whiteListBox.Location = new System.Drawing.Point(73, 38);
			this.whiteListBox.Name = "whiteListBox";
			this.whiteListBox.Size = new System.Drawing.Size(352, 20);
			this.whiteListBox.TabIndex = 1;
			// 
			// label2
			// 
			this.label2.Location = new System.Drawing.Point(7, 38);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(60, 26);
			this.label2.TabIndex = 15;
			this.label2.Text = "Whitelist IP Addresses";
			// 
			// RoleEditForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.AutoSize = true;
			this.ClientSize = new System.Drawing.Size(434, 343);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.whiteListBox);
			this.Controls.Add(this.grid);
			this.Controls.Add(this.button2);
			this.Controls.Add(this.button1);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.nameBox);
			this.Controls.Add(this.label1);
			this.Name = "RoleEditForm";
			this.Text = "Create Role";
			((System.ComponentModel.ISupportInitialize)(this.grid)).EndInit();
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.TextBox nameBox;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.Label label3;
		private System.Windows.Forms.Button button1;
		private System.Windows.Forms.Button button2;
		private System.Windows.Forms.DataGridView grid;
		private System.Windows.Forms.DataGridViewComboBoxColumn PrivilegeCodeColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn NamespaceColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn SetNameColumn;
		private System.Windows.Forms.TextBox whiteListBox;
		private System.Windows.Forms.Label label2;
	}
}