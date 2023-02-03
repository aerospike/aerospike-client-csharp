namespace Aerospike.Admin
{
	partial class AdminForm
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
            this.sideView = new System.Windows.Forms.TreeView();
            this.usersControl = new Aerospike.Admin.UsersControl();
            this.rolesControl = new Aerospike.Admin.RolesControl();
            this.SuspendLayout();
            // 
            // sideView
            // 
            this.sideView.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
            this.sideView.BackColor = System.Drawing.SystemColors.ControlLight;
            this.sideView.Font = new System.Drawing.Font("Arial", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
            this.sideView.FullRowSelect = true;
            this.sideView.HideSelection = false;
            this.sideView.Indent = 5;
            this.sideView.Location = new System.Drawing.Point(0, 0);
            this.sideView.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.sideView.Name = "sideView";
            this.sideView.ShowLines = false;
            this.sideView.Size = new System.Drawing.Size(166, 1149);
            this.sideView.TabIndex = 1;
            this.sideView.AfterSelect += new System.Windows.Forms.TreeViewEventHandler(this.ExampleSelected);
            // 
            // usersControl
            // 
            this.usersControl.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.usersControl.Location = new System.Drawing.Point(170, 0);
            this.usersControl.Margin = new System.Windows.Forms.Padding(13, 17, 13, 17);
            this.usersControl.Name = "usersControl";
            this.usersControl.Size = new System.Drawing.Size(1755, 1140);
            this.usersControl.TabIndex = 2;
            // 
            // rolesControl
            // 
            this.rolesControl.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.rolesControl.Location = new System.Drawing.Point(184, 0);
            this.rolesControl.Margin = new System.Windows.Forms.Padding(13, 17, 13, 17);
            this.rolesControl.Name = "rolesControl";
            this.rolesControl.Size = new System.Drawing.Size(1575, 1140);
            this.rolesControl.TabIndex = 3;
            // 
            // AdminForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(13F, 32F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1755, 1140);
            this.Controls.Add(this.sideView);
            this.Controls.Add(this.rolesControl);
            this.Controls.Add(this.usersControl);
            this.Margin = new System.Windows.Forms.Padding(6, 7, 6, 7);
            this.Name = "AdminForm";
            this.Text = "Aerospike User Administration";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FormClose);
            this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.TreeView sideView;
		private UsersControl usersControl;
		private RolesControl rolesControl;
	}
}