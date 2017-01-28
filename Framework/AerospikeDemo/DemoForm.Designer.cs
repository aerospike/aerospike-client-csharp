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
			this.splitContainer1 = new System.Windows.Forms.SplitContainer();
			this.benchmarkPanel = new System.Windows.Forms.Panel();
			this.retryOnTimeoutBox = new System.Windows.Forms.CheckBox();
			this.initializePanel = new System.Windows.Forms.Panel();
			this.initPctLabel = new System.Windows.Forms.Label();
			this.initPctBox = new System.Windows.Forms.TextBox();
			this.label5 = new System.Windows.Forms.Label();
			this.workloadPanel = new System.Windows.Forms.Panel();
			this.label18 = new System.Windows.Forms.Label();
			this.replicaBox = new System.Windows.Forms.ComboBox();
			this.label10 = new System.Windows.Forms.Label();
			this.writeBox = new System.Windows.Forms.TextBox();
			this.label11 = new System.Windows.Forms.Label();
			this.label9 = new System.Windows.Forms.Label();
			this.readBox = new System.Windows.Forms.TextBox();
			this.label8 = new System.Windows.Forms.Label();
			this.label7 = new System.Windows.Forms.Label();
			this.panel1 = new System.Windows.Forms.Panel();
			this.dynamicValueButton = new System.Windows.Forms.RadioButton();
			this.fixedValueButton = new System.Windows.Forms.RadioButton();
			this.groupBox1 = new System.Windows.Forms.GroupBox();
			this.threadPanel = new System.Windows.Forms.Panel();
			this.syncThreadBox = new System.Windows.Forms.TextBox();
			this.label25 = new System.Windows.Forms.Label();
			this.asyncButton = new System.Windows.Forms.RadioButton();
			this.syncButton = new System.Windows.Forms.RadioButton();
			this.maxCommandPanel = new System.Windows.Forms.Panel();
			this.asyncThreadBox = new System.Windows.Forms.TextBox();
			this.label26 = new System.Windows.Forms.Label();
			this.maxCommandBox = new System.Windows.Forms.TextBox();
			this.label27 = new System.Windows.Forms.Label();
			this.latencyGroup = new System.Windows.Forms.GroupBox();
			this.latencyDisplayLabel = new System.Windows.Forms.TextBox();
			this.latencyColumnsLabel = new System.Windows.Forms.Label();
			this.latencyColumnsBox = new System.Windows.Forms.TextBox();
			this.latencyShiftLabel = new System.Windows.Forms.Label();
			this.latencyShiftBox = new System.Windows.Forms.TextBox();
			this.label20 = new System.Windows.Forms.Label();
			this.debugBox = new System.Windows.Forms.CheckBox();
			this.latencyBox = new System.Windows.Forms.CheckBox();
			this.sleepBox = new System.Windows.Forms.TextBox();
			this.label17 = new System.Windows.Forms.Label();
			this.maxRetriesBox = new System.Windows.Forms.TextBox();
			this.label16 = new System.Windows.Forms.Label();
			this.label15 = new System.Windows.Forms.Label();
			this.timeoutBox = new System.Windows.Forms.TextBox();
			this.label14 = new System.Windows.Forms.Label();
			this.binSizeBox = new System.Windows.Forms.TextBox();
			this.binSizeLabel = new System.Windows.Forms.Label();
			this.binTypeBox = new System.Windows.Forms.ComboBox();
			this.label6 = new System.Windows.Forms.Label();
			this.recordsBox = new System.Windows.Forms.TextBox();
			this.label24 = new System.Windows.Forms.Label();
			this.codeBox = new System.Windows.Forms.RichTextBox();
			this.consoleBox = new System.Windows.Forms.TextBox();
			this.stopButton = new System.Windows.Forms.Button();
			this.runButton = new System.Windows.Forms.Button();
			this.nsBox = new System.Windows.Forms.TextBox();
			this.label3 = new System.Windows.Forms.Label();
			this.portBox = new System.Windows.Forms.TextBox();
			this.label2 = new System.Windows.Forms.Label();
			this.label1 = new System.Windows.Forms.Label();
			this.hostBox = new System.Windows.Forms.TextBox();
			this.setBox = new System.Windows.Forms.TextBox();
			this.label4 = new System.Windows.Forms.Label();
			this.examplesView = new System.Windows.Forms.TreeView();
			this.passwordBox = new System.Windows.Forms.TextBox();
			this.label12 = new System.Windows.Forms.Label();
			this.userBox = new System.Windows.Forms.TextBox();
			this.label13 = new System.Windows.Forms.Label();
			((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).BeginInit();
			this.splitContainer1.Panel1.SuspendLayout();
			this.splitContainer1.Panel2.SuspendLayout();
			this.splitContainer1.SuspendLayout();
			this.benchmarkPanel.SuspendLayout();
			this.initializePanel.SuspendLayout();
			this.workloadPanel.SuspendLayout();
			this.panel1.SuspendLayout();
			this.groupBox1.SuspendLayout();
			this.threadPanel.SuspendLayout();
			this.maxCommandPanel.SuspendLayout();
			this.latencyGroup.SuspendLayout();
			this.SuspendLayout();
			// 
			// splitContainer1
			// 
			this.splitContainer1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.splitContainer1.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
			this.splitContainer1.Location = new System.Drawing.Point(155, 32);
			this.splitContainer1.Name = "splitContainer1";
			this.splitContainer1.Orientation = System.Windows.Forms.Orientation.Horizontal;
			// 
			// splitContainer1.Panel1
			// 
			this.splitContainer1.Panel1.Controls.Add(this.benchmarkPanel);
			this.splitContainer1.Panel1.Controls.Add(this.codeBox);
			// 
			// splitContainer1.Panel2
			// 
			this.splitContainer1.Panel2.Controls.Add(this.consoleBox);
			this.splitContainer1.Panel2.Controls.Add(this.stopButton);
			this.splitContainer1.Panel2.Controls.Add(this.runButton);
			this.splitContainer1.Size = new System.Drawing.Size(1026, 753);
			this.splitContainer1.SplitterDistance = 399;
			this.splitContainer1.SplitterWidth = 6;
			this.splitContainer1.TabIndex = 0;
			// 
			// benchmarkPanel
			// 
			this.benchmarkPanel.Controls.Add(this.retryOnTimeoutBox);
			this.benchmarkPanel.Controls.Add(this.initializePanel);
			this.benchmarkPanel.Controls.Add(this.workloadPanel);
			this.benchmarkPanel.Controls.Add(this.panel1);
			this.benchmarkPanel.Controls.Add(this.groupBox1);
			this.benchmarkPanel.Controls.Add(this.latencyGroup);
			this.benchmarkPanel.Controls.Add(this.label20);
			this.benchmarkPanel.Controls.Add(this.debugBox);
			this.benchmarkPanel.Controls.Add(this.latencyBox);
			this.benchmarkPanel.Controls.Add(this.sleepBox);
			this.benchmarkPanel.Controls.Add(this.label17);
			this.benchmarkPanel.Controls.Add(this.maxRetriesBox);
			this.benchmarkPanel.Controls.Add(this.label16);
			this.benchmarkPanel.Controls.Add(this.label15);
			this.benchmarkPanel.Controls.Add(this.timeoutBox);
			this.benchmarkPanel.Controls.Add(this.label14);
			this.benchmarkPanel.Controls.Add(this.binSizeBox);
			this.benchmarkPanel.Controls.Add(this.binSizeLabel);
			this.benchmarkPanel.Controls.Add(this.binTypeBox);
			this.benchmarkPanel.Controls.Add(this.label6);
			this.benchmarkPanel.Controls.Add(this.recordsBox);
			this.benchmarkPanel.Controls.Add(this.label24);
			this.benchmarkPanel.Location = new System.Drawing.Point(0, 0);
			this.benchmarkPanel.Name = "benchmarkPanel";
			this.benchmarkPanel.Size = new System.Drawing.Size(540, 390);
			this.benchmarkPanel.TabIndex = 9;
			this.benchmarkPanel.Visible = false;
			// 
			// retryOnTimeoutBox
			// 
			this.retryOnTimeoutBox.AutoSize = true;
			this.retryOnTimeoutBox.Location = new System.Drawing.Point(176, 226);
			this.retryOnTimeoutBox.Name = "retryOnTimeoutBox";
			this.retryOnTimeoutBox.Size = new System.Drawing.Size(109, 17);
			this.retryOnTimeoutBox.TabIndex = 111;
			this.retryOnTimeoutBox.Text = "Retry On Timeout";
			this.retryOnTimeoutBox.UseVisualStyleBackColor = true;
			// 
			// initializePanel
			// 
			this.initializePanel.Controls.Add(this.initPctLabel);
			this.initializePanel.Controls.Add(this.initPctBox);
			this.initializePanel.Controls.Add(this.label5);
			this.initializePanel.Location = new System.Drawing.Point(14, 165);
			this.initializePanel.Name = "initializePanel";
			this.initializePanel.Size = new System.Drawing.Size(161, 28);
			this.initializePanel.TabIndex = 110;
			// 
			// initPctLabel
			// 
			this.initPctLabel.AutoSize = true;
			this.initPctLabel.Location = new System.Drawing.Point(134, 6);
			this.initPctLabel.Name = "initPctLabel";
			this.initPctLabel.Size = new System.Drawing.Size(15, 13);
			this.initPctLabel.TabIndex = 107;
			this.initPctLabel.Text = "%";
			// 
			// initPctBox
			// 
			this.initPctBox.Location = new System.Drawing.Point(89, 3);
			this.initPctBox.Name = "initPctBox";
			this.initPctBox.Size = new System.Drawing.Size(45, 20);
			this.initPctBox.TabIndex = 106;
			this.initPctBox.Text = "100";
			this.initPctBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label5
			// 
			this.label5.AutoSize = true;
			this.label5.Location = new System.Drawing.Point(3, 6);
			this.label5.Name = "label5";
			this.label5.Size = new System.Drawing.Size(84, 13);
			this.label5.TabIndex = 88;
			this.label5.Text = "Initialize Percent";
			// 
			// workloadPanel
			// 
			this.workloadPanel.Controls.Add(this.label18);
			this.workloadPanel.Controls.Add(this.replicaBox);
			this.workloadPanel.Controls.Add(this.label10);
			this.workloadPanel.Controls.Add(this.writeBox);
			this.workloadPanel.Controls.Add(this.label11);
			this.workloadPanel.Controls.Add(this.label9);
			this.workloadPanel.Controls.Add(this.readBox);
			this.workloadPanel.Controls.Add(this.label8);
			this.workloadPanel.Controls.Add(this.label7);
			this.workloadPanel.Location = new System.Drawing.Point(14, 165);
			this.workloadPanel.Name = "workloadPanel";
			this.workloadPanel.Size = new System.Drawing.Size(478, 28);
			this.workloadPanel.TabIndex = 108;
			// 
			// label18
			// 
			this.label18.AutoSize = true;
			this.label18.Location = new System.Drawing.Point(255, 7);
			this.label18.Name = "label18";
			this.label18.Size = new System.Drawing.Size(72, 13);
			this.label18.TabIndex = 115;
			this.label18.Text = "Read Replica";
			// 
			// replicaBox
			// 
			this.replicaBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
			this.replicaBox.FormattingEnabled = true;
			this.replicaBox.Location = new System.Drawing.Point(333, 2);
			this.replicaBox.Name = "replicaBox";
			this.replicaBox.Size = new System.Drawing.Size(118, 21);
			this.replicaBox.TabIndex = 114;
			// 
			// label10
			// 
			this.label10.AutoSize = true;
			this.label10.Location = new System.Drawing.Point(225, 6);
			this.label10.Name = "label10";
			this.label10.Size = new System.Drawing.Size(15, 13);
			this.label10.TabIndex = 91;
			this.label10.Text = "%";
			// 
			// writeBox
			// 
			this.writeBox.Location = new System.Drawing.Point(191, 3);
			this.writeBox.Name = "writeBox";
			this.writeBox.Size = new System.Drawing.Size(35, 20);
			this.writeBox.TabIndex = 86;
			this.writeBox.Text = "50";
			this.writeBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label11
			// 
			this.label11.AutoSize = true;
			this.label11.Location = new System.Drawing.Point(160, 6);
			this.label11.Name = "label11";
			this.label11.Size = new System.Drawing.Size(32, 13);
			this.label11.TabIndex = 90;
			this.label11.Text = "Write";
			// 
			// label9
			// 
			this.label9.AutoSize = true;
			this.label9.Location = new System.Drawing.Point(134, 6);
			this.label9.Name = "label9";
			this.label9.Size = new System.Drawing.Size(15, 13);
			this.label9.TabIndex = 89;
			this.label9.Text = "%";
			// 
			// readBox
			// 
			this.readBox.Location = new System.Drawing.Point(100, 3);
			this.readBox.Name = "readBox";
			this.readBox.Size = new System.Drawing.Size(35, 20);
			this.readBox.TabIndex = 85;
			this.readBox.Text = "50";
			this.readBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label8
			// 
			this.label8.AutoSize = true;
			this.label8.Location = new System.Drawing.Point(67, 6);
			this.label8.Name = "label8";
			this.label8.Size = new System.Drawing.Size(33, 13);
			this.label8.TabIndex = 88;
			this.label8.Text = "Read";
			// 
			// label7
			// 
			this.label7.AutoSize = true;
			this.label7.Location = new System.Drawing.Point(3, 6);
			this.label7.Name = "label7";
			this.label7.Size = new System.Drawing.Size(60, 13);
			this.label7.TabIndex = 87;
			this.label7.Text = "Work Load";
			// 
			// panel1
			// 
			this.panel1.Controls.Add(this.dynamicValueButton);
			this.panel1.Controls.Add(this.fixedValueButton);
			this.panel1.Location = new System.Drawing.Point(15, 136);
			this.panel1.Name = "panel1";
			this.panel1.Size = new System.Drawing.Size(246, 29);
			this.panel1.TabIndex = 107;
			// 
			// dynamicValueButton
			// 
			this.dynamicValueButton.AutoSize = true;
			this.dynamicValueButton.Location = new System.Drawing.Point(94, 6);
			this.dynamicValueButton.Name = "dynamicValueButton";
			this.dynamicValueButton.Size = new System.Drawing.Size(144, 17);
			this.dynamicValueButton.TabIndex = 4;
			this.dynamicValueButton.Text = "Dynamic Random Values";
			this.dynamicValueButton.UseVisualStyleBackColor = true;
			// 
			// fixedValueButton
			// 
			this.fixedValueButton.AutoSize = true;
			this.fixedValueButton.Checked = true;
			this.fixedValueButton.Location = new System.Drawing.Point(5, 6);
			this.fixedValueButton.Name = "fixedValueButton";
			this.fixedValueButton.Size = new System.Drawing.Size(80, 17);
			this.fixedValueButton.TabIndex = 3;
			this.fixedValueButton.TabStop = true;
			this.fixedValueButton.Text = "Fixed Value";
			this.fixedValueButton.UseVisualStyleBackColor = true;
			// 
			// groupBox1
			// 
			this.groupBox1.Controls.Add(this.threadPanel);
			this.groupBox1.Controls.Add(this.asyncButton);
			this.groupBox1.Controls.Add(this.syncButton);
			this.groupBox1.Controls.Add(this.maxCommandPanel);
			this.groupBox1.Location = new System.Drawing.Point(12, 3);
			this.groupBox1.Name = "groupBox1";
			this.groupBox1.Size = new System.Drawing.Size(380, 75);
			this.groupBox1.TabIndex = 106;
			this.groupBox1.TabStop = false;
			this.groupBox1.Text = "Connection Mode";
			// 
			// threadPanel
			// 
			this.threadPanel.Controls.Add(this.syncThreadBox);
			this.threadPanel.Controls.Add(this.label25);
			this.threadPanel.Location = new System.Drawing.Point(3, 42);
			this.threadPanel.Name = "threadPanel";
			this.threadPanel.Size = new System.Drawing.Size(160, 26);
			this.threadPanel.TabIndex = 66;
			// 
			// syncThreadBox
			// 
			this.syncThreadBox.Location = new System.Drawing.Point(68, 3);
			this.syncThreadBox.Name = "syncThreadBox";
			this.syncThreadBox.Size = new System.Drawing.Size(80, 20);
			this.syncThreadBox.TabIndex = 4;
			this.syncThreadBox.Text = "8";
			this.syncThreadBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label25
			// 
			this.label25.AutoSize = true;
			this.label25.Location = new System.Drawing.Point(0, 6);
			this.label25.Name = "label25";
			this.label25.Size = new System.Drawing.Size(46, 13);
			this.label25.TabIndex = 3;
			this.label25.Text = "Threads";
			// 
			// asyncButton
			// 
			this.asyncButton.AutoSize = true;
			this.asyncButton.Location = new System.Drawing.Point(99, 19);
			this.asyncButton.Name = "asyncButton";
			this.asyncButton.Size = new System.Drawing.Size(92, 17);
			this.asyncButton.TabIndex = 3;
			this.asyncButton.Text = "Asynchronous";
			this.asyncButton.UseVisualStyleBackColor = true;
			// 
			// syncButton
			// 
			this.syncButton.AutoSize = true;
			this.syncButton.Checked = true;
			this.syncButton.Location = new System.Drawing.Point(6, 19);
			this.syncButton.Name = "syncButton";
			this.syncButton.Size = new System.Drawing.Size(87, 17);
			this.syncButton.TabIndex = 2;
			this.syncButton.TabStop = true;
			this.syncButton.Text = "Synchronous";
			this.syncButton.UseVisualStyleBackColor = true;
			this.syncButton.CheckedChanged += new System.EventHandler(this.SyncCheckChanged);
			// 
			// maxCommandPanel
			// 
			this.maxCommandPanel.Controls.Add(this.asyncThreadBox);
			this.maxCommandPanel.Controls.Add(this.label26);
			this.maxCommandPanel.Controls.Add(this.maxCommandBox);
			this.maxCommandPanel.Controls.Add(this.label27);
			this.maxCommandPanel.Location = new System.Drawing.Point(3, 42);
			this.maxCommandPanel.Name = "maxCommandPanel";
			this.maxCommandPanel.Size = new System.Drawing.Size(358, 26);
			this.maxCommandPanel.TabIndex = 67;
			this.maxCommandPanel.Visible = false;
			// 
			// asyncThreadBox
			// 
			this.asyncThreadBox.Location = new System.Drawing.Point(300, 3);
			this.asyncThreadBox.Name = "asyncThreadBox";
			this.asyncThreadBox.Size = new System.Drawing.Size(50, 20);
			this.asyncThreadBox.TabIndex = 9;
			this.asyncThreadBox.Text = "1";
			this.asyncThreadBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label26
			// 
			this.label26.AutoSize = true;
			this.label26.Location = new System.Drawing.Point(203, 6);
			this.label26.Name = "label26";
			this.label26.Size = new System.Drawing.Size(96, 13);
			this.label26.TabIndex = 8;
			this.label26.Text = "Generator Threads";
			// 
			// maxCommandBox
			// 
			this.maxCommandBox.Location = new System.Drawing.Point(146, 3);
			this.maxCommandBox.Name = "maxCommandBox";
			this.maxCommandBox.Size = new System.Drawing.Size(50, 20);
			this.maxCommandBox.TabIndex = 7;
			this.maxCommandBox.Text = "40";
			this.maxCommandBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label27
			// 
			this.label27.AutoSize = true;
			this.label27.Location = new System.Drawing.Point(3, 6);
			this.label27.Name = "label27";
			this.label27.Size = new System.Drawing.Size(137, 13);
			this.label27.TabIndex = 6;
			this.label27.Text = "Max Concurrent Commands";
			// 
			// latencyGroup
			// 
			this.latencyGroup.Controls.Add(this.latencyDisplayLabel);
			this.latencyGroup.Controls.Add(this.latencyColumnsLabel);
			this.latencyGroup.Controls.Add(this.latencyColumnsBox);
			this.latencyGroup.Controls.Add(this.latencyShiftLabel);
			this.latencyGroup.Controls.Add(this.latencyShiftBox);
			this.latencyGroup.Location = new System.Drawing.Point(15, 249);
			this.latencyGroup.Name = "latencyGroup";
			this.latencyGroup.Size = new System.Drawing.Size(504, 104);
			this.latencyGroup.TabIndex = 102;
			this.latencyGroup.TabStop = false;
			this.latencyGroup.Text = "Latency Format";
			this.latencyGroup.Visible = false;
			// 
			// latencyDisplayLabel
			// 
			this.latencyDisplayLabel.BackColor = System.Drawing.SystemColors.Control;
			this.latencyDisplayLabel.Font = new System.Drawing.Font("Courier New", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.latencyDisplayLabel.Location = new System.Drawing.Point(3, 60);
			this.latencyDisplayLabel.Multiline = true;
			this.latencyDisplayLabel.Name = "latencyDisplayLabel";
			this.latencyDisplayLabel.ReadOnly = true;
			this.latencyDisplayLabel.Size = new System.Drawing.Size(483, 40);
			this.latencyDisplayLabel.TabIndex = 100;
			this.latencyDisplayLabel.Text = "<=1ms >1ms >8ms >64ms\r\n   x%   x%   x%    x%";
			// 
			// latencyColumnsLabel
			// 
			this.latencyColumnsLabel.AutoSize = true;
			this.latencyColumnsLabel.Location = new System.Drawing.Point(6, 27);
			this.latencyColumnsLabel.Name = "latencyColumnsLabel";
			this.latencyColumnsLabel.Size = new System.Drawing.Size(47, 13);
			this.latencyColumnsLabel.TabIndex = 96;
			this.latencyColumnsLabel.Text = "Columns";
			// 
			// latencyColumnsBox
			// 
			this.latencyColumnsBox.Location = new System.Drawing.Point(56, 24);
			this.latencyColumnsBox.Name = "latencyColumnsBox";
			this.latencyColumnsBox.Size = new System.Drawing.Size(52, 20);
			this.latencyColumnsBox.TabIndex = 97;
			this.latencyColumnsBox.Text = "4";
			this.latencyColumnsBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			this.latencyColumnsBox.TextChanged += new System.EventHandler(this.LatencyValueChanged);
			// 
			// latencyShiftLabel
			// 
			this.latencyShiftLabel.AutoSize = true;
			this.latencyShiftLabel.Location = new System.Drawing.Point(134, 27);
			this.latencyShiftLabel.Name = "latencyShiftLabel";
			this.latencyShiftLabel.Size = new System.Drawing.Size(76, 13);
			this.latencyShiftLabel.TabIndex = 98;
			this.latencyShiftLabel.Text = "Exponent Shift";
			// 
			// latencyShiftBox
			// 
			this.latencyShiftBox.Location = new System.Drawing.Point(212, 24);
			this.latencyShiftBox.Name = "latencyShiftBox";
			this.latencyShiftBox.Size = new System.Drawing.Size(52, 20);
			this.latencyShiftBox.TabIndex = 99;
			this.latencyShiftBox.Text = "3";
			this.latencyShiftBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			this.latencyShiftBox.TextChanged += new System.EventHandler(this.LatencyValueChanged);
			// 
			// label20
			// 
			this.label20.AutoSize = true;
			this.label20.Location = new System.Drawing.Point(472, 198);
			this.label20.Name = "label20";
			this.label20.Size = new System.Drawing.Size(20, 13);
			this.label20.TabIndex = 94;
			this.label20.Text = "ms";
			// 
			// debugBox
			// 
			this.debugBox.AutoSize = true;
			this.debugBox.Location = new System.Drawing.Point(306, 226);
			this.debugBox.Name = "debugBox";
			this.debugBox.RightToLeft = System.Windows.Forms.RightToLeft.No;
			this.debugBox.Size = new System.Drawing.Size(88, 17);
			this.debugBox.TabIndex = 3;
			this.debugBox.Text = "Debug Mode";
			this.debugBox.UseVisualStyleBackColor = true;
			// 
			// latencyBox
			// 
			this.latencyBox.AutoSize = true;
			this.latencyBox.Location = new System.Drawing.Point(21, 226);
			this.latencyBox.Name = "latencyBox";
			this.latencyBox.Size = new System.Drawing.Size(145, 17);
			this.latencyBox.TabIndex = 95;
			this.latencyBox.Text = "Enable Latency Tracking";
			this.latencyBox.UseVisualStyleBackColor = true;
			this.latencyBox.CheckedChanged += new System.EventHandler(this.LatencyChanged);
			// 
			// sleepBox
			// 
			this.sleepBox.Location = new System.Drawing.Point(419, 195);
			this.sleepBox.Name = "sleepBox";
			this.sleepBox.Size = new System.Drawing.Size(52, 20);
			this.sleepBox.TabIndex = 89;
			this.sleepBox.Text = "0";
			this.sleepBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label17
			// 
			this.label17.AutoSize = true;
			this.label17.Location = new System.Drawing.Point(302, 198);
			this.label17.Name = "label17";
			this.label17.Size = new System.Drawing.Size(115, 13);
			this.label17.TabIndex = 93;
			this.label17.Text = "Sleep Between Retries";
			// 
			// maxRetriesBox
			// 
			this.maxRetriesBox.Location = new System.Drawing.Point(240, 195);
			this.maxRetriesBox.Name = "maxRetriesBox";
			this.maxRetriesBox.Size = new System.Drawing.Size(52, 20);
			this.maxRetriesBox.TabIndex = 88;
			this.maxRetriesBox.Text = "0";
			this.maxRetriesBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label16
			// 
			this.label16.AutoSize = true;
			this.label16.Location = new System.Drawing.Point(173, 198);
			this.label16.Name = "label16";
			this.label16.Size = new System.Drawing.Size(63, 13);
			this.label16.TabIndex = 92;
			this.label16.Text = "Max Retries";
			// 
			// label15
			// 
			this.label15.AutoSize = true;
			this.label15.Location = new System.Drawing.Point(148, 198);
			this.label15.Name = "label15";
			this.label15.Size = new System.Drawing.Size(20, 13);
			this.label15.TabIndex = 91;
			this.label15.Text = "ms";
			// 
			// timeoutBox
			// 
			this.timeoutBox.Location = new System.Drawing.Point(82, 195);
			this.timeoutBox.Name = "timeoutBox";
			this.timeoutBox.Size = new System.Drawing.Size(66, 20);
			this.timeoutBox.TabIndex = 87;
			this.timeoutBox.Text = "0";
			this.timeoutBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label14
			// 
			this.label14.AutoSize = true;
			this.label14.Location = new System.Drawing.Point(16, 198);
			this.label14.Name = "label14";
			this.label14.Size = new System.Drawing.Size(45, 13);
			this.label14.TabIndex = 90;
			this.label14.Text = "Timeout";
			// 
			// binSizeBox
			// 
			this.binSizeBox.Location = new System.Drawing.Point(219, 112);
			this.binSizeBox.Name = "binSizeBox";
			this.binSizeBox.Size = new System.Drawing.Size(64, 20);
			this.binSizeBox.TabIndex = 72;
			this.binSizeBox.Text = "50";
			this.binSizeBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// binSizeLabel
			// 
			this.binSizeLabel.AutoSize = true;
			this.binSizeLabel.Location = new System.Drawing.Point(173, 115);
			this.binSizeLabel.Name = "binSizeLabel";
			this.binSizeLabel.Size = new System.Drawing.Size(45, 13);
			this.binSizeLabel.TabIndex = 75;
			this.binSizeLabel.Text = "Bin Size";
			// 
			// binTypeBox
			// 
			this.binTypeBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
			this.binTypeBox.FormattingEnabled = true;
			this.binTypeBox.Location = new System.Drawing.Point(82, 111);
			this.binTypeBox.Name = "binTypeBox";
			this.binTypeBox.Size = new System.Drawing.Size(81, 21);
			this.binTypeBox.TabIndex = 71;
			this.binTypeBox.SelectedIndexChanged += new System.EventHandler(this.BinTypeChanged);
			// 
			// label6
			// 
			this.label6.AutoSize = true;
			this.label6.Location = new System.Drawing.Point(16, 115);
			this.label6.Name = "label6";
			this.label6.Size = new System.Drawing.Size(49, 13);
			this.label6.TabIndex = 74;
			this.label6.Text = "Bin Type";
			// 
			// recordsBox
			// 
			this.recordsBox.Location = new System.Drawing.Point(82, 84);
			this.recordsBox.Name = "recordsBox";
			this.recordsBox.Size = new System.Drawing.Size(80, 20);
			this.recordsBox.TabIndex = 66;
			this.recordsBox.Text = "1000000";
			this.recordsBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
			// 
			// label24
			// 
			this.label24.AutoSize = true;
			this.label24.Location = new System.Drawing.Point(16, 87);
			this.label24.Name = "label24";
			this.label24.Size = new System.Drawing.Size(47, 13);
			this.label24.TabIndex = 68;
			this.label24.Text = "Records";
			// 
			// codeBox
			// 
			this.codeBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.codeBox.BackColor = System.Drawing.SystemColors.Window;
			this.codeBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
			this.codeBox.Location = new System.Drawing.Point(0, 0);
			this.codeBox.Name = "codeBox";
			this.codeBox.ReadOnly = true;
			this.codeBox.Size = new System.Drawing.Size(1019, 400);
			this.codeBox.TabIndex = 8;
			this.codeBox.Text = "";
			// 
			// consoleBox
			// 
			this.consoleBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
			this.consoleBox.BackColor = System.Drawing.SystemColors.Window;
			this.consoleBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
			this.consoleBox.Font = new System.Drawing.Font("Courier New", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.consoleBox.Location = new System.Drawing.Point(0, 34);
			this.consoleBox.Multiline = true;
			this.consoleBox.Name = "consoleBox";
			this.consoleBox.ReadOnly = true;
			this.consoleBox.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.consoleBox.Size = new System.Drawing.Size(1019, 289);
			this.consoleBox.TabIndex = 11;
			this.consoleBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.ConsoleKeyDown);
			// 
			// stopButton
			// 
			this.stopButton.Location = new System.Drawing.Point(82, 5);
			this.stopButton.Name = "stopButton";
			this.stopButton.Size = new System.Drawing.Size(75, 23);
			this.stopButton.TabIndex = 10;
			this.stopButton.Text = "Stop";
			this.stopButton.UseVisualStyleBackColor = true;
			this.stopButton.Click += new System.EventHandler(this.StopExample);
			// 
			// runButton
			// 
			this.runButton.Location = new System.Drawing.Point(1, 5);
			this.runButton.Name = "runButton";
			this.runButton.Size = new System.Drawing.Size(75, 23);
			this.runButton.TabIndex = 9;
			this.runButton.Text = "Start";
			this.runButton.UseVisualStyleBackColor = true;
			this.runButton.MouseClick += new System.Windows.Forms.MouseEventHandler(this.RunExample);
			// 
			// nsBox
			// 
			this.nsBox.Location = new System.Drawing.Point(621, 6);
			this.nsBox.Name = "nsBox";
			this.nsBox.Size = new System.Drawing.Size(100, 20);
			this.nsBox.TabIndex = 3;
			this.nsBox.Text = "test";
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Location = new System.Drawing.Point(558, 9);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(64, 13);
			this.label3.TabIndex = 11;
			this.label3.Text = "Namespace";
			// 
			// portBox
			// 
			this.portBox.Location = new System.Drawing.Point(200, 6);
			this.portBox.Name = "portBox";
			this.portBox.Size = new System.Drawing.Size(50, 20);
			this.portBox.TabIndex = 2;
			this.portBox.Text = "3000";
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Location = new System.Drawing.Point(174, 9);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(26, 13);
			this.label2.TabIndex = 9;
			this.label2.Text = "Port";
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(2, 9);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(63, 13);
			this.label1.TabIndex = 8;
			this.label1.Text = "Server Host";
			// 
			// hostBox
			// 
			this.hostBox.Location = new System.Drawing.Point(65, 6);
			this.hostBox.Name = "hostBox";
			this.hostBox.Size = new System.Drawing.Size(100, 20);
			this.hostBox.TabIndex = 1;
			this.hostBox.Text = "localhost";
			// 
			// setBox
			// 
			this.setBox.Location = new System.Drawing.Point(754, 6);
			this.setBox.Name = "setBox";
			this.setBox.Size = new System.Drawing.Size(100, 20);
			this.setBox.TabIndex = 4;
			this.setBox.Text = "demoset";
			// 
			// label4
			// 
			this.label4.AutoSize = true;
			this.label4.Location = new System.Drawing.Point(731, 9);
			this.label4.Name = "label4";
			this.label4.Size = new System.Drawing.Size(23, 13);
			this.label4.TabIndex = 13;
			this.label4.Text = "Set";
			// 
			// examplesView
			// 
			this.examplesView.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
			this.examplesView.BackColor = System.Drawing.SystemColors.ControlLight;
			this.examplesView.FullRowSelect = true;
			this.examplesView.HideSelection = false;
			this.examplesView.Indent = 5;
			this.examplesView.Location = new System.Drawing.Point(5, 32);
			this.examplesView.Name = "examplesView";
			this.examplesView.ShowLines = false;
			this.examplesView.Size = new System.Drawing.Size(144, 753);
			this.examplesView.TabIndex = 14;
			this.examplesView.AfterSelect += new System.Windows.Forms.TreeViewEventHandler(this.ExampleSelected);
			// 
			// passwordBox
			// 
			this.passwordBox.Location = new System.Drawing.Point(450, 6);
			this.passwordBox.Name = "passwordBox";
			this.passwordBox.Size = new System.Drawing.Size(100, 20);
			this.passwordBox.TabIndex = 16;
			this.passwordBox.UseSystemPasswordChar = true;
			// 
			// label12
			// 
			this.label12.AutoSize = true;
			this.label12.Location = new System.Drawing.Point(397, 9);
			this.label12.Name = "label12";
			this.label12.Size = new System.Drawing.Size(53, 13);
			this.label12.TabIndex = 18;
			this.label12.Text = "Password";
			// 
			// userBox
			// 
			this.userBox.Location = new System.Drawing.Point(288, 6);
			this.userBox.Name = "userBox";
			this.userBox.Size = new System.Drawing.Size(100, 20);
			this.userBox.TabIndex = 15;
			// 
			// label13
			// 
			this.label13.AutoSize = true;
			this.label13.Location = new System.Drawing.Point(257, 9);
			this.label13.Name = "label13";
			this.label13.Size = new System.Drawing.Size(29, 13);
			this.label13.TabIndex = 17;
			this.label13.Text = "User";
			// 
			// DemoForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(1184, 789);
			this.Controls.Add(this.label12);
			this.Controls.Add(this.userBox);
			this.Controls.Add(this.label13);
			this.Controls.Add(this.examplesView);
			this.Controls.Add(this.setBox);
			this.Controls.Add(this.label4);
			this.Controls.Add(this.nsBox);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.portBox);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.label1);
			this.Controls.Add(this.hostBox);
			this.Controls.Add(this.splitContainer1);
			this.Controls.Add(this.passwordBox);
			this.Name = "DemoForm";
			this.Text = "Aerospike Database Client Demo";
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FormClose);
			this.splitContainer1.Panel1.ResumeLayout(false);
			this.splitContainer1.Panel2.ResumeLayout(false);
			this.splitContainer1.Panel2.PerformLayout();
			((System.ComponentModel.ISupportInitialize)(this.splitContainer1)).EndInit();
			this.splitContainer1.ResumeLayout(false);
			this.benchmarkPanel.ResumeLayout(false);
			this.benchmarkPanel.PerformLayout();
			this.initializePanel.ResumeLayout(false);
			this.initializePanel.PerformLayout();
			this.workloadPanel.ResumeLayout(false);
			this.workloadPanel.PerformLayout();
			this.panel1.ResumeLayout(false);
			this.panel1.PerformLayout();
			this.groupBox1.ResumeLayout(false);
			this.groupBox1.PerformLayout();
			this.threadPanel.ResumeLayout(false);
			this.threadPanel.PerformLayout();
			this.maxCommandPanel.ResumeLayout(false);
			this.maxCommandPanel.PerformLayout();
			this.latencyGroup.ResumeLayout(false);
			this.latencyGroup.PerformLayout();
			this.ResumeLayout(false);
			this.PerformLayout();

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
        private System.Windows.Forms.Panel benchmarkPanel;
        private System.Windows.Forms.Label label20;
        private System.Windows.Forms.CheckBox debugBox;
        private System.Windows.Forms.CheckBox latencyBox;
        private System.Windows.Forms.TextBox latencyShiftBox;
        private System.Windows.Forms.Label latencyShiftLabel;
        private System.Windows.Forms.TextBox latencyColumnsBox;
        private System.Windows.Forms.Label latencyColumnsLabel;
        private System.Windows.Forms.TextBox sleepBox;
        private System.Windows.Forms.Label label17;
        private System.Windows.Forms.TextBox maxRetriesBox;
        private System.Windows.Forms.Label label16;
        private System.Windows.Forms.Label label15;
        private System.Windows.Forms.TextBox timeoutBox;
        private System.Windows.Forms.Label label14;
        private System.Windows.Forms.TextBox binSizeBox;
        private System.Windows.Forms.Label binSizeLabel;
        private System.Windows.Forms.ComboBox binTypeBox;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.TextBox recordsBox;
        private System.Windows.Forms.Label label24;
        private System.Windows.Forms.TextBox latencyDisplayLabel;
        private System.Windows.Forms.GroupBox latencyGroup;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.RadioButton asyncButton;
        private System.Windows.Forms.RadioButton syncButton;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.RadioButton dynamicValueButton;
        private System.Windows.Forms.RadioButton fixedValueButton;
        private System.Windows.Forms.Panel threadPanel;
        private System.Windows.Forms.TextBox syncThreadBox;
        private System.Windows.Forms.Label label25;
        private System.Windows.Forms.Panel maxCommandPanel;
        private System.Windows.Forms.TextBox asyncThreadBox;
        private System.Windows.Forms.Label label26;
        private System.Windows.Forms.TextBox maxCommandBox;
        private System.Windows.Forms.Label label27;
        private System.Windows.Forms.Panel workloadPanel;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.TextBox writeBox;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.TextBox readBox;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Panel initializePanel;
        private System.Windows.Forms.Label initPctLabel;
        private System.Windows.Forms.TextBox initPctBox;
        private System.Windows.Forms.Label label5;
		private System.Windows.Forms.TextBox passwordBox;
		private System.Windows.Forms.Label label12;
		private System.Windows.Forms.TextBox userBox;
		private System.Windows.Forms.Label label13;
		private System.Windows.Forms.CheckBox retryOnTimeoutBox;
		private System.Windows.Forms.Label label18;
		private System.Windows.Forms.ComboBox replicaBox;
    }
}