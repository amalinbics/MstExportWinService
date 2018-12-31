namespace MstExportWinService
{
    partial class ProjectInstaller
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
            this.MstExport = new System.ServiceProcess.ServiceProcessInstaller();
            this.MstExportWinService = new System.ServiceProcess.ServiceInstaller();
            // 
            // MstExport
            // 
            this.MstExport.Account = System.ServiceProcess.ServiceAccount.LocalService;
            this.MstExport.Password = null;
            this.MstExport.Username = null;
            // 
            // MstExportWinService
            // 
            this.MstExportWinService.DisplayName = "MstExpWinService";
            this.MstExportWinService.ServiceName = "MstExport";
            this.MstExportWinService.StartType = System.ServiceProcess.ServiceStartMode.Automatic;
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.MstExport,
            this.MstExportWinService});

        }

        #endregion

        private System.ServiceProcess.ServiceProcessInstaller MstExport;
        private System.ServiceProcess.ServiceInstaller MstExportWinService;
    }
}