using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MstExportWinService
{
    public partial class Service1 : ServiceBase
    {
        private int running;
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                //Debugger.Launch();
                //new Thread(new ThreadStart(ThreadProc)).Start();
                runThread();

            }
            catch (Exception ex)
            {
                Logging.LogError(ex);
            }
        }

        protected override void OnStop()
        {
        }

        public static void ThreadProc()
        {
            TimeSpan timeOutInt = TimeSpan.FromMinutes((double)Convert.ToInt32(ConfigurationManager.AppSettings["Interval"]));
            while (true)
            {
                GenerateOfflineDB.CallWebService();
                Thread.Sleep(timeOutInt);
            }
        }

        public void runThread()
        {
            if (Interlocked.CompareExchange(ref running, 1, 0) == 0)
            {
                Thread t = new Thread
                (
                    () =>
                    {
                        try
                        {
                            new Thread(new ThreadStart(ThreadProc)).Start();
                        }
                        catch
                        {
                            //Without the catch any exceptions will be unhandled
                            //(Maybe that's what you want, maybe not*)
                        }
                        finally
                        {
                            //Regardless of exceptions, we need this to happen:
                            running = 0;
                        }
                    }
                );
                t.IsBackground = true;
                t.Name = "myThread";
                t.Start();
            }
            else
            {
                Logging.WriteLog("Previous Process is already running...", EventLogEntryType.Error);
            }

        }
    }
}
