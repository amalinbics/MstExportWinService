using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MstExportWinService
{
    public static class ApplicationConstants
    {
        public struct Connection
        {
            public const String ConnectionString = "MstExportWinService";
        }

        public struct Errors
        {
            public const String FunctionError = "Error occured. Function : in {0}, Class : {1}, Error Message : {2} Stack Trace : {3}";
            public const String ConnectionString = "Could not find {0} connection string in config file";

        }

        public struct Logging
        {
            public const String Log = "MasterExport";
            public const String Source = "ExpWinServ";
        }
    }
}
