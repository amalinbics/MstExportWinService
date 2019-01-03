using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Diagnostics;
using System.Configuration;
using System.IO;
using Microsoft.WindowsAzure; // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Blob storage types
using Microsoft.Azure;

namespace MstExportWinService
{
    public class GenerateOfflineDB
    {
        public static string ConnStr = ConfigurationManager.ConnectionStrings["MstExportWinService"].ToString().Trim();
        //public static string DbPath = ConfigurationManager.AppSettings["DBPath"].ToString().Trim();
        public static string SourceAzureContainer = ConfigurationManager.AppSettings["SourceStorageContainer"].ToString().Trim();
        public static string DestinationAzureContainer = ConfigurationManager.AppSettings["DestinationStorageContainer"].ToString().Trim();
        public static void CallWebService()
        {
            try
            {
                CreateBulkInsertfile();
            }
            catch (Exception ex)
            {
                Logging.WriteToFileException(ex.Message);
            }
        }

        public static void CreateBulkInsertfile()
        {
            string filepath = "";
            try
            {
                var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

                SqlConnection con = new SqlConnection(ConnStr);
                con.Open();

                DataTable dtProducts = new DataTable();
                DataTable dtSalesContractSalesAlias = new DataTable();
                DataTable dtINSite = new DataTable();
                DataTable dtINSiteTank = new DataTable();
                DataTable dtINSiteTankSubCompartment = new DataTable();
                DataTable dtTankChartDetails = new DataTable();
                DataTable dtTankChartKeel = new DataTable();
                DataTable dtTankChartTrim = new DataTable();
                DataTable dtVehicle = new DataTable();
                DataTable dtVehicleComp = new DataTable();
                DataTable dtVehicleSubComp = new DataTable();
                DataTable dtVessel = new DataTable();
                DataTable dtMarineloc = new DataTable();
                DataTable dtSalesPLUBtn = new DataTable();
                DataTable dtProdCont = new DataTable();
                DataTable dtInSiteTankProductAPI = new DataTable();
                DataTable dtINSiteTank_Products = new DataTable();

                SqlCommand cmd = new SqlCommand();
                cmd.Connection = con;

                SqlDataAdapter sa = new SqlDataAdapter();


                //SalesContractSalesAlias
                string SalesContractSalesAliasQry = @"SELECT SysTrxNo,
                                                        SysTrxLine,
                                                        StandardAcctID,
                                                        ContractID,
                                                        ContractDescr,
                                                        SalesAliasID,
                                                        StartDate,
                                                        EndDate,
                                                        VendorProductxRef,
                                                        CompanyID FROM  SalesContractSalesAlias";
                cmd.CommandText = SalesContractSalesAliasQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtSalesContractSalesAlias);
               
                //Products

                //string ProdQry = "SELECT DISTINCT INS.SiteID, MasterProdID, ParentID, P.Code,REPLACE( P.Descr,'" + "" + "','') Descr, P.SellByUOMID, SU.Code as SellByUOM, P.DefOnHandUOMID, U.Code as OnHandUOM," +
                string ProdQry = "SELECT DISTINCT INS.SiteID, MasterProdID, ParentID, NULL as ProdID, P.Code,RTRIM(REPLACE( REPLACE( P.Descr,',',' '),'\"\','')) Descr, P.SellByUOMID, SU.Code as SellByUOM, P.DefOnHandUOMID, U.Code as OnHandUOM," +
                                 " CN.CODE AS OnCountUOM," +
                                 " CN.UOMID as OnCountUOMID," +
                                 " P.DefConversionUOMID, CV.Code as OnConversionUOM," +
                                 " U.ConversionFactor, ISNULL(SU.IsPackaged, 'N') IsPackaged, CASE WHEN P.MasterProdType = 'B' THEN 'Y' ELSE 'N' END as IsBillable," +
                                 " 0 as UnitPrice, 0 as AvailableQty, MasterProdType, H.Descr as HazmatDesc, " +//P.Explanation as CriticalDescription," +
                                 " (SELECT Explanation FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = P.MasterProdID)) AND" +
                                 " MasterProdType = 'P') as CriticalDescription," +
                                 " P.BIUOMID, BI.Code as BIUOM, CASE WHEN BI.IsPackaged = 'N' THEN 'Y' ELSE 'N' END as BIEnableTankReadings, 'N' as AllowNegative," +
                                 " P.CompanyID,P.CustomerID,P.TemperatureCorrectID, P.SpecificGravity,P.IsBulk FROM Products P" +
                                 " LEFT JOIN UOM U ON P.CustomerID = U.CustomerID AND P.DefOnHandUOMID = U.UOMID" +
                                 " LEFT JOIN UOM SU ON P.CustomerID = SU.CustomerID AND P.SellByUOMID = SU.UOMID" +
                                 " LEFT JOIN UOM CV ON P.CustomerID = CV.CustomerID AND P.DefConversionUOMID = CV.UOMID" +
                                 " LEFT JOIN UOM CN ON P.CustomerID = CN.CustomerID AND P.DefCountUOMID = CN.UOMID" +
                                 " LEFT JOIN UOM BI ON P.CustomerID = BI.CustomerID AND P.BIUOMID = BI.UOMID" +
                                 " JOIN INSite INS ON P.CompanyID = INS.CompanyID" +
                                 " JOIN INSiteBillingItem B ON INS.SiteID = B.SiteID AND P.MasterProdID = B.BillingItemID" +
                                 " AND B.Active = 'Y' AND B.ActiveMarneDelvApp = 'Y' /*P.BIActiveMarneDelvApp ='Y'*/" +
                                 " LEFT JOIN HzrdMaterialsInstruction H ON P.CustomerID = H.CustomerID AND P.HzrdMaterialID = H.HzrdMaterialID" +
                                 " UNION ALL" +
                                 " SELECT DISTINCT INS.SiteID,MasterProdID, ParentID, (SELECT ProdID FROM Products WHERE" +
                                 " MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = P.MasterProdID)) AND" +
                                 " MasterProdType = 'P') as ProdId, P.Code, RTRIM(REPLACE( REPLACE( P.Descr,',',' '),'\"\','')) Descr, P.SellByUOMID, SU.Code as SellByUOM, P.DefOnHandUOMID, U.Code as OnHandUOM," +
                                 " CASE WHEN ISNULL(INP.CountUOMID, 0) = 0 THEN CN.Code ELSE (SELECT CODE FROM UOM WHERE UOMID = INP.CountUOMID) END AS OnCountUOM," +
                                 " ISNULL(INP.CountUOMID, CN.UOMID) as OnCountUOMID," +
                                 " P.DefConversionUOMID, CV.Code as OnConversionUOM," +
                                 " U.ConversionFactor, ISNULL(SU.IsPackaged, 'N') IsPackaged, CASE WHEN P.MasterProdType = 'B' THEN 'Y' ELSE 'N' END as IsBillable," +
                                 " 0 as UnitPrice, INP.OnHand as AvailableQty, MasterProdType, H.Descr as HazmatDesc," + //P.Explanation as CriticalDescription," +
                                 " (SELECT Explanation FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = P.MasterProdID)) AND" +
                                 " MasterProdType = 'P') as CriticalDescription," +
                                 " P.BIUOMID, BI.Code as BIUOM, CASE WHEN BI.IsPackaged = 'N' THEN 'Y' ELSE 'N' END as BIEnableTankReadings, (SELECT ISNULL(EnterAsNegativeOrderedQuantity,'N') FROM ProdCont WHERE ProdContID=INP.ProdContID AND MasterProdID=P.MasterProdID) AS AllowNegative," +
                                 " P.CompanyID,P.CustomerID,P.TemperatureCorrectID, P.SpecificGravity,P.IsBulk FROM Products P" +
                                 " LEFT JOIN UOM U ON P.CustomerID = U.CustomerID AND P.DefOnHandUOMID = U.UOMID" +
                                 " LEFT JOIN UOM SU ON P.CustomerID = SU.CustomerID AND P.SellByUOMID = SU.UOMID" +
                                 " LEFT JOIN UOM CV ON P.CustomerID = CV.CustomerID AND P.DefConversionUOMID = CV.UOMID" +
                                 " LEFT JOIN UOM CN ON P.CustomerID = CN.CustomerID AND P.DefCountUOMID = CN.UOMID" +
                                 " LEFT JOIN UOM BI ON P.CustomerID = BI.CustomerID AND P.BIUOMID = BI.UOMID" +
                                 " JOIN INSite INS ON P.CompanyID = INS.CompanyID" +
                                 " JOIN INSiteProdCont INP ON INS.SiteID = INP.SiteID AND P.ParentID = INP.ProdContID" +
                                 " AND INP.ActiveMarneDelvApp = 'Y'" +
                                 " LEFT JOIN HzrdMaterialsInstruction H ON P.CustomerID = H.CustomerID AND P.HzrdMaterialID = H.HzrdMaterialID" +
                                 " UNION ALL" +//Added for Masterproduct type 'P'
                                 " SELECT null as SiteID, MasterProdID, ParentID, ProdId, P.Code, RTRIM(REPLACE( REPLACE( P.Descr,',',' '),'\"\','')) Descr, P.SellByUOMID," +
                                 " SU.Code as SellByUOM, P.DefOnHandUOMID,U.Code as OnHandUOM, null AS OnCountUOM, null as OnCountUOMID ,P.DefConversionUOMID," +
                                 " CV.Code as OnConversionUOM, U.ConversionFactor, ISNULL(SU.IsPackaged, 'N') IsPackaged, CASE WHEN P.MasterProdType = 'B' THEN 'Y'" +
                                 " ELSE 'N' END as IsBillable, 0 as UnitPrice,0 as AvailableQty, MasterProdType, H.Descr as HazmatDesc, (SELECT Explanation" +
                                 " FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE" +
                                 " MasterProdID = P.MasterProdID)) AND MasterProdType = 'P') as CriticalDescription, P.BIUOMID, BI.Code as BIUOM, CASE WHEN" +
                                 " BI.IsPackaged = 'N' THEN 'Y' ELSE 'N' END as BIEnableTankReadings," +
                                 " 'N' as AllowNegative, P.CompanyID,P.CustomerID, P.TemperatureCorrectID, P.SpecificGravity, P.IsBulk" +
                                 " from Products P LEFT JOIN UOM U ON P.CustomerID = U.CustomerID AND P.DefOnHandUOMID =" +
                                 " U.UOMID LEFT JOIN UOM SU ON P.CustomerID = SU.CustomerID AND P.SellByUOMID = SU.UOMID LEFT JOIN UOM CV ON P.CustomerID =" +
                                 " CV.CustomerID AND P.DefConversionUOMID = CV.UOMID LEFT JOIN UOM CN ON P.CustomerID = CN.CustomerID AND P.DefCountUOMID = CN.UOMID" +
                                 " LEFT JOIN UOM BI ON P.CustomerID = BI.CustomerID AND P.BIUOMID = BI.UOMID LEFT JOIN" +
                                 " HzrdMaterialsInstruction H ON P.CustomerID = H.CustomerID AND P.HzrdMaterialID = H.HzrdMaterialID  where P.MasterProdType = 'P'";

                cmd.CommandText = ProdQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtProducts);
                //Products



                //INSite
                string INSiteQry = "SELECT SiteID, Code + ' - ' + CompanyID as Code, LongDescr, FormattedAddress, FormattedLineAddress, INSiteType, CompanyID, CustomerID, LastModifiedDtTm, EnableElectronicDOI, EnableMarineDelivery, SiteType, Inactive" +
                                   " FROM INSite WHERE Inactive = 'N' AND EnableMarineDelivery = 1 ORDER BY CODE ASC";

                cmd.CommandText = INSiteQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtINSite);
                //INSite

                //INSiteTank
                string INSiteTankQry = "SELECT INSiteID,ProdContID,NULL SubCompartmentID, I.INSiteTankID AS TankID, I.Code, T.Code as TankType, Description as Descr, I.TankCapacity, I.CustomerID, TC.Denominator," +
                                " MAX(DepthFeet) DepthFeet,DBO.GetMaxInch(CASE WHEN dbo.TankSubCompartmentExists(I.CustomerID, I.INSiteTankID) = 'Y' THEN 0 ELSE TC.TankChartID END) AS MaxInch," +
                                " MAX(Depth)Depth, MAX(DepthFraction) DepthFraction, dbo.GetMaxDenominator(CASE WHEN dbo.TankSubCompartmentExists(I.CustomerID, I.INSiteTankID) = 'Y' THEN 0 ELSE TC.TankChartID END) AS MaxDenominator," +
                                " TC.VolumeUOM, TC.LinearUOM, UV.Code as VolumeUOMCode, UC.Code as LinearUOMCode," +
                                " CASE WHEN dbo.TankSubCompartmentExists(I.CustomerID, I.INSiteTankID) = 'Y' THEN NULL ELSE TC.TankChartID END AS TankChartID," +
                                " CASE WHEN dbo.TankSubCompartmentExists(I.CustomerID, I.INSiteTankID) = 'Y' THEN 'Y' ELSE dbo.TankChartTrimExists(I.CustomerID, TC.TankChartID) END as HasTrimCorrections," +
                                " CASE WHEN I.LinearExpansionCoeff IS NULL THEN 'N' ELSE 'Y' END As HasLinearExpansionCoeff, I.CompanyID, I.Active, LinearExpansionCoeff, Insulated, TankOperatingTemp" +
                                " FROM INSiteTank I" +
                                " JOIN INSiteTank_Products IP ON I.CustomerID = IP.CustomerID AND I.INSiteTankID = IP.INSiteTankID" +
                                " JOIN TankChart TC ON I.CustomerID = TC.CustomerID AND I.TankChartID = TC.TankChartID" +
                                " JOIN TankChartDetail TD ON I.CustomerID = TD.CustomerID AND I.TankChartID = TD.TankChartID" +
                                " LEFT JOIN TankType T ON I.CustomerID = T.CustomerID AND I.TankTypeID = T.TankTypeID" +
                                " LEFT JOIN UOM UV ON TC.CustomerID = UV.CustomerID AND TC.VolumeUOM = UV.UOMID" +
                                " LEFT JOIN UOM UC ON TC.CustomerID = UC.CustomerID AND TC.LinearUOM = UC.UOMID" +
                                " WHERE dbo.TankSubCompartmentExists(I.CustomerID, I.INSiteTankID) = 'N'" +
                                //" --I.CompanyID = @CompanyID AND I.Active = 'Y' AND I.CustomerID = @CustomerID AND INSiteID = @INSiteID"+
                                //" --AND((ISNULL(@ParentID, '') = '') OR(ProdContID = @ParentID))"+
                                //" --AND DBO.GetCS2EffTankProdContID(I.INSiteTankID, @OrderDate) = @ParentID"+
                                " GROUP BY INSiteID, ProdContID, I.INSiteTankID, I.Code, T.Code, Description, I.TankCapacity, I.CustomerID, INSiteID, TC.Denominator," +
                                " TC.VolumeUOM, TC.LinearUOM, TC.TankChartID, UV.Code, UC.Code, I.LinearExpansionCoeff, I.CompanyID, I.Active, LinearExpansionCoeff, Insulated, TankOperatingTemp" +
                                //" --ORDER BY I.Code"+
                                " UNION" +
                                " SELECT  INSiteID, SubProdContID, NULL SubCompartmentID, I.INSiteTankID AS TankID, I.Code, T.Code as TankType, Description as Descr, I.TankCapacity, I.CustomerID, TC.Denominator," +
                                " MAX(DepthFeet) DepthFeet, DBO.GetMaxInch(CASE WHEN dbo.TankSubCompartmentExists(I.CustomerID, I.INSiteTankID) = 'Y' THEN 0 ELSE TC.TankChartID END) AS MaxInch," +
                                " MAX(Depth) Depth, MAX(DepthFraction) DepthFraction, dbo.GetMaxDenominator(CASE WHEN dbo.TankSubCompartmentExists(I.CustomerID, I.INSiteTankID) = 'Y' THEN 0 ELSE TC.TankChartID END) AS MaxDenominator," +
                                " TC.VolumeUOM, TC.LinearUOM, UV.Code as VolumeUOMCode, UC.Code as LinearUOMCode," +
                                " TC.TankChartID, dbo.TankChartTrimExists(I.CustomerID, TC.TankChartID) as HasTrimCorrections," +
                                " CASE WHEN I.LinearExpansionCoeff IS NULL THEN 'N' ELSE 'Y' END As HasLinearExpansionCoeff, I.CompanyID, I.Active, LinearExpansionCoeff, Insulated, TankOperatingTemp" +
                                " FROM INSiteTank I" +
                                " JOIN INSiteTank_Products IP ON I.CustomerID = IP.CustomerID AND I.INSiteTankID = IP.INSiteTankID" +
                                " JOIN Substitutes S ON S.CustomerID = IP.CustomerID AND S.ProdContID = IP.ProdContID" +
                                " LEFT JOIN TankChart TC ON I.CustomerID = TC.CustomerID AND I.TankChartID = TC.TankChartID" +
                                " LEFT JOIN TankChartDetail TD ON I.CustomerID = TD.CustomerID AND I.TankChartID = TD.TankChartID" +
                                " LEFT JOIN TankType T ON I.CustomerID = T.CustomerID AND I.TankTypeID = T.TankTypeID" +
                                " LEFT JOIN UOM UV ON TC.CustomerID = UV.CustomerID AND TC.VolumeUOM = UV.UOMID" +
                                " LEFT JOIN UOM UC ON TC.CustomerID = UC.CustomerID AND TC.LinearUOM = UC.UOMID" +
                                //" --WHERE--I.CompanyID = @CompanyID AND I.Active = 'Y' AND I.CustomerID = @CustomerID AND INSiteID = @INSiteID"+
                                //" --AND((ISNULL(@ParentID, '') = '') OR(S.SubProdContID = @ParentID))"+
                                //" --AND DBO.GetCS2EffTankProdContID(I.INSiteTankID, @OrderDate) = S.ProdContID"+
                                " GROUP BY INSiteID, SubProdContID, I.INSiteTankID, I.Code, T.Code, Description, I.TankCapacity, I.CustomerID, INSiteID, TC.Denominator," +
                                " TC.VolumeUOM, TC.LinearUOM, TC.TankChartID, UV.Code, UC.Code, I.LinearExpansionCoeff, I.CompanyID, I.Active, LinearExpansionCoeff, Insulated, TankOperatingTemp" +
                                " UNION" +
                                " SELECT DISTINCT IST.INSiteID, IP.ProdContID, NULL SubCompartmentID, IST.INSiteTankID AS TankID, IST.Code, T.Code as TankType, Description as Descr, IST.TankCapacity, IST.CustomerID," +
                                " TC.Denominator, NULL DepthFeet, NULL MaxInch, NULL Depth, NULL DepthFraction, NULL MaxDenominator, NULL VolumeUOM, NULL LinearUOM, NULL as VolumeUOMCode, NULL as LinearUOMCode," +
                                " NULL TankChartID, 'Y' as HasTrimCorrections," +
                                " CASE WHEN IST.LinearExpansionCoeff IS NULL THEN 'N' ELSE 'Y' END As HasLinearExpansionCoeff, IST.CompanyID, NULL Active, LinearExpansionCoeff, Insulated, TankOperatingTemp" +
                                " FROM INSiteTank IST" +
                                " JOIN INSiteTank_Products IP ON IST.CustomerID = IP.CustomerID AND IST.INSiteTankID = IP.INSiteTankID" +
                                " JOIN InSiteTankSubCompartments I ON I.CustomerID = IST.CustomerID AND I.INSiteTankID = IST.INSiteTankID" +
                                " JOIN TankChart TC ON I.CustomerID = TC.CustomerID AND I.TankChartID = TC.TankChartID" +
                                " JOIN TankChartDetail TD ON I.CustomerID = TD.CustomerID AND I.TankChartID = TD.TankChartID" +
                                " LEFT JOIN TankType T ON I.CustomerID = T.CustomerID AND IST.TankTypeID = T.TankTypeID" +
                                " LEFT JOIN UOM UV ON TC.CustomerID = UV.CustomerID AND TC.VolumeUOM = UV.UOMID" +
                                " LEFT JOIN UOM UC ON TC.CustomerID = UC.CustomerID AND TC.LinearUOM = UC.UOMID" +
                                //" --WHERE IST.CompanyID = @CompanyID AND IST.CustomerID = @CustomerID AND IST.INSiteID = @INSiteID"+
                                //" --AND((ISNULL(@ParentID, '') = '') OR(IP.ProdContID = @ParentID))"+
                                //" --AND DBO.GetCS2EffTankProdContID(IST.INSiteTankID, @OrderDate) = @ParentID"+
                                " GROUP BY IST.INSiteID, IP.ProdContID, IST.INSiteTankID, IST.Code, T.Code, Description, IST.TankCapacity, IST.CustomerID, INSiteID, TC.Denominator," +
                                " TC.VolumeUOM, TC.LinearUOM, UV.Code, UC.Code, IST.LinearExpansionCoeff, IST.CompanyID, LinearExpansionCoeff, Insulated, TankOperatingTemp";

                cmd.CommandText = INSiteTankQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtINSiteTank);
                //INSiteTank

                //INSiteTankSubCompartment
                string InsiteTankSubCompQry = "SELECT SubCompartmentID, INSiteTankID, ReadingSide,CompartmentCode AS Code,TC.Code AS TankChartCode, I.TankChartID, I.CustomerID, TC.Denominator, CompartmentCode," +
                                              " MAX(DepthFeet) DepthFeet, MAX(Depth) Depth, MAX(DepthFraction) DepthFraction,DBO.GetMaxInch(I.TankChartID) AS MaxInch, DBO.GetMaxDenominator(I.TankChartID) MaxDenominator," +
                                              " TC.VolumeUOM, TC.LinearUOM, dbo.GetUOMCode(TC.VolumeUOM) As VolumeUOMCode, dbo.GetUOMCode(TC.LinearUOM) as LinearUOMCode" +
                                              " FROM InSiteTankSubCompartments I" +
                                              " JOIN TankChart TC ON I.CustomerID = TC.CustomerID AND I.TankChartID = TC.TankChartID" +
                                              " JOIN TankChartDetail TD ON I.CustomerID = TD.CustomerID AND I.TankChartID = TD.TankChartID" +
                                              //" --WHERE I.CustomerID = @CustomerID AND INSiteTankID = @INSiteTankID" +
                                              " GROUP BY SubCompartmentID, INSiteTankID, ReadingSide, I.TankChartID, I.CustomerID, TC.Denominator, CompartmentCode," +
                                              " TC.VolumeUOM, TC.LinearUOM,TC.Code" +
                                              " ORDER BY CASE WHEN ReadingSide = 'P' THEN '1'" +
                                              " WHEN ReadingSide = 'E' THEN '2' WHEN ReadingSide = 'S' THEN '3' ELSE ReadingSide END ASC";
                cmd.CommandText = InsiteTankSubCompQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtINSiteTankSubCompartment);
                //INSiteTankSubCompartment

                //TankChartDetail
                cmd.CommandText = "Select * From TankChartDetail";
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtTankChartDetails);
                //TankChartDetail

                //TankChartKeel
                cmd.CommandText = "Select * From TankChartKeel";
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtTankChartKeel);
                //TankChartKeel

                //TankChartTrim
                cmd.CommandText = "Select * From TankChartTrim";
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtTankChartTrim);
                //TankChartTrim

                //Vehicle
                string VehicleQry = "SELECT V.VehicleID, V.Code, V.Descr, EnableSubCompartment, EnforceShipmentMarineApp, COUNT(TD.TankChartID) as TankCount,V.CompanyID,V.CustomerID FROM Vehicle V" +
                                    " LEFT JOIN VehicleCompartments VC ON V.CustomerID = VC.CustomerID AND V.VehicleID = VC.VehicleID" +
                                    " LEFT JOIN VehicleSubCompartments VS ON VC.CustomerID = VS.CustomerID AND VC.CompartmentID = VS.CompartmentID" +
                                    " LEFT JOIN TankChart T on VS.CustomerID = T.CustomerID AND VS.TankChartID = T.TankChartID" +
                                    " LEFT JOIN TankChartDetail TD ON VS.CustomerID = TD.CustomerID AND VS.TankChartID = TD.TankChartID" +
                                    //" --WHERE V.CompanyID = @CompanyID AND V.CustomerID = @CustomerID" +
                                    " GROUP BY V.VehicleID, V.Code, V.Descr, EnableSubCompartment, EnforceShipmentMarineApp,V.CompanyID,V.CustomerID ORDER BY V.Descr";

                cmd.CommandText = VehicleQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtVehicle);
                //Vehicle

                //VehicleCompartments
                string VehicleCompQry = "SELECT CompartmentID, Code, Capacity,CustomerID,VehicleID  FROM VehicleCompartments ORDER BY Code";

                cmd.CommandText = VehicleCompQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtVehicleComp);
                //VehicleCompartments

                //VehicleSubCompartments
                string VehicleSubCompQry = "SELECT VehicleID,VC.CompartmentID,SubCompartmentID, ReadingSide, VS.TankChartID, Denominator," +
                                           " MAX(DepthFeet) DepthFeet, MAX(Depth) Depth, MAX(DepthFraction) DepthFraction ," +
                                           " CAST(DBO.GetMaxInch(VS.TankChartID) AS INT) AS MaxInch, DBO.GetMaxDenominator(VS.TankChartID) MaxDenominator," +
                                           " TC.VolumeUOM, U.Code AS VolumeUOMCode,VS.CustomerID" +
                                           " FROM VehicleCompartments VC" +
                                           " JOIN VehicleSubCompartments VS ON VC.CustomerID = VS.CustomerID AND VC.CompartmentID = VS.CompartmentID" +
                                           " JOIN TankChart TC ON VC.CustomerID = TC.CustomerID AND VS.TankChartID = TC.TankChartID" +
                                           " JOIN TankChartDetail TD ON VS.CustomerID = TD.CustomerID AND VS.TankChartID = TD.TankChartID" +
                                           " JOIN UOM U ON VC.CustomerID = U.CustomerID AND TC.VolumeUOM = U.UOMID" +
                                           //" --WHERE VS.CustomerID = @CustomerID AND VehicleID = @VehicleID AND VC.CompartmentID = @CompartmentID" +
                                           " GROUP BY ReadingSide, Denominator, SubCompartmentID, VS.TankChartID, TC.VolumeUOM, U.Code,VehicleID,VS.CustomerID,VC.CompartmentID" +
                                           " ORDER BY CASE WHEN ReadingSide = 'P' THEN '1' WHEN ReadingSide = 'E' THEN '2' WHEN ReadingSide = 'S' THEN '3' ELSE ReadingSide END ASC";

                cmd.CommandText = VehicleSubCompQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtVehicleSubComp);
                //VehicleSubCompartments

                //Vessel
                string VesselQry = "SELECT DISTINCT VesselID, VesselCode, VesselDescr, GrossTonnage, IMONo, StandardAcctNo, CustomerName, OwnershipStdAcctID, StandardAcctNo AS CustomerNumber," +
                                   " V.CustomerID,V.CompanyID FROM Vessel V LEFT JOIN ARShipto AR ON V.CustomerID = AR.CustomerID AND V.OwnershipStdAcctID = AR.StandardAcctID" +
                                   //" --WHERE V.CustomerID = @CustomerID AND(V.CompanyID = @CompanyID OR V.CompanyID = '00')" +
                                   " ORDER BY VesselCode";

                cmd.CommandText = VesselQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtVessel);
                //Vessel

                //Marine Location
                //cmd.CommandText = "MN_GetMarineLoc";
                //cmd.CommandType = CommandType.StoredProcedure;
                //cmd.Parameters.AddWithValue("@CustomerID", "4108");
                //cmd.Parameters.AddWithValue("@Lat", 29.9146493);
                //cmd.Parameters.AddWithValue("@Long", -90.05396029999997);
                //sa = new SqlDataAdapter();
                //sa.SelectCommand = cmd;
                //sa.Fill(dtMarineloc);

                string MarineLocQry = "SELECT MarineLocID, z.Code, Descr, z.DefLocDescr, z.Latitude, z.Longitude,NULL Distance FROM MarineLoc z WHERE z.DisplayFlag = 'Y'";
                cmd.CommandText = MarineLocQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtMarineloc);

                //Marine Location

                //ProdCont
                string ProdContQry = "SELECT ProdContID, ProdID, ContID, Explanation, HzrdMaterialID, MaterialSafetyDataID, DefActiveToSell, DefActiveToPurch, DefCostMethod," +
                                     " DefCountUOMID, DefCountFrequencyID, DefMaxOnhand, DefMinOnhand, DefOnHandUOMID, DefConversionUOMID, DefConversionFactor, DefReportNegative," +
                                     " DefReportCommitted, DefGLCodeID, ProdFrtGroupID, ProdTaxGroupID, GLMacroSub, CompanyID, ProdTypeID, ActiveForPO, PurchGroupID, TargetDaysOnHandQty," +
                                     " CustomerID, Weight, DefBlendRecipeID, CostingStyle,OrderQtyCalcMethod, ActiveMarneDelvApp, EnterAsNegativeOrderedQuantity from ProdCont";

                cmd.CommandText = ProdContQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtProdCont);

                //ProdCont

                //InSiteTank_ProductAPI
                string InsiteTankPrdQry = "SELECT ProductAPIID, InSiteTankID, ProdContID, API_Rating, Notes, EffDtTm, CustomerID from InSiteTank_ProductAPI";

                cmd.CommandText = InsiteTankPrdQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtInSiteTankProductAPI);

                //InSiteTank_ProductAPI

                //Logging.WriteToFileException("SalesPLUBtnQryBeforeCalled");

                //SalesPLUButtons
                string SalesPLUBtnQry = "SELECT A.SiteID, A.MasterProdID,P.ParentID AS ProdContID, P.Code, P.Descr, P.SellByUOMID, SU.Code as SellByUOM, P.DefOnHandUOMID, U.Code as OnHandUOM," +
                                        " CN.CODE AS OnCountUOM,CN.UOMID as OnCountUOMID,P.DefConversionUOMID, CV.Code as OnConversionUOM," +
                                        " U.ConversionFactor, ISNULL(SU.IsPackaged, 'N') IsPackaged, CASE WHEN P.MasterProdType = 'B' THEN 'Y' ELSE 'N' END as IsBillable," +
                                        " 0 as UnitPrice, 0 as AvailableQty, MasterProdType, H.Descr as HazmatDesc," +
                                        //" --P.Explanation as CriticalDescription," +
                                        " (SELECT Explanation FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = (SELECT ParentID FROM Products WHERE MasterProdID = A.MasterProdID)) AND" +
                                        " MasterProdType = 'P') as CriticalDescription,P.BIUOMID, BI.Code as BIUOM, CASE WHEN BI.IsPackaged = 'N' THEN 'Y' ELSE 'N' END as BIEnableTankReadings," +
                                        " ISNULL((SELECT ISNULL(EnterAsNegativeOrderedQuantity, 'N') FROM ProdCont WHERE ProdContID = P.ParentID AND A.MasterProdID = P.MasterProdID),'N') AS AllowNegative," +
                                        " CompanyID, P.CustomerID FROM SalesPLUButtons A" +
                                        " JOIN Products P ON P.CustomerID = A.CustomerID AND A.MasterProdID = P.MasterProdID" +
                                        " LEFT JOIN UOM U ON P.CustomerID = U.CustomerID AND P.DefOnHandUOMID = U.UOMID" +
                                        " LEFT JOIN UOM SU ON P.CustomerID = SU.CustomerID AND P.SellByUOMID = SU.UOMID" +
                                        " LEFT JOIN UOM CV ON P.CustomerID = CV.CustomerID AND P.DefConversionUOMID = CV.UOMID" +
                                        " LEFT JOIN UOM CN ON P.CustomerID = CN.CustomerID AND P.DefCountUOMID = CN.UOMID" +
                                        " LEFT JOIN UOM BI ON P.CustomerID = BI.CustomerID AND P.BIUOMID = BI.UOMID" +
                                        " LEFT JOIN HzrdMaterialsInstruction H ON P.CustomerID = H.CustomerID AND P.HzrdMaterialID = H.HzrdMaterialID" +
                                        " WHERE P.MasterProdType IN('S', 'B')";

                cmd.CommandText = SalesPLUBtnQry;
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtSalesPLUBtn);
                //Logging.WriteToFileException("SalesPLUBtnQryAfterCalled");
                //SalesPLUButtons

                //INSiteTank_Products
                cmd.CommandText = "select * from INSiteTank_Products";
                sa = new SqlDataAdapter();
                sa.SelectCommand = cmd;
                sa.Fill(dtINSiteTank_Products);
                //INSiteTank_Products

                con.Close();

                string Dirpath = AppDomain.CurrentDomain.BaseDirectory + "DBFile\\";
                filepath = AppDomain.CurrentDomain.BaseDirectory + "DBFile\\MyDataBase.db";

                if (!Directory.Exists(Dirpath))
                    Directory.CreateDirectory(Dirpath);

                if (File.Exists(filepath))
                {
                    File.Delete(filepath);
                }

                SQLiteConnection.CreateFile(filepath);

                SQLiteConnection m_dbConnection = new SQLiteConnection("Data Source=" + filepath + "; Version=3;");
                m_dbConnection.Open();

                //string sql = "create table TankChartDetail (TankChartID int, Depth numeric, Volume numeric, DepthFraction int, DepthExtFraction numeric, CustomerID varchar(32), LastModifiedDtTm varchar(100), DepthFeet int)";

                //SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
                //command.ExecuteNonQuery();

                using (var SqlLtcommand = new SQLiteCommand(m_dbConnection))
                {
                    SqlLtcommand.CommandText = "Create Table Products (SiteID int, MasterProdID int, ParentID int,ProdID TEXT, Code TEXT, Descr TEXT, SellByUOMID int, SellByUOM TEXT, DefOnHandUOMID int, OnHandUOM TEXT," +
                        "OnCountUOM TEXT, OnCountUOMID int, DefConversionUOMID int, OnConversionUOM TEXT, ConversionFactor Numeric, IsPackaged TEXT, IsBillable TEXT, UnitPrice Numeric, AvailableQty Numeric, MasterProdType TEXT," +
                        "HazmatDesc TEXT, CriticalDescription TEXT, BIUOMID int, BIUOM TEXT, BIEnableTankReadings TEXT, AllowNegative TEXT, CompanyID TEXT, CustomerID TEXT,TemperatureCorrectID TEXT, SpecificGravity TEXT, IsBulk TEXT)";

                    //SqlLtcommand.CommandText = "Create Table Products (SiteID int, MasterProdID int, ParentID TEXT, Code TEXT, Descr TEXT, SellByUOMID TEXT, SellByUOM TEXT, DefOnHandUOMID TEXT, OnHandUOM TEXT," +
                    //    "OnCountUOM TEXT, OnCountUOMID TEXT, DefConversionUOMID TEXT, OnConversionUOM TEXT, ConversionFactor TEXT, IsPackaged TEXT, IsBillable TEXT, UnitPrice TEXT, AvailableQty TEXT, MasterProdType TEXT," +
                    //    "HazmatDesc TEXT, CriticalDescription TEXT, BIUOMID TEXT, BIUOM TEXT, BIEnableTankReadings TEXT, AllowNegative TEXT, CompanyID TEXT, CustomerID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = @"CREATE TABLE SalesContractSalesAlias (SysTrxNo NUMERIC(20,0) ,SysTrxLine INT,StandardAcctID INTEGER,ContractID VARCHAR(24),ContractDescr VARCHAR(50),SalesAliasID INT,StartDate DOUBLE,EndDate DOUBLE,VendorProductxRef VARCHAR(40),CompanyID VARCHAR(8))";
                    SqlLtcommand.ExecuteNonQuery();


                    SqlLtcommand.CommandText = "Create Table INSite (SiteID int, Code TEXT, LongDescr TEXT, FormattedAddress TEXT, FormattedLineAddress TEXT, INSiteType TEXT, CompanyID TEXT, CustomerID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table INSiteTanks (INSiteID int, ProdContID int, SubCompartmentID int, TankID int, Code TEXT, TankType TEXT, Descr TEXT, TankCapacity numeric, CustomerID TEXT, Denominator int," +
                        " DepthFeet int, MaxInch int, Depth int, DepthFraction int, MaxDenominator int, VolumeUOM int, LinearUOM int, VolumeUOMCode TEXT, LinearUOMCode TEXT, TankChartID int, HasTrimCorrections TEXT, HasLinearExpansionCoeff TEXT, CompanyID TEXT, Active TEXT, LinearExpansionCoeff TEXT, Insulated Char, TankOperatingTemp INT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table INSiteTankSubCompartments (SubCompartmentID int, INSiteTankID int, ReadingSide TEXT, Code TEXT, TankChartCode TEXT, TankChartID int, CustomerID TEXT, Denominator int, CompartmentCode TEXT," +
                        " DepthFeet int, Depth int, DepthFraction int, MaxInch int, MaxDenominator int, VolumeUOM TEXT, LinearUOM TEXT, VolumeUOMCode TEXT, LinearUOMCode TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table TankChartDetail (TankChartID int, Depth numeric, Volume numeric, DepthFraction int, DepthExtFraction numeric, CustomerID varchar(32), LastModifiedDtTm varchar(100), DepthFeet int)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table TankChartKeel (TankChartKeelID int, TankChartID int, KeelTo char, KeelDegree numeric, CustomerID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table TankChartTrim (TankChartTrimID int, TankChartKeelID int, TankChartID int, TrimFeet int, TrimInch int, TrimExtFraction numeric, CorrInch int, CorrFeet int, CorrFraction int, CorrExtFraction numeric, DivisionTemp TEXT, DenominatorTemp int, CustomerID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table Vehicle (VehicleID int, Code TEXT, Descr TEXT, EnableSubCompartment TEXT, EnforceShipmentMarineApp TEXT, TankCount int, CompanyID TEXT, CustomerID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table VehicleCompartments (CompartmentID int, Code TEXT, Capacity int, CustomerID TEXT, VehicleID int)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table VehicleSubCompartments (VehicleID int, CompartmentID int, SubCompartmentID int, ReadingSide TEXT, TankChartID int, Denominator int, DepthFeet int, Depth int, DepthFraction int, MaxInch int, MaxDenominator int, VolumeUOM TEXT, VolumeUOMCode TEXT, CustomerID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table Vessel (VesselID int, VesselCode TEXT, VesselDescr TEXT, GrossTonnage numeric, IMONo TEXT, StandardAcctNo TEXT, CustomerName TEXT, OwnershipStdAcctID int, CustomerNumber TEXT, CustomerID TEXT, CompanyID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "Create Table MarineLoc (MarineLocID int, Code TEXT, Descr TEXT, DefLocDescr TEXT, Latitude Numeric, Longitude Numeric, Distance Numeric)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "CREATE TABLE ProdCont (ProdContID INT, ProdID INT,ContID INT,Explanation TEXT,HzrdMaterialID INT, MaterialSafetyDataID INT, DefActiveToSell TEXT, DefActiveToPurch TEXT, DefCostMethod TEXT, DefCountUOMID INT, DefCountFrequencyID INT, DefMaxOnhand Numeric, DefMinOnhand Numeric," +
                                               " DefOnHandUOMID INT,DefConversionUOMID INT,DefConversionFactor NUMERIC,DefReportNegative TEXT,DefReportCommitted TEXT,DefGLCodeID INT,ProdFrtGroupID INT,ProdTaxGroupID INT,GLMacroSub TEXT,CompanyID TEXT,ProdTypeID INT,ActiveForPO TEXT,PurchGroupID INT,TargetDaysOnHandQty Numeric," +
                                               " CustomerID TEXT, Weight Numeric, DefBlendRecipeID INT, CostingStyle TEXT, OrderQtyCalcMethod TEXT, ActiveMarneDelvApp TEXT, EnterAsNegativeOrderedQuantity TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    SqlLtcommand.CommandText = "CREATE TABLE InSiteTank_ProductAPI (ProductAPIID INT, InSiteTankID INT, ProdContID INT, API_Rating Numeric, Notes TEXT, EffDtTm DOUBLE, CustomerID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    //Logging.WriteToFileException("SalesPLUBtnTableBeforeCreated");

                    SqlLtcommand.CommandText = "CREATE TABLE SalesPLUButtons (SiteID INTEGER, MasterProdID INTEGER,ProdContID TEXT,Code TEXT,Descr TEXT, SellByUOMID TEXT, SellByUOM TEXT, DefOnHandUOMID TEXT, OnHandUOM TEXT, OnCountUOM TEXT, OnCountUOMID TEXT, DefConversionUOMID TEXT, OnConversionUOM TEXT," +
                                               " ConversionFactor NUMERIC,IsPackaged TEXT,IsBillable TEXT,UnitPrice TEXT,AvailableQty NUMERIC,MasterProdType TEXT,HazmatDesc TEXT,CriticalDescription TEXT,BIUOMID TEXT,BIUOM TEXT,BIEnableTankReadings TEXT,AllowNegative TEXT,CompanyID TEXT,CustomerID TEXT); ";
                    SqlLtcommand.ExecuteNonQuery();

                    //Logging.WriteToFileException("SalesPLUBtnTableAfterCreated");

                    SqlLtcommand.CommandText = "CREATE TABLE INSiteTank_Products (INSiteTankID INT, ProdContID INT, EffectiveDate datetime, ProductGroupID TEXT, CustomerID TEXT)";
                    SqlLtcommand.ExecuteNonQuery();

                    using (var transaction = m_dbConnection.BeginTransaction())
                    {
                        try
                        {
                            //Products
                            for (int i = 0; i < dtProducts.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO Products (SiteID, MasterProdID, ParentID, Code, Descr, SellByUOMID, SellByUOM, DefOnHandUOMID, OnHandUOM, OnCountUOM, OnCountUOMID, DefConversionUOMID, OnConversionUOM," +
                                //" ConversionFactor, IsPackaged, IsBillable, UnitPrice, AvailableQty, MasterProdType, HazmatDesc, CriticalDescription, BIUOMID, BIUOM, BIEnableTankReadings, AllowNegative, CompanyID, CustomerID) VALUES" +
                                //"(" + dtProducts.Rows[i]["SiteID"].ToString().Trim() + "," + dtProducts.Rows[i]["MasterProdID"].ToString().Trim() + "," + dtProducts.Rows[i]["ParentID"].ToString().Trim() + ",'" + dtProducts.Rows[i]["Code"].ToString().Trim() + "','" + dtProducts.Rows[i]["Descr"].ToString().Trim() + "'," + dtProducts.Rows[i]["SellByUOMID"].ToString().Trim() + ",'" + dtProducts.Rows[i]["SellByUOM"].ToString().Trim() + "'," + dtProducts.Rows[i]["DefOnHandUOMID"].ToString().Trim() + "," +
                                //"'" + dtProducts.Rows[i]["OnHandUOM"].ToString().Trim() + "','" + dtProducts.Rows[i]["OnCountUOM"].ToString().Trim() + "'," + dtProducts.Rows[i]["OnCountUOMID"].ToString().Trim() + "," + dtProducts.Rows[i]["DefConversionUOMID"].ToString().Trim() + ",'" + dtProducts.Rows[i]["OnConversionUOM"].ToString().Trim() + "'," + dtProducts.Rows[i]["ConversionFactor"].ToString().Trim() + ",'" + dtProducts.Rows[i]["IsPackaged"].ToString().Trim() + "','" + dtProducts.Rows[i]["IsBillable"].ToString().Trim() + "'," +
                                //"" + dtProducts.Rows[i]["UnitPrice"].ToString().Trim() + "," + dtProducts.Rows[i]["AvailableQty"].ToString().Trim() + ",'" + dtProducts.Rows[i]["MasterProdType"].ToString().Trim() + "','" + dtProducts.Rows[i]["HazmatDesc"].ToString().Trim() + "','" + dtProducts.Rows[i]["CriticalDescription"].ToString().Trim() + "'," + dtProducts.Rows[i]["BIUOMID"].ToString().Trim() + ",'" + dtProducts.Rows[i]["BIUOM"].ToString().Trim() + "','" + dtProducts.Rows[i]["BIEnableTankReadings"].ToString().Trim() + "'," +
                                //"'" + dtProducts.Rows[i]["AllowNegative"].ToString().Trim() + "','" + dtProducts.Rows[i]["CompanyID"].ToString().Trim() + "','" + dtProducts.Rows[i]["CustomerID"].ToString().Trim() + "')";

                                //"INSERT INTO Products (SiteID, MasterProdID, ParentID, Code, Descr, SellByUOMID, SellByUOM, DefOnHandUOMID, OnHandUOM, OnCountUOM, OnCountUOMID, DefConversionUOMID, OnConversionUOM," +
                                //    " ConversionFactor, IsPackaged, IsBillable, UnitPrice, AvailableQty, MasterProdType, HazmatDesc, CriticalDescription, BIUOMID, BIUOM, BIEnableTankReadings, AllowNegative, CompanyID, CustomerID) VALUES" +
                                //    "(" + dtProducts.Rows[i]["SiteID"].ToString().Trim() + "," + dtProducts.Rows[i]["MasterProdID"].ToString().Trim() + ",'" + dtProducts.Rows[i]["ParentID"].ToString().Trim() + "','" + dtProducts.Rows[i]["Code"].ToString().Trim() + "','" + dtProducts.Rows[i]["Descr"].ToString().Trim() + "','" + dtProducts.Rows[i]["SellByUOMID"].ToString().Trim() + "','" + dtProducts.Rows[i]["SellByUOM"].ToString().Trim() + "','" + dtProducts.Rows[i]["DefOnHandUOMID"].ToString().Trim() + "'," +
                                //    "'" + dtProducts.Rows[i]["OnHandUOM"].ToString().Trim() + "','" + dtProducts.Rows[i]["OnCountUOM"].ToString().Trim() + "','" + dtProducts.Rows[i]["OnCountUOMID"].ToString().Trim() + "','" + dtProducts.Rows[i]["DefConversionUOMID"].ToString().Trim() + "','" + dtProducts.Rows[i]["OnConversionUOM"].ToString().Trim() + "','" + dtProducts.Rows[i]["ConversionFactor"].ToString().Trim() + "','" + dtProducts.Rows[i]["IsPackaged"].ToString().Trim() + "','" + dtProducts.Rows[i]["IsBillable"].ToString().Trim() + "'," +
                                //    "'" + dtProducts.Rows[i]["UnitPrice"].ToString().Trim() + "','" + dtProducts.Rows[i]["AvailableQty"].ToString().Trim() + "','" + dtProducts.Rows[i]["MasterProdType"].ToString().Trim() + "','" + dtProducts.Rows[i]["HazmatDesc"].ToString().Trim() + "','" + dtProducts.Rows[i]["CriticalDescription"].ToString().Trim() + "','" + dtProducts.Rows[i]["BIUOMID"].ToString().Trim() + "','" + dtProducts.Rows[i]["BIUOM"].ToString().Trim() + "','" + dtProducts.Rows[i]["BIEnableTankReadings"].ToString().Trim() + "'," +
                                //    "'" + dtProducts.Rows[i]["AllowNegative"].ToString().Trim() + "','" + dtProducts.Rows[i]["CompanyID"].ToString().Trim() + "','" + dtProducts.Rows[i]["CustomerID"].ToString().Trim() + "')";

                                "INSERT INTO Products (SiteID, MasterProdID, ParentID, ProdID, Code, Descr, SellByUOMID, SellByUOM, DefOnHandUOMID, OnHandUOM, OnCountUOM, OnCountUOMID, DefConversionUOMID, OnConversionUOM," +
                                    " ConversionFactor, IsPackaged, IsBillable, UnitPrice, AvailableQty, MasterProdType, HazmatDesc, CriticalDescription, BIUOMID, BIUOM, BIEnableTankReadings, AllowNegative, CompanyID, CustomerID,TemperatureCorrectID,SpecificGravity,IsBulk) VALUES" +
                                    "(@SiteID, @MasterProdID, @ParentID, @ProdID, @Code, @Descr, @SellByUOMID, @SellByUOM, @DefOnHandUOMID, @OnHandUOM, @OnCountUOM, @OnCountUOMID, @DefConversionUOMID, @OnConversionUOM," +
                                    " @ConversionFactor, @IsPackaged, @IsBillable, @UnitPrice, @AvailableQty, @MasterProdType, @HazmatDesc, @CriticalDescription, @BIUOMID, @BIUOM, @BIEnableTankReadings, @AllowNegative, @CompanyID, @CustomerID,@TemperatureCorrectID,@SpecificGravity,@IsBulk)";

                                SqlLtcommand.Parameters.AddWithValue("@SiteID", dtProducts.Rows[i]["SiteID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MasterProdID", dtProducts.Rows[i]["MasterProdID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ParentID", dtProducts.Rows[i]["ParentID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProdID", dtProducts.Rows[i]["ProdID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Code", dtProducts.Rows[i]["Code"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Descr", dtProducts.Rows[i]["Descr"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@SellByUOMID", dtProducts.Rows[i]["SellByUOMID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@SellByUOM", dtProducts.Rows[i]["SellByUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefOnHandUOMID", dtProducts.Rows[i]["DefOnHandUOMID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@OnHandUOM", dtProducts.Rows[i]["OnHandUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@OnCountUOM", dtProducts.Rows[i]["OnCountUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@OnCountUOMID", dtProducts.Rows[i]["OnCountUOMID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefConversionUOMID", dtProducts.Rows[i]["DefConversionUOMID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@OnConversionUOM", dtProducts.Rows[i]["OnConversionUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ConversionFactor", dtProducts.Rows[i]["ConversionFactor"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@IsPackaged", dtProducts.Rows[i]["IsPackaged"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@IsBillable", dtProducts.Rows[i]["IsBillable"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@UnitPrice", dtProducts.Rows[i]["UnitPrice"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@AvailableQty", dtProducts.Rows[i]["AvailableQty"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MasterProdType", dtProducts.Rows[i]["MasterProdType"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@HazmatDesc", dtProducts.Rows[i]["HazmatDesc"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CriticalDescription", dtProducts.Rows[i]["CriticalDescription"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@BIUOMID", dtProducts.Rows[i]["BIUOMID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@BIUOM", dtProducts.Rows[i]["BIUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@BIEnableTankReadings", dtProducts.Rows[i]["BIEnableTankReadings"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@AllowNegative", dtProducts.Rows[i]["AllowNegative"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CompanyID", dtProducts.Rows[i]["CompanyID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtProducts.Rows[i]["CustomerID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TemperatureCorrectID", dtProducts.Rows[i]["TemperatureCorrectID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@SpecificGravity", dtProducts.Rows[i]["SpecificGravity"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@IsBulk", dtProducts.Rows[i]["IsBulk"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //Products

                            //SalesContractSalesAlias
                            for (int i = 0; i < dtSalesContractSalesAlias.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText = @"INSERT INTO SalesContractSalesAlias(SysTrxNo,SysTrxLine,StandardAcctID,ContractID,ContractDescr,SalesAliasID,StartDate,EndDate,VendorProductxRef,CompanyID )      
                                                            VALUES ( @SysTrxNo,@SysTrxLine,@StandardAcctID,@ContractID,@ContractDescr,@SalesAliasID,@StartDate,@EndDate,@VendorProductxRef,@CompanyID)";

                                SqlLtcommand.Parameters.AddWithValue("@SysTrxNo", dtSalesContractSalesAlias.Rows[i]["SysTrxNo"]);
                                SqlLtcommand.Parameters.AddWithValue("@SysTrxLine", dtSalesContractSalesAlias.Rows[i]["SysTrxLine"]);
                                SqlLtcommand.Parameters.AddWithValue("@StandardAcctID", dtSalesContractSalesAlias.Rows[i]["StandardAcctID"]);
                                SqlLtcommand.Parameters.AddWithValue("@ContractID", dtSalesContractSalesAlias.Rows[i]["ContractID"]);
                                SqlLtcommand.Parameters.AddWithValue("@ContractDescr", dtSalesContractSalesAlias.Rows[i]["ContractDescr"]);
                                SqlLtcommand.Parameters.AddWithValue("@SalesAliasID", dtSalesContractSalesAlias.Rows[i]["SalesAliasID"]);
                                SqlLtcommand.Parameters.AddWithValue("@StartDate", (Convert.ToDateTime(dtSalesContractSalesAlias.Rows[i]["StartDate"]).ToUniversalTime() - epoch).TotalSeconds);

                                SqlLtcommand.Parameters.AddWithValue("@EndDate", dtSalesContractSalesAlias.Rows[i]["EndDate"] != DBNull.Value
                                    ? (Convert.ToDateTime(dtSalesContractSalesAlias.Rows[i]["EndDate"]).ToUniversalTime() - epoch).TotalSeconds : 0);
                                SqlLtcommand.Parameters.AddWithValue("@VendorProductxRef", dtSalesContractSalesAlias.Rows[i]["VendorProductxRef"]);
                                SqlLtcommand.Parameters.AddWithValue("@CompanyID", dtSalesContractSalesAlias.Rows[i]["CompanyID"]);

                                SqlLtcommand.ExecuteNonQuery();
                            }


                            //INSite
                            for (int i = 0; i < dtINSite.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO INSite (SiteID, Code, LongDescr, FormattedAddress, FormattedLineAddress, INSiteType, CompanyID, CustomerID) VALUES" +
                                //"(" + dtINSite.Rows[i]["SiteID"].ToString().Trim() + ",'" + dtINSite.Rows[i]["Code"].ToString().Trim() + "','" + dtINSite.Rows[i]["LongDescr"].ToString().Trim() + "','" + dtINSite.Rows[i]["FormattedAddress"].ToString().Trim() + "','" + dtINSite.Rows[i]["FormattedLineAddress"].ToString().Trim() + "','" + dtINSite.Rows[i]["INSiteType"].ToString().Trim() + "','" + dtINSite.Rows[i]["CompanyID"].ToString().Trim() + "','" + dtINSite.Rows[i]["CustomerID"].ToString().Trim() + "')";

                                "INSERT INTO INSite (SiteID, Code, LongDescr, FormattedAddress, FormattedLineAddress, INSiteType, CompanyID, CustomerID) VALUES" +
                                    "(@SiteID, @Code, @LongDescr, @FormattedAddress, @FormattedLineAddress, @INSiteType, @CompanyID, @CustomerID)";

                                SqlLtcommand.Parameters.AddWithValue("@SiteID", dtINSite.Rows[i]["SiteID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Code", dtINSite.Rows[i]["Code"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@LongDescr", dtINSite.Rows[i]["LongDescr"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@FormattedAddress", dtINSite.Rows[i]["FormattedAddress"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@FormattedLineAddress", dtINSite.Rows[i]["FormattedLineAddress"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@INSiteType", dtINSite.Rows[i]["INSiteType"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CompanyID", dtINSite.Rows[i]["CompanyID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtINSite.Rows[i]["CustomerID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //INSite

                            //INSiteTanks
                            for (int i = 0; i < dtINSiteTank.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO INSiteTank (INSiteID, ProdContID, SubCompartmentID, TankID, Code, TankType, Descr, TankCapacity, CustomerID, Denominator, DepthFeet, MaxInch, Depth, DepthFraction, MaxDenominator, VolumeUOM, LinearUOM, VolumeUOMCode, LinearUOMCode, TankChartID, HasTrimCorrections, HasLinearExpansionCoeff, CompanyID, Active) VALUES" +
                                //"(" + dtINSiteTank.Rows[i]["INSiteID"].ToString().Trim() + "," + dtINSiteTank.Rows[i]["ProdContID"].ToString().Trim() + "," + dtINSiteTank.Rows[i]["SubCompartmentID"].ToString().Trim() + "," + dtINSiteTank.Rows[i]["TankID"].ToString().Trim() + ",'" + dtINSiteTank.Rows[i]["Code"].ToString().Trim() + "','" + dtINSiteTank.Rows[i]["TankType"].ToString().Trim() + "','" + dtINSiteTank.Rows[i]["Descr"].ToString().Trim() + "'," + dtINSiteTank.Rows[i]["TankCapacity"].ToString().Trim() + "," +
                                //"'" + dtINSiteTank.Rows[i]["CustomerID"].ToString().Trim() + "'," + dtINSiteTank.Rows[i]["Denominator"].ToString().Trim() + "," + dtINSiteTank.Rows[i]["DepthFeet"].ToString().Trim() + "," + dtINSiteTank.Rows[i]["MaxInch"].ToString().Trim() + "," + dtINSiteTank.Rows[i]["Depth"].ToString().Trim() + "," + dtINSiteTank.Rows[i]["DepthFraction"].ToString().Trim() + "," + dtINSiteTank.Rows[i]["MaxDenominator"].ToString().Trim() + ",'" + dtINSiteTank.Rows[i]["VolumeUOM"].ToString().Trim() + "'," +
                                //"'" + dtINSiteTank.Rows[i]["LinearUOM"].ToString().Trim() + "','" + dtINSiteTank.Rows[i]["VolumeUOMCode"].ToString().Trim() + "','" + dtINSiteTank.Rows[i]["LinearUOMCode"].ToString().Trim() + "'," + dtINSiteTank.Rows[i]["TankChartID"].ToString().Trim() + ",'" + dtINSiteTank.Rows[i]["HasTrimCorrections"].ToString().Trim() + "','" + dtINSiteTank.Rows[i]["HasLinearExpansionCoeff"].ToString().Trim() + "','" + dtINSiteTank.Rows[i]["CompanyID"].ToString().Trim() + "','" + dtINSiteTank.Rows[i]["Active"].ToString().Trim() + "')";

                                "INSERT INTO INSiteTanks (INSiteID, ProdContID, SubCompartmentID, TankID, Code, TankType, Descr, TankCapacity, CustomerID, Denominator, DepthFeet, MaxInch, Depth, DepthFraction, MaxDenominator, VolumeUOM, LinearUOM, VolumeUOMCode, LinearUOMCode, TankChartID, HasTrimCorrections, HasLinearExpansionCoeff, CompanyID, Active, LinearExpansionCoeff, Insulated, TankOperatingTemp) VALUES" +
                                    "(@INSiteID, @ProdContID, @SubCompartmentID, @TankID, @Code, @TankType, @Descr, @TankCapacity, @CustomerID, @Denominator, @DepthFeet, @MaxInch, @Depth, @DepthFraction, @MaxDenominator, @VolumeUOM, @LinearUOM, @VolumeUOMCode, @LinearUOMCode, @TankChartID, @HasTrimCorrections, @HasLinearExpansionCoeff, @CompanyID, @Active, @LinearExpansionCoeff, @Insulated, @TankOperatingTemp)";

                                SqlLtcommand.Parameters.AddWithValue("@INSiteID", dtINSiteTank.Rows[i]["INSiteID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProdContID", dtINSiteTank.Rows[i]["ProdContID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@SubCompartmentID", dtINSiteTank.Rows[i]["SubCompartmentID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankID", dtINSiteTank.Rows[i]["TankID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Code", dtINSiteTank.Rows[i]["Code"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankType", dtINSiteTank.Rows[i]["TankType"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Descr", dtINSiteTank.Rows[i]["Descr"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankCapacity", dtINSiteTank.Rows[i]["TankCapacity"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtINSiteTank.Rows[i]["CustomerID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Denominator", dtINSiteTank.Rows[i]["Denominator"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthFeet", dtINSiteTank.Rows[i]["DepthFeet"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MaxInch", dtINSiteTank.Rows[i]["MaxInch"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Depth", dtINSiteTank.Rows[i]["Depth"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthFraction", dtINSiteTank.Rows[i]["DepthFraction"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MaxDenominator", dtINSiteTank.Rows[i]["MaxDenominator"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VolumeUOM", dtINSiteTank.Rows[i]["VolumeUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@LinearUOM", dtINSiteTank.Rows[i]["LinearUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VolumeUOMCode", dtINSiteTank.Rows[i]["VolumeUOMCode"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@LinearUOMCode", dtINSiteTank.Rows[i]["LinearUOMCode"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankChartID", dtINSiteTank.Rows[i]["TankChartID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@HasTrimCorrections", dtINSiteTank.Rows[i]["HasTrimCorrections"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@HasLinearExpansionCoeff", dtINSiteTank.Rows[i]["HasLinearExpansionCoeff"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CompanyID", dtINSiteTank.Rows[i]["CompanyID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Active", dtINSiteTank.Rows[i]["Active"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@LinearExpansionCoeff", dtINSiteTank.Rows[i]["LinearExpansionCoeff"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Insulated", dtINSiteTank.Rows[i]["Insulated"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankOperatingTemp", dtINSiteTank.Rows[i]["TankOperatingTemp"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //INSiteTanks

                            //INSiteTankSubCompartments
                            for (int i = 0; i < dtINSiteTankSubCompartment.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO INSiteTankSubCompartments (SubCompartmentID, INSiteTankID, ReadingSide, Code, TankChartCode, TankChartID, CustomerID, Denominator, CompartmentCode, DepthFeet, Depth, DepthFraction, MaxInch,  MaxDenominator, VolumeUOM, LinearUOM, VolumeUOMCode, LinearUOMCode) VALUES" +
                                //"(" + dtINSiteTankSubCompartment.Rows[i]["SubCompartmentID"].ToString().Trim() + "," + dtINSiteTankSubCompartment.Rows[i]["INSiteTankID"].ToString().Trim() + ",'" + dtINSiteTankSubCompartment.Rows[i]["ReadingSide"].ToString().Trim() + "','" + dtINSiteTankSubCompartment.Rows[i]["Code"].ToString().Trim() + "','" + dtINSiteTankSubCompartment.Rows[i]["TankChartCode"].ToString().Trim() + "'," + dtINSiteTankSubCompartment.Rows[i]["TankChartID"].ToString().Trim() + ",'" + dtINSiteTankSubCompartment.Rows[i]["CustomerID"].ToString().Trim() + "'," + dtINSiteTankSubCompartment.Rows[i]["Denominator"].ToString().Trim() + "," +
                                //"'" + dtINSiteTankSubCompartment.Rows[i]["CompartmentCode"].ToString().Trim() + "'," + dtINSiteTankSubCompartment.Rows[i]["DepthFeet"].ToString().Trim() + "," + dtINSiteTankSubCompartment.Rows[i]["Depth"].ToString().Trim() + "," + dtINSiteTankSubCompartment.Rows[i]["DepthFraction"].ToString().Trim() + "," + dtINSiteTankSubCompartment.Rows[i]["MaxInch"].ToString().Trim() + "," + dtINSiteTankSubCompartment.Rows[i]["MaxDenominator"].ToString().Trim() + ",'" + dtINSiteTankSubCompartment.Rows[i]["VolumeUOM"].ToString().Trim() + "'," +
                                //"'" + dtINSiteTankSubCompartment.Rows[i]["LinearUOM"].ToString().Trim() + "','" + dtINSiteTankSubCompartment.Rows[i]["VolumeUOMCode"].ToString().Trim() + "','" + dtINSiteTankSubCompartment.Rows[i]["LinearUOMCode"].ToString().Trim() + "')";

                                "INSERT INTO INSiteTankSubCompartments (SubCompartmentID, INSiteTankID, ReadingSide, Code, TankChartCode, TankChartID, CustomerID, Denominator, CompartmentCode, DepthFeet, Depth, DepthFraction, MaxInch,  MaxDenominator, VolumeUOM, LinearUOM, VolumeUOMCode, LinearUOMCode) VALUES" +
                                    "(@SubCompartmentID, @INSiteTankID, @ReadingSide, @Code, @TankChartCode, @TankChartID, @CustomerID, @Denominator, @CompartmentCode, @DepthFeet, @Depth, @DepthFraction, @MaxInch, @MaxDenominator, @VolumeUOM, @LinearUOM, @VolumeUOMCode, @LinearUOMCode)";

                                SqlLtcommand.Parameters.AddWithValue("@SubCompartmentID", dtINSiteTankSubCompartment.Rows[i]["SubCompartmentID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@INSiteTankID", dtINSiteTankSubCompartment.Rows[i]["INSiteTankID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ReadingSide", dtINSiteTankSubCompartment.Rows[i]["ReadingSide"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Code", dtINSiteTankSubCompartment.Rows[i]["Code"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankChartCode", dtINSiteTankSubCompartment.Rows[i]["TankChartCode"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankChartID", dtINSiteTankSubCompartment.Rows[i]["TankChartID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtINSiteTankSubCompartment.Rows[i]["CustomerID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Denominator", dtINSiteTankSubCompartment.Rows[i]["Denominator"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CompartmentCode", dtINSiteTankSubCompartment.Rows[i]["CompartmentCode"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthFeet", dtINSiteTankSubCompartment.Rows[i]["DepthFeet"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Depth", dtINSiteTankSubCompartment.Rows[i]["Depth"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthFraction", dtINSiteTankSubCompartment.Rows[i]["DepthFraction"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MaxInch", dtINSiteTankSubCompartment.Rows[i]["MaxInch"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MaxDenominator", dtINSiteTankSubCompartment.Rows[i]["MaxDenominator"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VolumeUOM", dtINSiteTankSubCompartment.Rows[i]["VolumeUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@LinearUOM", dtINSiteTankSubCompartment.Rows[i]["LinearUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VolumeUOMCode", dtINSiteTankSubCompartment.Rows[i]["VolumeUOMCode"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@LinearUOMCode", dtINSiteTankSubCompartment.Rows[i]["LinearUOMCode"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //INSiteTankSubCompartments

                            //TankChartDetail
                            for (int i = 0; i < dtTankChartDetails.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO TankChartDetail (TankChartID, Depth, Volume, DepthFraction, DepthExtFraction, CustomerID, LastModifiedDtTm, DepthFeet) VALUES" +
                                //"(" + dtTankChartDetails.Rows[i]["TankChartID"].ToString().Trim() + "," + dtTankChartDetails.Rows[i]["Depth"].ToString().Trim() + "," + dtTankChartDetails.Rows[i]["Volume"].ToString().Trim() + "," + dtTankChartDetails.Rows[i]["DepthFraction"].ToString().Trim() + "," + dtTankChartDetails.Rows[i]["DepthExtFraction"].ToString().Trim() + ",'" + dtTankChartDetails.Rows[i]["CustomerID"].ToString().Trim() + "','" + dtTankChartDetails.Rows[i]["LastModifiedDtTm"].ToString().Trim() + "'," + dtTankChartDetails.Rows[i]["DepthFeet"].ToString().Trim() + ")";

                                "INSERT INTO TankChartDetail (TankChartID, Depth, Volume, DepthFraction, DepthExtFraction, CustomerID, LastModifiedDtTm, DepthFeet) VALUES" +
                                "(@TankChartID, @Depth, @Volume, @DepthFraction, @DepthExtFraction, @CustomerID, @LastModifiedDtTm, @DepthFeet)";

                                SqlLtcommand.Parameters.AddWithValue("@TankChartID", dtTankChartDetails.Rows[i]["TankChartID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Depth", dtTankChartDetails.Rows[i]["Depth"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Volume", dtTankChartDetails.Rows[i]["Volume"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthFraction", dtTankChartDetails.Rows[i]["DepthFraction"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthExtFraction", dtTankChartDetails.Rows[i]["DepthExtFraction"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtTankChartDetails.Rows[i]["CustomerID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@LastModifiedDtTm", dtTankChartDetails.Rows[i]["LastModifiedDtTm"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthFeet", dtTankChartDetails.Rows[i]["DepthFeet"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //TankChartDetail

                            //TankChartKeel
                            for (int i = 0; i < dtTankChartKeel.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO TankChartKeel (TankChartKeelID, TankChartID, KeelTo, KeelDegree, CustomerID) VALUES" +
                                //"(" + dtTankChartKeel.Rows[i]["TankChartKeelID"].ToString().Trim() + "," + dtTankChartKeel.Rows[i]["TankChartID"].ToString().Trim() + ",'" + dtTankChartKeel.Rows[i]["KeelTo"].ToString().Trim() + "'," + dtTankChartKeel.Rows[i]["KeelDegree"].ToString().Trim() + ",'" + dtTankChartKeel.Rows[i]["CustomerID"].ToString().Trim() + "')";

                                "INSERT INTO TankChartKeel (TankChartKeelID, TankChartID, KeelTo, KeelDegree, CustomerID) VALUES" +
                                "(@TankChartKeelID, @TankChartID, @KeelTo, @KeelDegree, @CustomerID)";

                                SqlLtcommand.Parameters.AddWithValue("@TankChartKeelID", dtTankChartKeel.Rows[i]["TankChartKeelID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankChartID", dtTankChartKeel.Rows[i]["TankChartID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@KeelTo", dtTankChartKeel.Rows[i]["KeelTo"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@KeelDegree", dtTankChartKeel.Rows[i]["KeelDegree"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtTankChartKeel.Rows[i]["CustomerID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //TankChartKeel

                            //TankChartTrim
                            for (int i = 0; i < dtTankChartTrim.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO TankChartTrim (TankChartTrimID, TankChartKeelID, TankChartID, TrimFeet, TrimInch, TrimExtFraction, CorrInch, CorrFeet, CorrFraction, CorrExtFraction, DivisionTemp, DenominatorTemp, CustomerID) VALUES" +
                                //"(" + dtTankChartTrim.Rows[i]["TankChartTrimID"].ToString().Trim() + "," + dtTankChartTrim.Rows[i]["TankChartKeelID"].ToString().Trim() + "," + dtTankChartTrim.Rows[i]["TankChartID"].ToString().Trim() + ",'" + dtTankChartTrim.Rows[i]["TrimFeet"].ToString().Trim() + "','" + dtTankChartTrim.Rows[i]["TrimInch"].ToString().Trim() + "','" + dtTankChartTrim.Rows[i]["TrimExtFraction"].ToString().Trim() + "','" + dtTankChartTrim.Rows[i]["CorrInch"].ToString().Trim() + "'," +
                                //"'" + dtTankChartTrim.Rows[i]["CorrFeet"].ToString().Trim() + "', '" + dtTankChartTrim.Rows[i]["CorrFraction"].ToString().Trim() + "','" + dtTankChartTrim.Rows[i]["CorrExtFraction"].ToString().Trim() + "','" + dtTankChartTrim.Rows[i]["DivisionTemp"].ToString().Trim() + "','" + dtTankChartTrim.Rows[i]["DenominatorTemp"].ToString().Trim() + "','" + dtTankChartTrim.Rows[i]["CustomerID"].ToString().Trim() + "')";

                                "INSERT INTO TankChartTrim (TankChartTrimID, TankChartKeelID, TankChartID, TrimFeet, TrimInch, TrimExtFraction, CorrInch, CorrFeet, CorrFraction, CorrExtFraction, DivisionTemp, DenominatorTemp, CustomerID) VALUES" +
                                "(@TankChartTrimID, @TankChartKeelID, @TankChartID, @TrimFeet, @TrimInch, @TrimExtFraction, @CorrInch, @CorrFeet, @CorrFraction, @CorrExtFraction, @DivisionTemp, @DenominatorTemp, @CustomerID)";

                                SqlLtcommand.Parameters.AddWithValue("@TankChartTrimID", dtTankChartTrim.Rows[i]["TankChartTrimID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankChartKeelID", dtTankChartTrim.Rows[i]["TankChartKeelID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankChartID", dtTankChartTrim.Rows[i]["TankChartID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TrimFeet", dtTankChartTrim.Rows[i]["TrimFeet"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TrimInch", dtTankChartTrim.Rows[i]["TrimInch"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TrimExtFraction", dtTankChartTrim.Rows[i]["TrimExtFraction"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CorrInch", dtTankChartTrim.Rows[i]["CorrInch"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CorrFeet", dtTankChartTrim.Rows[i]["CorrFeet"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CorrFraction", dtTankChartTrim.Rows[i]["CorrFraction"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CorrExtFraction", dtTankChartTrim.Rows[i]["CorrExtFraction"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DivisionTemp", dtTankChartTrim.Rows[i]["DivisionTemp"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DenominatorTemp", dtTankChartTrim.Rows[i]["DenominatorTemp"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtTankChartTrim.Rows[i]["CustomerID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //TankChartTrim

                            //Vehicle
                            for (int i = 0; i < dtVehicle.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO Vehicle (VehicleID, Code, Descr, EnableSubCompartment, EnforceShipmentMarineApp, TankCount, CompanyID, CustomerID) VALUES" +
                                //"(" + dtVehicle.Rows[i]["VehicleID"].ToString().Trim() + ",'" + dtVehicle.Rows[i]["Code"].ToString().Trim() + "','" + dtVehicle.Rows[i]["Descr"].ToString().Trim() + "','" + dtVehicle.Rows[i]["EnableSubCompartment"].ToString().Trim() + "','" + dtVehicle.Rows[i]["EnforceShipmentMarineApp"].ToString().Trim() + "'," +
                                //" " + dtVehicle.Rows[i]["TankCount"].ToString().Trim() + ",'" + dtVehicle.Rows[i]["CompanyID"].ToString().Trim() + "','" + dtVehicle.Rows[i]["CustomerID"].ToString().Trim() + "')";

                                "INSERT INTO Vehicle (VehicleID, Code, Descr, EnableSubCompartment, EnforceShipmentMarineApp, TankCount, CompanyID, CustomerID) VALUES" +
                                "(@VehicleID, @Code, @Descr, @EnableSubCompartment, @EnforceShipmentMarineApp, @TankCount, @CompanyID, @CustomerID)";

                                SqlLtcommand.Parameters.AddWithValue("@VehicleID", dtVehicle.Rows[i]["VehicleID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Code", dtVehicle.Rows[i]["Code"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Descr", dtVehicle.Rows[i]["Descr"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@EnableSubCompartment", dtVehicle.Rows[i]["EnableSubCompartment"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@EnforceShipmentMarineApp", dtVehicle.Rows[i]["EnforceShipmentMarineApp"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankCount", dtVehicle.Rows[i]["TankCount"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CompanyID", dtVehicle.Rows[i]["CompanyID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtVehicle.Rows[i]["CustomerID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //Vehicle

                            //VehicleCompartments
                            for (int i = 0; i < dtVehicleComp.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO VehicleCompartments (CompartmentID, Code, Capacity, CustomerID, VehicleID) VALUES" +
                                //"(" + dtVehicleComp.Rows[i]["CompartmentID"].ToString().Trim() + ",'" + dtVehicleComp.Rows[i]["Code"].ToString().Trim() + "'," + dtVehicleComp.Rows[i]["Capacity"].ToString().Trim() + ",'" + dtVehicleComp.Rows[i]["CustomerID"].ToString().Trim() + "','" + dtVehicleComp.Rows[i]["VehicleID"].ToString().Trim() + "')";

                                "INSERT INTO VehicleCompartments (CompartmentID, Code, Capacity, CustomerID, VehicleID) VALUES" +
                                "(@CompartmentID, @Code, @Capacity, @CustomerID, @VehicleID)";

                                SqlLtcommand.Parameters.AddWithValue("@CompartmentID", dtVehicleComp.Rows[i]["CompartmentID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Code", dtVehicleComp.Rows[i]["Code"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Capacity", dtVehicleComp.Rows[i]["Capacity"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtVehicleComp.Rows[i]["CustomerID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VehicleID", dtVehicleComp.Rows[i]["VehicleID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //VehicleCompartments

                            //VehicleSubCompartments
                            for (int i = 0; i < dtVehicleSubComp.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO VehicleSubCompartments (SubCompartmentID, ReadingSide, TankChartID, Denominator, DepthFeet, Depth, DepthFraction, MaxInch, MaxDenominator, VolumeUOM, VolumeUOMCode, VehicleID, CustomerID, CompartmentID) VALUES" +
                                //"(" + dtVehicleSubComp.Rows[i]["SubCompartmentID"].ToString().Trim() + ",'" + dtVehicleSubComp.Rows[i]["ReadingSide"].ToString().Trim() + "'," + dtVehicleSubComp.Rows[i]["TankChartID"].ToString().Trim() + "," + dtVehicleSubComp.Rows[i]["Denominator"].ToString().Trim() + "," + dtVehicleSubComp.Rows[i]["DepthFeet"].ToString().Trim() + "," +
                                //"" + dtVehicleSubComp.Rows[i]["Depth"].ToString().Trim() + "," + dtVehicleSubComp.Rows[i]["DepthFraction"].ToString().Trim() + "," + dtVehicleSubComp.Rows[i]["MaxInch"].ToString().Trim() + "," + dtVehicleSubComp.Rows[i]["MaxDenominator"].ToString().Trim() + ",'" + dtVehicleSubComp.Rows[i]["VolumeUOM"].ToString().Trim() + "'," +
                                //"'" + dtVehicleSubComp.Rows[i]["VolumeUOMCode"].ToString().Trim() + "'," + dtVehicleSubComp.Rows[i]["VehicleID"].ToString().Trim() + ",'" + dtVehicleSubComp.Rows[i]["CustomerID"].ToString().Trim() + "'," + dtVehicleSubComp.Rows[i]["CompartmentID"].ToString().Trim() + ")";

                                "INSERT INTO VehicleSubCompartments (SubCompartmentID, ReadingSide, TankChartID, Denominator, DepthFeet, Depth, DepthFraction, MaxInch, MaxDenominator, VolumeUOM, VolumeUOMCode, VehicleID, CustomerID, CompartmentID) VALUES" +
                                "(@SubCompartmentID, @ReadingSide, @TankChartID, @Denominator, @DepthFeet, @Depth, @DepthFraction, @MaxInch, @MaxDenominator, @VolumeUOM, @VolumeUOMCode, @VehicleID, @CustomerID, @CompartmentID)";

                                SqlLtcommand.Parameters.AddWithValue("@SubCompartmentID", dtVehicleSubComp.Rows[i]["SubCompartmentID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ReadingSide", dtVehicleSubComp.Rows[i]["ReadingSide"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TankChartID", dtVehicleSubComp.Rows[i]["TankChartID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Denominator", dtVehicleSubComp.Rows[i]["Denominator"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthFeet", dtVehicleSubComp.Rows[i]["DepthFeet"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Depth", dtVehicleSubComp.Rows[i]["Depth"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DepthFraction", dtVehicleSubComp.Rows[i]["DepthFraction"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MaxInch", dtVehicleSubComp.Rows[i]["MaxInch"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MaxDenominator", dtVehicleSubComp.Rows[i]["MaxDenominator"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VolumeUOM", dtVehicleSubComp.Rows[i]["VolumeUOM"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VolumeUOMCode", dtVehicleSubComp.Rows[i]["VolumeUOMCode"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VehicleID", dtVehicleSubComp.Rows[i]["VehicleID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtVehicleSubComp.Rows[i]["CustomerID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CompartmentID", dtVehicleSubComp.Rows[i]["CompartmentID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //VehicleSubCompartments

                            //Vessel
                            for (int i = 0; i < dtVessel.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO Vessel (VesselID, VesselCode, VesselDescr, GrossTonnage, IMONo, StandardAcctNo, CustomerName, OwnershipStdAcctID, CustomerNumber, CustomerID, CompanyID) VALUES" +
                                //"(" + dtVessel.Rows[i]["VesselID"].ToString().Trim() + ",'" + dtVessel.Rows[i]["VesselCode"].ToString().Trim() + "','" + dtVessel.Rows[i]["VesselDescr"].ToString().Trim() + "','" + dtVessel.Rows[i]["GrossTonnage"].ToString().Trim() + "','" + dtVessel.Rows[i]["IMONo"].ToString().Trim() + "'," +
                                //"'" + dtVessel.Rows[i]["StandardAcctNo"].ToString().Trim() + "','" + dtVessel.Rows[i]["CustomerName"].ToString().Trim() + "','" + dtVessel.Rows[i]["OwnershipStdAcctID"].ToString().Trim() + "','" + dtVessel.Rows[i]["CustomerNumber"].ToString().Trim() + "','" + dtVessel.Rows[i]["CustomerID"].ToString().Trim() + "','" + dtVessel.Rows[i]["CompanyID"].ToString().Trim() + "')";

                                "INSERT INTO Vessel (VesselID, VesselCode, VesselDescr, GrossTonnage, IMONo, StandardAcctNo, CustomerName, OwnershipStdAcctID, CustomerNumber, CustomerID, CompanyID) VALUES" +
                                "(@VesselID, @VesselCode, @VesselDescr, @GrossTonnage, @IMONo, @StandardAcctNo, @CustomerName, @OwnershipStdAcctID, @CustomerNumber, @CustomerID, @CompanyID)";

                                SqlLtcommand.Parameters.AddWithValue("@VesselID", dtVessel.Rows[i]["VesselID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VesselCode", dtVessel.Rows[i]["VesselCode"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@VesselDescr", dtVessel.Rows[i]["VesselDescr"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@GrossTonnage", dtVessel.Rows[i]["GrossTonnage"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@IMONo", dtVessel.Rows[i]["IMONo"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@StandardAcctNo", dtVessel.Rows[i]["StandardAcctNo"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerName", dtVessel.Rows[i]["CustomerName"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@OwnershipStdAcctID", dtVessel.Rows[i]["OwnershipStdAcctID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerNumber", dtVessel.Rows[i]["CustomerNumber"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtVessel.Rows[i]["CustomerID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CompanyID", dtVessel.Rows[i]["CompanyID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //Vessel

                            //MarineLoc
                            for (int i = 0; i < dtMarineloc.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                //"INSERT INTO MarineLoc (MarineLocID, Code, Descr, DefLocDescr, Latitude, Longitude, Distance) VALUES" +
                                //"(" + dtMarineloc.Rows[i]["MarineLocID"].ToString().Trim() + ",'" + dtMarineloc.Rows[i]["Code"].ToString().Trim() + "','" + dtMarineloc.Rows[i]["Descr"].ToString().Trim() + "','" + dtMarineloc.Rows[i]["DefLocDescr"].ToString().Trim() + "'," + dtMarineloc.Rows[i]["Latitude"].ToString().Trim() + "," +
                                //"" + dtMarineloc.Rows[i]["Longitude"].ToString().Trim() + "," + dtMarineloc.Rows[i]["Distance"].ToString().Trim() + ")";

                                "INSERT INTO MarineLoc (MarineLocID, Code, Descr, DefLocDescr, Latitude, Longitude, Distance) VALUES" +
                                "(@MarineLocID,@Code,@Descr,@DefLocDescr,@Latitude,@Longitude,@Distance)";

                                SqlLtcommand.Parameters.AddWithValue("@MarineLocID", dtMarineloc.Rows[i]["MarineLocID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Code", dtMarineloc.Rows[i]["Code"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Descr", dtMarineloc.Rows[i]["Descr"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefLocDescr", dtMarineloc.Rows[i]["DefLocDescr"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Latitude", dtMarineloc.Rows[i]["Latitude"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Longitude", dtMarineloc.Rows[i]["Longitude"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Distance", dtMarineloc.Rows[i]["Distance"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //MarineLoc

                            //ProdCont
                            for (int i = 0; i < dtProdCont.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =
                                "Insert into ProdCont (ProdContID, ProdID, ContID, Explanation, HzrdMaterialID, MaterialSafetyDataID, DefActiveToSell, DefActiveToPurch, DefCostMethod," +
                                                        " DefCountUOMID, DefCountFrequencyID, DefMaxOnhand, DefMinOnhand, DefOnHandUOMID, DefConversionUOMID, DefConversionFactor, DefReportNegative," +
                                                        " DefReportCommitted, DefGLCodeID, ProdFrtGroupID, ProdTaxGroupID, GLMacroSub, CompanyID, ProdTypeID, ActiveForPO, PurchGroupID, TargetDaysOnHandQty," +
                                                        " CustomerID, Weight, DefBlendRecipeID, CostingStyle,OrderQtyCalcMethod, ActiveMarneDelvApp, EnterAsNegativeOrderedQuantity) VALUES" +
                                                        " (@ProdContID, @ProdID, @ContID, @Explanation, @HzrdMaterialID, @MaterialSafetyDataID, @DefActiveToSell, @DefActiveToPurch, @DefCostMethod," +
                                                        " @DefCountUOMID, @DefCountFrequencyID, @DefMaxOnhand, @DefMinOnhand, @DefOnHandUOMID, @DefConversionUOMID, @DefConversionFactor, @DefReportNegative," +
                                                        " @DefReportCommitted, @DefGLCodeID, @ProdFrtGroupID, @ProdTaxGroupID, @GLMacroSub, @CompanyID, @ProdTypeID, @ActiveForPO, @PurchGroupID, @TargetDaysOnHandQty," +
                                                        " @CustomerID, @Weight, @DefBlendRecipeID, @CostingStyle, @OrderQtyCalcMethod, @ActiveMarneDelvApp, @EnterAsNegativeOrderedQuantity)";

                                SqlLtcommand.Parameters.AddWithValue("@ProdContID", dtProdCont.Rows[i]["ProdContID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProdID", dtProdCont.Rows[i]["ProdID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ContID", dtProdCont.Rows[i]["ContID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Explanation", dtProdCont.Rows[i]["Explanation"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@HzrdMaterialID", dtProdCont.Rows[i]["HzrdMaterialID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@MaterialSafetyDataID", dtProdCont.Rows[i]["MaterialSafetyDataID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefActiveToSell", dtProdCont.Rows[i]["DefActiveToSell"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefActiveToPurch", dtProdCont.Rows[i]["DefActiveToPurch"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefCostMethod", dtProdCont.Rows[i]["DefCostMethod"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefCountUOMID", dtProdCont.Rows[i]["DefCountUOMID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefCountFrequencyID", dtProdCont.Rows[i]["DefCountFrequencyID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefMaxOnhand", dtProdCont.Rows[i]["DefMaxOnhand"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefMinOnhand", dtProdCont.Rows[i]["DefMinOnhand"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefOnHandUOMID", dtProdCont.Rows[i]["DefOnHandUOMID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefConversionUOMID", dtProdCont.Rows[i]["DefConversionUOMID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefConversionFactor", dtProdCont.Rows[i]["DefConversionFactor"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefReportNegative", dtProdCont.Rows[i]["DefReportNegative"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefReportCommitted", dtProdCont.Rows[i]["DefReportCommitted"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefGLCodeID", dtProdCont.Rows[i]["DefGLCodeID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProdFrtGroupID", dtProdCont.Rows[i]["ProdFrtGroupID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProdTaxGroupID", dtProdCont.Rows[i]["ProdTaxGroupID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@GLMacroSub", dtProdCont.Rows[i]["GLMacroSub"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CompanyID", dtProdCont.Rows[i]["CompanyID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProdTypeID", dtProdCont.Rows[i]["ProdTypeID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ActiveForPO", dtProdCont.Rows[i]["ActiveForPO"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@PurchGroupID", dtProdCont.Rows[i]["PurchGroupID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@TargetDaysOnHandQty", dtProdCont.Rows[i]["TargetDaysOnHandQty"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtProdCont.Rows[i]["CustomerID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Weight", dtProdCont.Rows[i]["Weight"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@DefBlendRecipeID", dtProdCont.Rows[i]["DefBlendRecipeID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CostingStyle", dtProdCont.Rows[i]["CostingStyle"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@OrderQtyCalcMethod", dtProdCont.Rows[i]["OrderQtyCalcMethod"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ActiveMarneDelvApp", dtProdCont.Rows[i]["ActiveMarneDelvApp"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@EnterAsNegativeOrderedQuantity", dtProdCont.Rows[i]["EnterAsNegativeOrderedQuantity"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //ProdCont

                            //InSiteTankProductAPI

                            for (int i = 0; i < dtInSiteTankProductAPI.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =

                                "INSERT INTO InSiteTank_ProductAPI (ProductAPIID, InSiteTankID, ProdContID, API_Rating, Notes, EffDtTm, CustomerID) VALUES" +
                                "(@ProductAPIID,@InSiteTankID,@ProdContID,@API_Rating,@Notes,@EffDtTm,@CustomerID)";

                                SqlLtcommand.Parameters.AddWithValue("@ProductAPIID", dtInSiteTankProductAPI.Rows[i]["ProductAPIID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@InSiteTankID", dtInSiteTankProductAPI.Rows[i]["InSiteTankID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProdContID", dtInSiteTankProductAPI.Rows[i]["ProdContID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@API_Rating", dtInSiteTankProductAPI.Rows[i]["API_Rating"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@Notes", dtInSiteTankProductAPI.Rows[i]["Notes"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@EffDtTm", (Convert.ToDateTime(dtInSiteTankProductAPI.Rows[i]["EffDtTm"]).ToUniversalTime() - epoch).TotalSeconds);
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtInSiteTankProductAPI.Rows[i]["CustomerID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //InSiteTankProductAPI

                            //SalesPLUButtons
                            if (dtSalesPLUBtn != null && dtSalesPLUBtn.Rows.Count > 0)
                            {
                                //Logging.WriteToFileException("SalesPLUBtnTableCount: " + dtSalesPLUBtn.Rows.Count);

                                for (int i = 0; i < dtSalesPLUBtn.Rows.Count; i++)
                                {
                                    //Logging.WriteToFileException("SalesPLUBtnTableBeforeInserted " + i);

                                    SqlLtcommand.CommandText =
                                    "Insert into SalesPLUButtons (SiteID, MasterProdID, ProdContID, Code, Descr, SellByUOMID, SellByUOM, DefOnHandUOMID, OnHandUOM, OnCountUOM, OnCountUOMID, DefConversionUOMID, OnConversionUOM," +
                                                   " ConversionFactor,IsPackaged,IsBillable,UnitPrice,AvailableQty,MasterProdType,HazmatDesc,CriticalDescription,BIUOMID,BIUOM,BIEnableTankReadings,AllowNegative,CompanyID,CustomerID) VALUES" +
                                        "(@SiteID, @MasterProdID, @ProdContID, @Code, @Descr, @SellByUOMID, @SellByUOM, @DefOnHandUOMID, @OnHandUOM, @OnCountUOM, @OnCountUOMID, @DefConversionUOMID, @OnConversionUOM," +
                                        " @ConversionFactor, @IsPackaged, @IsBillable, @UnitPrice, @AvailableQty, @MasterProdType, @HazmatDesc, @CriticalDescription, @BIUOMID, @BIUOM, @BIEnableTankReadings, @AllowNegative, @CompanyID, @CustomerID)";

                                    SqlLtcommand.Parameters.AddWithValue("@SiteID", dtSalesPLUBtn.Rows[i]["SiteID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@MasterProdID", dtSalesPLUBtn.Rows[i]["MasterProdID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@ProdContID", dtSalesPLUBtn.Rows[i]["ProdContID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@Code", dtSalesPLUBtn.Rows[i]["Code"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@Descr", dtSalesPLUBtn.Rows[i]["Descr"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@SellByUOMID", dtSalesPLUBtn.Rows[i]["SellByUOMID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@SellByUOM", dtSalesPLUBtn.Rows[i]["SellByUOM"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@DefOnHandUOMID", dtSalesPLUBtn.Rows[i]["DefOnHandUOMID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@OnHandUOM", dtSalesPLUBtn.Rows[i]["OnHandUOM"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@OnCountUOM", dtSalesPLUBtn.Rows[i]["OnCountUOM"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@OnCountUOMID", dtSalesPLUBtn.Rows[i]["OnCountUOMID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@DefConversionUOMID", dtSalesPLUBtn.Rows[i]["DefConversionUOMID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@OnConversionUOM", dtSalesPLUBtn.Rows[i]["OnConversionUOM"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@ConversionFactor", dtSalesPLUBtn.Rows[i]["ConversionFactor"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@IsPackaged", dtSalesPLUBtn.Rows[i]["IsPackaged"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@IsBillable", dtSalesPLUBtn.Rows[i]["IsBillable"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@UnitPrice", dtSalesPLUBtn.Rows[i]["UnitPrice"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@AvailableQty", dtSalesPLUBtn.Rows[i]["AvailableQty"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@MasterProdType", dtSalesPLUBtn.Rows[i]["MasterProdType"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@HazmatDesc", dtSalesPLUBtn.Rows[i]["HazmatDesc"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@CriticalDescription", dtSalesPLUBtn.Rows[i]["CriticalDescription"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@BIUOMID", dtSalesPLUBtn.Rows[i]["BIUOMID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@BIUOM", dtSalesPLUBtn.Rows[i]["BIUOM"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@BIEnableTankReadings", dtSalesPLUBtn.Rows[i]["BIEnableTankReadings"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@AllowNegative", dtSalesPLUBtn.Rows[i]["AllowNegative"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@CompanyID", dtSalesPLUBtn.Rows[i]["CompanyID"].ToString().Trim());
                                    SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtSalesPLUBtn.Rows[i]["CustomerID"].ToString().Trim());

                                    SqlLtcommand.ExecuteNonQuery();
                                    //Logging.WriteToFileException("SalesPLUBtnTableAfterInserted " + i);
                                }
                            }
                            //SalesPLUButtons

                            //INSiteTank_Products
                            for (int i = 0; i < dtINSiteTank_Products.Rows.Count; i++)
                            {
                                SqlLtcommand.CommandText =

                                "INSERT INTO INSiteTank_Products (INSiteTankID, ProdContID, EffectiveDate, ProductGroupID, CustomerID) VALUES" +
                                "(@INSiteTankID,@ProdContID,@EffectiveDate,@ProductGroupID,@CustomerID)";

                                SqlLtcommand.Parameters.AddWithValue("@INSiteTankID", dtINSiteTank_Products.Rows[i]["INSiteTankID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProdContID", dtINSiteTank_Products.Rows[i]["ProdContID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@EffectiveDate", dtINSiteTank_Products.Rows[i]["EffectiveDate"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@ProductGroupID", dtINSiteTank_Products.Rows[i]["ProductGroupID"].ToString().Trim());
                                SqlLtcommand.Parameters.AddWithValue("@CustomerID", dtINSiteTank_Products.Rows[i]["CustomerID"].ToString().Trim());

                                SqlLtcommand.ExecuteNonQuery();
                            }
                            //INSiteTank_Products

                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            //Logging.WriteLog("Error in BulkInsertFile - " + ex.Message.ToString(), EventLogEntryType.Error);
                            Logging.WriteToFileException("Error in BulkInsertFile - " + ex.Message);
                        }
                    }
                }
                m_dbConnection.Close();
            }
            catch (Exception ex)
            {
                Logging.WriteToFileException("Error in DB File Creation - " + ex.Message);
            }

            try
            {
                string _storageConnection = CloudConfigurationManager.GetSetting("StorageConnectionString");
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(_storageConnection);

                // Create the blob client.
                CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

                CloudBlobContainer sourceblobContainer = blobClient.GetContainerReference(SourceAzureContainer);
                sourceblobContainer.CreateIfNotExists();
                sourceblobContainer.SetPermissions(
                new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });

                // Retrieve a reference to a container.
                CloudBlobContainer destinationblobcontainer = blobClient.GetContainerReference(DestinationAzureContainer);

                // Create the container if it doesn't already exist.
                destinationblobcontainer.CreateIfNotExists();
                destinationblobcontainer.SetPermissions(
                new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });

                CloudBlockBlob sourceBlob = sourceblobContainer.GetBlockBlobReference("Offline.db");
                sourceBlob.DeleteIfExists();

                CloudBlockBlob destinationBlob = destinationblobcontainer.GetBlockBlobReference("Offline.db");
                destinationBlob.DeleteIfExists();

                using (var fileStream = System.IO.File.OpenRead(@"" + filepath + ""))
                {
                    sourceBlob.UploadFromStream(fileStream);
                }

                destinationBlob.StartCopy(sourceBlob);

                destinationBlob.Properties.CacheControl = "no-store";
                destinationBlob.SetProperties();

                sourceBlob.Delete(DeleteSnapshotsOption.IncludeSnapshots);
            }
            catch (Exception ex)
            {
                //Logging.WriteLog("Error in CloudStorage - " + ex.Message, EventLogEntryType.Error);
                Logging.WriteToFileException("Error in CloudStorage - " + ex.Message);
            }
        }
    }
}
