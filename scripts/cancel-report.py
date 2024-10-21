import os
import snowflake.connector
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Step 1: Connect to Snowflake and Run the Query
def run_query():
    # Snowflake connection parameters
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
        # database=os.getenv("SNOWFLAKE_DATABASE"),
        # schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    # SQL query to run
    query = """
        WITH T
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,D.SERVICER
            ,D.MDS_SP_CD
            ,F.ACCTG_YR_MTH FISCAL_YR_MTH_CREATED
            ,F2.ACCTG_YR_MTH FISCAL_YR_MTH_CLOSED
        FROM PRD_DB2.HS_PERM_TBLS.IH_SERVICEORDER_FULLDETAIL_TBL D

        LEFT JOIN PRD_DB2.HS_DW_VIEWS.NPMATFISCALDT_NEW F
        ON D.SO_CRT_DT = F.CAL_DT
        LEFT JOIN PRD_DB2.HS_DW_VIEWS.NPMATFISCALDT_NEW F2
        ON COALESCE(D.SVC_ORD_CLO_DT,D.FNL_SVC_CAL_DT) = F2.CAL_DT
        WHERE 

        D.SO_STS_DT = DATEADD(DAY, -1, CURRENT_DATE)
        AND D.PAY_MET NOT IN ('D2C','PA', 'HW Init', 'IW', 'IWBND', 'PT')
        AND D.SO_STS_CD = 'CA'
        ),D
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,D.SERVICER
            ,D.PROVIDER_ORDER_NO
            ,D.SERVICETYPE
            ,D.RGN_NO
            ,D.SVC_PRD_PLN_CD
            ,D.AP_PCHS_DATE
            ,D.AP_CURR_ST_DATE
            ,D.AP_CURR_EXP_DATE
            ,D.AP_CANC_IND
            ,D.SG_CANC_DATE
            ,D.PAY_MET
            ,D.PAYMENT_TYP_GRP
            ,D.NAI_CVG_CD
            ,D.PAID_TYP
            ,D.THD_PTY_FLG
            ,D.CLIENT_NM
            ,D.CLIENT_CVG_CD
            ,CASE
                    WHEN (D.CLIENT_NM IN('Assurant - PA') AND PAY_MET IN('PA','SP')) THEN 'Assurant_PA'
                    WHEN (D.CLIENT_NM NOT IN('Assurant - PA') AND PAY_MET IN('PA','SP')) THEN 'Backbook_PA' 
                    ELSE 'Other_Pay_Met'
            END PAY_MET_OBL
            ,D.CTA_NO
            ,D.ATH_NO
            ,D.PO_NO
            ,D.ORI_THD_PTY_ID
            ,D.THD_PTY_ID
            ,D.CMB_THD_PTY_ID
            ,D.PM_CHK_CD_INIT
            ,D.PM_CHK_CD
            ,D.PM_CHK_FLG
            ,D.SO_HPR_NED_FL
            ,D.HS_SP_CD
            ,D.NPS_SP_CD
            ,D.MDS_SP_CD
            ,D.MDS_CD
            ,D.PSV_DIV_OGP_NO
            ,D.MFG_BND_NM
            ,CASE WHEN (MFG_BND_NM LIKE 'KENM%' OR MFG_BND_NM = 'SEARS') THEN REPLACE(TRIM(D.PSV_ITM_MDL_NO),CHAR(46),'') ELSE TRIM(D.PSV_ITM_MDL_NO) END PSV_ITM_MDL_NO
            ,D.PSV_MDS_SRL_NO SERIAL_NO
            ,D.SL_DT
            ,D.SO_PY_MET_CD_INIT
            ,D.SO_PY_MET_CD
            ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(D.SVC_RQ_DS),CHAR(10),' '),CHAR(13),' '),CHAR(34),' '),CHAR(42),' '),CHAR(124),' ') SVC_RQ_DS
            ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(D.SO_INS_1_DS),CHAR(10),' '),CHAR(13),' '),CHAR(34),' '),CHAR(42),' '),CHAR(124),' ') SO_INS_1_DS
            ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(D.SO_INS_2_DS),CHAR(10),' '),CHAR(13),' '),CHAR(34),' '),CHAR(42),' '),CHAR(124),' ') SO_INS_2_DS
            ,D.CHANNEL
            ,D.CRT_UN_TYP_CD
            ,D.CRT_UN_NM
            ,D.SO_CRT_UN_NO
            ,D.SO_CRT_EMP_NO
            ,D.SO_CRT_DT
            ,D.SVC_ORI_SCH_DT
            ,D.SVC_SCH_DT
            ,D.SO_STS_CD
            ,D.SO_STS_DT
            ,D.COMPLETE_CODE
            ,D.SUB_ACN_CD
            ,D.SVC_CAL_TYPE
            ,D.PRY_JOB_CD
            ,D.JOB_CD_CRG_CD
            ,D.JOB_CD_DS
            ,D.CANCEL_CODE
            ,CASE WHEN D.CANCEL_CODE IS NULL THEN NULL ELSE R.SCH_RSN_DS END CANCEL_RSN
            ,D.N_ATP
            ,D.N_NH
            ,D.N_OTH
            ,D.TST_TM_MIN_QT TRANSIT_TM
            ,D.SVC_ATP_TOT_MIN_QT SVC_ATP_TOT_MIN
            ,D.PRT_GRS_AM_CC
            ,D.LAB_GRS_AM_CC
            ,D.TOT_GRS_AM_CC
            ,D.SVC_ORD_CLL_AM
            ,D.PARTS_USED
            ,D.PARTS_INSTALLED
            ,D.PARTS_UNINSTALLED
            ,D.RESCHEDULES
            ,D.FST_ATP_DT
            ,D.LST_ATP_DT
            ,D.LST_ATP_TECH_ID_NO
            ,REPLACE(REPLACE(REPLACE(D.LST_ATP_TECH_CMT_1,CHAR(10),' '),CHAR(13),' '),CHAR(34),'')||REPLACE(REPLACE(REPLACE(D.LST_ATP_TECH_CMT_2,CHAR(10),' '),CHAR(13),' '),CHAR(34),'') LST_TEC_CMTS
            ,D.SVC_ORD_CLO_DT
            ,CASE
                    WHEN D.SO_CRT_DT < '2022-04-10' THEN H.TIER
                    WHEN D.SO_CRT_DT >= '2022-04-10' THEN H.NEW_TIER
                    ELSE ''
            END TIER
            ,C.CN_NAME_1ST
            ,C.CN_NAME_LAST
            ,C.CN_HOUSE_RTE_NO
            ,C.CN_STREET_NME
            ,C.CN_STREET_SFX
            ,C.CN_TOWN CITY
            ,C.CN_ST_PROV STATE
            ,C.CN_ZIP_PC
            ,C.CN_TEL_NO
            ,D.SVC_CUS_ID_NO
            ,D.ITM_SUF_NO
            ,D.AGR_SUF_NO
            ,T.FISCAL_YR_MTH_CREATED
            ,T.FISCAL_YR_MTH_CLOSED
            ,D.CYCLE_TIME
            ,L.THD_PTY_DS PROCID_DS
            ,D.FNL_SVC_CAL_DT
            ,D.WHR_BGT_FTY_ID_NO
            ,L2.DED_AM SERVICE_FEE_AM
            ,CASE 
                WHEN TRIM(D.NPS_SP_CD) IN('AELAUNDRY','LAUNDRY','LAUNDRYHE','LWLAUNDRY','MISC26','PRLAUNDRY') THEN 'LAUNDRY'
                WHEN TRIM(D.NPS_SP_CD) IN('AEREFRIG','LGREFRIDGE','LWREFRIG','MAREFRIG','MISC46','REFRIGERAT','REFRIGHE','REFRIGRC','SUBZERO') THEN 'REFRIGERATION'
                WHEN TRIM(D.NPS_SP_CD) IN('AECOOKING1','AECOOKING2','COOKING1','COOKING1HE','COOKING2','LWCOOKING1','LWCOOKING2','LWMWAVEBI','MACOOKING1','MACOOKING2','MICROSHARP','MISC22','MWAVEBI','ODKITCHN') THEN 'COOK'
                WHEN TRIM(D.NPS_SP_CD) IN('AEDISHCOMP','AEWPDISH','DISH/COMP','DISHCOMPHE','LWDISHCOMP','MADISHCOMP','WPDISH') THEN 'DISH'
            END SPECIALTY
        FROM PRD_DB2.HS_PERM_TBLS.IH_SERVICEORDER_FULLDETAIL_TBL D
        JOIN T
        ON D.SVC_UN_NO = T.SVC_UN_NO AND D.SO_NO = T.SO_NO
        LEFT JOIN PRD_NFDT.NFDT_PERM_TBLS.REF_RESCHEDULECODES R
        ON R.SCH_RSN_CD = D.CANCEL_CODE
        LEFT JOIN IH_DATASCIENCE.HS_REFERENCE.GEO_HIERARCHY H
        ON D.CN_ZIP_PC = H.ZIP_CODE AND D.SVC_UN_NO = H.DISTRICT
        LEFT JOIN PRD_DB2.HS_DW_VIEWS.NPJXTCN C
        ON D.SVC_CUS_ID_NO = C.CN_CUST_KEY
        --LEFT JOIN DEV_NFDT.NFDT_PERM_TBLS.REF_PROCID_CLIENT_LIST L
        LEFT JOIN PRD_DB2.BATCH.NPSXTP3 L
        ON D.CMB_THD_PTY_ID = L.THD_PTY_ID AND L.MDS_CD = 'ALL-MDS'
        LEFT JOIN PRD_DB2.BATCH.NPSXTP3 L2
        ON D.ORI_THD_PTY_ID = L2.THD_PTY_ID AND L2.MDS_CD = 'ALL-MDS'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100
        ),
        RP
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,LISTAGG(RP.PARSOCOMPLETEDATE,' | ') WITHIN GROUP (ORDER BY RP.PARSOCOMPLETEDATE ASC) PARENT_SO_CLO_DT
            ,LISTAGG(RP.PARNPSSERVICEORDER,' | ') WITHIN GROUP (ORDER BY RP.PARSOCOMPLETEDATE ASC) PARENT_RCL_SO_NO
            ,LISTAGG(RP.RECALLDAYS,' | ') WITHIN GROUP (ORDER BY RP.PARSOCOMPLETEDATE ASC) PARENT_RCL_DAYS
        FROM D
        JOIN
                    (SELECT *
                        ,ROW_NUMBER() OVER (
                            PARTITION BY PARNPSUNITNUMBER||PARNPSSERVICEORDER
                            ORDER BY PARNPSUNITNUMBER||PARNPSSERVICEORDER DESC
                            ) AS ROW_NUM
                    FROM PRD_NFDT.NFDT_METRIC_TBLS.TBL_ARCHIVESORECALLS
                    QUALIFY ROW_NUM = 1
                    ) RP
        ON D.SVC_UN_NO = RP.CHDNPSUNITNUMBER AND D.SO_NO = RP.CHDNPSSERVICEORDER AND D.SVC_UN_NO = RP.PARNPSUNITNUMBER
        GROUP BY 1,2
        ),
        RC
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,LISTAGG(RC.CHDSOCREATEDATE,' | ') WITHIN GROUP (ORDER BY RC.CHDNPSSERVICEORDER ASC) CHILD_SO_CREATE_DT
            ,LISTAGG(RC.CHDNPSSERVICEORDER,' | ') WITHIN GROUP (ORDER BY RC.CHDNPSSERVICEORDER ASC) CHILD_RCL_SO_NO
            ,LISTAGG(DATEDIFF(dd,D.SVC_ORD_CLO_DT,RC.CHDSOCREATEDATE),' | ') WITHIN GROUP (ORDER BY RC.CHDNPSSERVICEORDER ASC) CHILD_RCL_DAYS
        FROM D
        JOIN
                    (SELECT *
                        ,ROW_NUMBER() OVER (
                            PARTITION BY CHDNPSUNITNUMBER||CHDNPSSERVICEORDER
                            ORDER BY CHDNPSUNITNUMBER||CHDNPSSERVICEORDER DESC
                            ) AS ROW_NUM
                    FROM PRD_NFDT.NFDT_METRIC_TBLS.TBL_ARCHIVESORECALLS
                    QUALIFY ROW_NUM = 1
                    ) RC
        ON D.SVC_UN_NO = RC.PARNPSUNITNUMBER AND D.SO_NO = RC.PARNPSSERVICEORDER AND D.SVC_UN_NO = RC.CHDNPSUNITNUMBER
        GROUP BY 1,2
        ),
        A
        AS
        (
        SELECT TRIM(A.NPSUNITNUMBER) NPSUNITNUMBER
            ,TRIM(A.NPSSERVICEORDER) NPSSERVICEORDER
            ,LISTAGG(A.ATTEMPTSEQUENCE,' | ') WITHIN GROUP (ORDER BY A.ATTEMPTSEQUENCE ASC) SVC_ATMPT_NO
            ,LISTAGG(DATEDIFF(minute,A.ARRIVETIME ,A.DEPARTTIME),' | ') WITHIN GROUP (ORDER BY A.ATTEMPTSEQUENCE ASC) SVC_ATP_TOT_MIN
            ,LISTAGG(DISTINCT SA.SVC_CAL_DT,' | ') WITHIN GROUP (ORDER BY SA.SVC_CAL_DT ASC) ATMPT_SVC_CAL_DT
            ,LISTAGG(SA.SVC_TEC_EMP_NO,' | ') WITHIN GROUP (ORDER BY A.ATTEMPTSEQUENCE ASC) SVC_TEC_EMP_ID_NO
            ,LISTAGG(SA.SVC_CAL_CD,' | ') WITHIN GROUP (ORDER BY A.ATTEMPTSEQUENCE ASC) SVC_CAL_CD
            ,LISTAGG(CONCAT(SA.SVC_ATP_CMT_1_DS,SA.SVC_ATM_CMT_2_DS), ' | ') WITHIN GROUP (ORDER BY A.ATTEMPTSEQUENCE ASC) SVC_TEC_CMMTS
        FROM PRD_NFDT.NFDT_METRIC_TBLS.TBL_ARCHIVESOATTEMPTDETAILS A
        JOIN T
        ON TRIM(T.SVC_UN_NO) = TRIM(A.NPSUNITNUMBER) AND TRIM(T.SO_NO) = TRIM(A.NPSSERVICEORDER)
        JOIN PRD_DB2.HS_DW_VIEWS.NPSXTSA SA
        ON TRIM(SA.SVC_UN_NO) = TRIM(T.SVC_UN_NO) AND TRIM(SA.SO_NO) = TRIM(T.SO_NO) AND A.ATTEMPTDATE = SA.SVC_CAL_DT
        GROUP BY 1,2
        ),
        C1
        AS
        (
        SELECT DISTINCT C.SVC_UN_NO
            ,C.SO_NO
            ,C.CLM_REF_NO
            ,C.CLM_THD_PTY_ID
            ,C.CLM_OBL_NO
            ,C.CLM_STS_CD
            ,C.SBM_DT
            ,C.REJ_DT
            ,C.PD_DT
            ,C.CLM_CLO_CD
            ,C.LAB_REQ_AM
            ,C.LAB_APV_AM
            ,C.PRT_REQ_AM
            ,C.PRT_APV_AM
            ,C.TAX_REQ_AM
            ,C.TAX_APV_AM
            ,C.TOT_REQ_AM
            ,C.TOT_APV_AM
            ,C.CLO_DT
            ,C.CLM_REV_FL
            ,CLO.CLM_CLO_DS
        FROM PRD_CLAIMS.BATCH.NCCXTCM_CLM C
        JOIN D
        ON D.SVC_UN_NO = C.SVC_UN_NO AND D.SO_NO = C.SO_NO
        LEFT JOIN PRD_CLAIMS.BATCH.NCCXTCL_CLO CLO
        ON C.CLM_CLO_CD = CLO.CLM_CLO_CD
        ),
        C
        AS
        (
        SELECT C.SVC_UN_NO
            ,C.SO_NO
            ,C.CLM_REF_NO
            ,C.CLM_OBL_NO
            ,C.CLM_THD_PTY_ID
            ,C.CLM_STS_CD
            ,C.SBM_DT
            ,C.REJ_DT
            ,C.PD_DT
            ,C.CLM_CLO_CD
            ,C.LAB_REQ_AM
            ,C.LAB_APV_AM
            ,C.PRT_REQ_AM
            ,C.PRT_APV_AM
            ,C.TAX_REQ_AM
            ,C.TAX_APV_AM
            ,C.TOT_REQ_AM
            ,C.TOT_APV_AM
            ,C.CLO_DT
            ,C.CLM_REV_FL
            ,C.CLM_CLO_DS
            ,SUM(P.LAB_PD_AM) P_LAB_APV_AM
            ,SUM(P.PRT_PD_AM) P_PRT_APV_AM
            ,SUM(P.OTH_PD_AM) P_OTH_APV_AM
            ,SUM(P.TOT_PD_AM) P_TOT_APV_AM
        FROM C1 C
        LEFT JOIN PRD_CLAIMS.BATCH.NCCXTCP_PY P
        ON C.CLM_REF_NO = P.CLM_REF_NO
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
        ),
        C3
        AS
        (
        SELECT C.SVC_UN_NO
            ,C.SO_NO
            ,C.CLM_REF_NO
            ,C.CLM_OBL_NO
            ,C.CLM_THD_PTY_ID
            ,C.CLM_STS_CD
            ,C.SBM_DT
            ,C.REJ_DT
            ,C.PD_DT
            ,C.CLM_CLO_CD
            ,C.LAB_REQ_AM
            ,CASE WHEN C.P_LAB_APV_AM IS NOT NULL AND C.P_LAB_APV_AM >= C.LAB_APV_AM THEN C.P_LAB_APV_AM ELSE C.LAB_APV_AM END LAB_APV_AM
            ,C.PRT_REQ_AM
            ,CASE WHEN C.P_PRT_APV_AM IS NOT NULL AND C.P_PRT_APV_AM >= C.PRT_APV_AM THEN C.P_PRT_APV_AM ELSE C.PRT_APV_AM END PRT_APV_AM
            ,C.TAX_REQ_AM
            ,C.TAX_APV_AM
            ,C.TOT_REQ_AM
            ,CASE WHEN C.P_TOT_APV_AM IS NOT NULL AND C.P_TOT_APV_AM >= C.TOT_APV_AM THEN C.P_TOT_APV_AM ELSE C.TOT_APV_AM END TOT_APV_AM
            ,C.CLO_DT
            ,C.CLM_REV_FL
            ,C.CLM_CLO_DS
        FROM C
        ),
        C2
        AS
        (					
        SELECT C.SVC_UN_NO
            ,C.SO_NO
            ,LISTAGG(C.CLM_REF_NO,' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) CLM_REF_NO
            ,LISTAGG(C.CLM_THD_PTY_ID,' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) CLM_THD_PTY_ID
            ,LISTAGG(C.CLM_OBL_NO,' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) CLM_OBL_NO
            ,LISTAGG(C.CLM_STS_CD,' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) CLM_STS_CD
            ,LISTAGG((CASE WHEN (TRIM(C.SBM_DT) = '' OR C.SBM_DT IS NULL) THEN '1111-11-11' ELSE C.SBM_DT END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) SBM_DT
            ,LISTAGG((CASE WHEN (TRIM(C.REJ_DT) = '' OR C.REJ_DT IS NULL) THEN '1111-11-11' ELSE C.REJ_DT END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) REJ_DT
            ,LISTAGG((CASE WHEN (TRIM(C.PD_DT) = '' OR C.PD_DT IS NULL) THEN '1111-11-11' ELSE C.PD_DT END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) PD_DT
            ,LISTAGG((CASE WHEN (TRIM(C.CLM_CLO_CD) = '' OR C.CLM_CLO_CD IS NULL) THEN '*' ELSE C.CLM_CLO_CD||'-'||C.CLM_CLO_DS END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) CLM_CLO_CD
            ,LISTAGG((CASE WHEN (TRIM(C.LAB_REQ_AM) = '' OR C.LAB_REQ_AM IS NULL) THEN 0 ELSE C.LAB_REQ_AM END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) LAB_REQ_AM
            ,LISTAGG((CASE WHEN (TRIM(C.LAB_APV_AM) = '' OR C.LAB_APV_AM IS NULL) THEN 0 ELSE C.LAB_APV_AM END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) LAB_APV_AM
            ,LISTAGG((CASE WHEN (TRIM(C.PRT_REQ_AM) = '' OR C.PRT_REQ_AM IS NULL) THEN 0 ELSE C.PRT_REQ_AM END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) PRT_REQ_AM
            ,LISTAGG((CASE WHEN (TRIM(C.PRT_APV_AM) = '' OR C.PRT_APV_AM IS NULL) THEN 0 ELSE C.PRT_APV_AM END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) PRT_APV_AM
            ,LISTAGG((CASE WHEN (TRIM(C.TAX_REQ_AM) = '' OR C.TAX_REQ_AM IS NULL) THEN 0 ELSE C.TAX_REQ_AM END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) TAX_REQ_AM
            ,LISTAGG((CASE WHEN (TRIM(C.TAX_APV_AM) = '' OR C.TAX_APV_AM IS NULL) THEN 0 ELSE C.TAX_APV_AM END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) TAX_APV_AM
            ,LISTAGG((CASE WHEN (TRIM(C.TOT_REQ_AM) = '' OR C.TOT_REQ_AM IS NULL) THEN 0 ELSE C.TOT_REQ_AM END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) TOT_REQ_AM
            ,LISTAGG((CASE WHEN (TRIM(C.TOT_APV_AM) = '' OR C.TOT_APV_AM IS NULL) THEN 0 ELSE C.TOT_APV_AM END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) TOT_APV_AM
            ,LISTAGG((CASE WHEN (TRIM(C.CLO_DT) = '' OR C.CLO_DT IS NULL) THEN '1111-11-11' ELSE C.CLO_DT END),' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) CLO_DT
            ,LISTAGG(C.CLM_REV_FL,' | ') WITHIN GROUP (ORDER BY (CASE WHEN C.CLM_STS_CD = 'CLO' THEN 1 WHEN C.CLM_STS_CD = 'SUB' THEN 2 WHEN C.CLM_STS_CD = 'PPY' THEN 3 WHEN C.CLM_STS_CD = 'FPY' THEN 4 END) ASC,C.SBM_DT ASC) CLM_REV_FL
        FROM C3 C
        JOIN D
        ON D.SVC_UN_NO = C.SVC_UN_NO AND D.SO_NO = C.SO_NO
        --WHERE C.CLM_STS_CD IN('SUB','FPY','PPY')
        GROUP BY 1,2
        ),
        P
        AS
        (
        SELECT TRIM(P.SVC_UN_NO) SVC_UN_NO
            ,TRIM(P.SO_NO) SO_NO
            ,SUM(CASE WHEN P.SVC_PRT_STS_CD = 'I' THEN P.PRT_CST_PRC_AM * (CASE WHEN P.PRT_ORD_QT > 0 THEN P.PRT_ORD_QT ELSE P.SVC_ATP_PRT_QT END) END) PRT_INS_COST_SUM
            ,SUM(CASE WHEN P.SVC_PRT_STS_CD = 'I' THEN P.PRT_SEL_PRC_AM END) PRT_INS_SELL_SUM
            ,SUM(CASE WHEN P.SVC_PRT_STS_CD = 'I' THEN Z.COST * (CASE WHEN P.PRT_ORD_QT > 0 THEN P.PRT_ORD_QT ELSE P.SVC_ATP_PRT_QT END) END) LAWSON_INS_COST_SUM
            ,SUM(CASE WHEN P.SVC_PRT_STS_CD = 'I' THEN Z.SELLPRICE * (CASE WHEN P.PRT_ORD_QT > 0 THEN P.PRT_ORD_QT ELSE P.SVC_ATP_PRT_QT END) END) LAWSON_INS_SELL_SUM
            ,LISTAGG(P.SVC_DIV_NO||'-'||P.PRT_PRC_LIS_SRC_NO||'-'||TRIM(P.SVC_PRT_NO),' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) SKU
            ,LISTAGG(P.PRT_DS,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_DS
            ,LISTAGG(P.PRT_SEQ_NO,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_SEQ_NO
            ,LISTAGG(CASE WHEN P.PRT_ORD_QT > 0 THEN P.PRT_ORD_QT ELSE P.SVC_ATP_PRT_QT END,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_ORD_QT
            --,LISTAGG(P.SVC_ATP_PRT_QT,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_ORD_QT
            ,LISTAGG(P.CVG_CD,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_CVG_CD
            ,LISTAGG(P.PRT_CST_PRC_AM * (CASE WHEN P.PRT_ORD_QT > 0 THEN P.PRT_ORD_QT ELSE P.SVC_ATP_PRT_QT END),' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_CST_PRC_AM
            ,LISTAGG(P.PRT_SEL_PRC_AM,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_SEL_PRC_AM
            ,LISTAGG(ROUND(Z.COST * (CASE WHEN P.PRT_ORD_QT > 0 THEN P.PRT_ORD_QT ELSE P.SVC_ATP_PRT_QT END),2),' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) LAWSON_COST
            ,LISTAGG(ROUND(Z.SELLPRICE * (CASE WHEN P.PRT_ORD_QT > 0 THEN P.PRT_ORD_QT ELSE P.SVC_ATP_PRT_QT END),2),' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) LAWSON_SELL
            ,LISTAGG(P.SVC_PRT_TYP_CD,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) SVC_PRT_TYP_CD
            ,LISTAGG(P.SVC_PRT_STS_CD,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_STS_CD
            ,LISTAGG(P.SVC_PRT_STS_DT,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_STS_DT
            --,LISTAGG(L.PRT_ORD_STS_CD,' | ') WITHIN GROUP (ORDER BY L.PRT_SEQ_NO ASC) PRT_ORD_STS_CD
            ,LISTAGG(P.PRT_ORD_DT,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) PRT_ORD_DT
            ,LISTAGG(P.EMP_ID_NO,' | ') WITHIN GROUP (ORDER BY P.PRT_SEQ_NO ASC) ORD_PRT_EMP_ID_NO
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTPN P
        JOIN D
        ON TRIM(D.SVC_UN_NO) = TRIM(P.SVC_UN_NO) AND TRIM(D.SO_NO) = TRIM(P.SO_NO)
        LEFT JOIN PRD_ITEM_MASTER.HS_ITEM_MASTER.ZZ_DBMATCH_PART_MASTER_TBL Z
        ON P.SVC_DIV_NO = LPAD(Z.DIV,3,'0') AND P.PRT_PRC_LIS_SRC_NO = Z.PLS AND TRIM(P.SVC_PRT_NO) = TRIM(Z.PART)
        --WHERE P.SVC_PRT_STS_CD = 'I'
        --WHERE (P.SVC_PRT_TYP_CD = 'L' AND P.SVC_PRT_STS_CD = 'I')
        GROUP BY 1,2
        ),
        PO
        AS
        (
        SELECT O.SVC_UN_NO
            ,O.SO_NO
            ,LISTAGG(L.PRT_ORD_STS_CD,' | ') WITHIN GROUP (ORDER BY L.PRT_SEQ_NO ASC) PRT_ORD_STS_CD
        FROM PRD_DB2.HS_DW_VIEWS.NPNXTPO O
        JOIN D
        ON TRIM(D.SVC_UN_NO) = TRIM(O.SVC_UN_NO) AND TRIM(D.SO_NO) = TRIM(O.SO_NO)
        JOIN PRD_DB2.HS_DW_VIEWS.NPNXTPL L
        ON O.PRT_ORD_NO = L.PRT_ORD_NO AND O.PRT_ORD_DT = L.PRT_ORD_DT
        GROUP BY 1,2
        ),
        J
        AS
        (
        SELECT TRIM(J.SVC_UN_NO) SVC_UN_NO
            ,TRIM(J.SO_NO) SO_NO
            ,LISTAGG(CONCAT(TRIM(J.JOB_CD),'-',L.JOBCODE_DESCRIPTION),' | ') WITHIN GROUP (ORDER BY J.JOB_CD_SEQ_NO ASC) ALL_JOB_CODES_DS
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTSJ J
        LEFT JOIN 
            (SELECT DISTINCT JOBCODE
                ,JOBCODE_DESCRIPTION
                ,SPECIALTY_CODE
            FROM PRD_NFDT.NFDT_PERM_TBLS.REF_JOBCODELIST) L
        ON TRIM(J.JOB_CD) = TRIM(L.JOBCODE)
        JOIN T
        ON TRIM(T.SVC_UN_NO) = TRIM(J.SVC_UN_NO) AND TRIM(T.SO_NO) = TRIM(J.SO_NO)
        WHERE (L.SPECIALTY_CODE = T.MDS_SP_CD OR L.SPECIALTY_CODE IS NULL)
        --AND J.JOB_CD IN('95000','95001','95002','95003','95004')
        GROUP BY 1,2
        ),
        E
        AS
        (
        SELECT D.SVC_UN_NO
            ,E.EMPLOYEE_ENTERPRISEID RACFID
            ,N.EMP_RAC_ID_NO eID
            ,N.EMP_FST_NM||' '||EMP_LST_NM EMPLOYEE_NAME
            ,E.MANAGER_NAME
            ,N.EMP_TRM_DT
            ,N.EMP_ID_NO
            ,L.TITLE
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTEI N
        JOIN D
        ON D.LST_ATP_TECH_ID_NO = N.EMP_ID_NO AND D.SVC_UN_NO = N.SVC_UN_NO
        LEFT JOIN PRD_NFDT.NFDT_PERM_TBLS.TECHNICIAN_ROSTER_CURRENT E
        ON D.LST_ATP_TECH_ID_NO = E.EMPLOYEE_NPSID AND D.SVC_UN_NO = E.SERVICE_UNIT
        --LEFT JOIN PRD_HR.HR.LDAP L
        --ON E.EMPLOYEE_ENTERPRISEID = UPPER(L.UID) AND L.BUSINESSCATEGORY IN('PPX','TSH') AND L.SEARSHREMPLOYEESTATUS = 'A'
        LEFT JOIN
                    (SELECT *
                        ,ROW_NUMBER() OVER (
                            PARTITION BY UID
                            ORDER BY JOBDATE DESC
                            ) AS ROW_NUM
                    FROM PRD_HR.HR.LDAP
                    QUALIFY ROW_NUM = 1
                    ) L
        ON E.EMPLOYEE_ENTERPRISEID = UPPER(L.UID) AND L.BUSINESSCATEGORY IN('PPX','TSH') AND L.SEARSHREMPLOYEESTATUS = 'A'
        GROUP BY 1,2,3,4,5,6,7,8
        ),
        O
        AS
        (
        SELECT *
        FROM (
            SELECT ENTERPRISEID
            ,SERVICEUNIT
            ,ENDDATE
            ,ROW_NUMBER() OVER (PARTITION BY ENTERPRISEID ORDER BY STARTDATE DESC) AS ROW_NUM
            FROM DEV_NFDT.NFDT_PERM_TBLS.TECHNICIAN_NPSID
            QUALIFY ROW_NUM = 1
            ) O
        JOIN E
        ON E.eID = O.ENTERPRISEID
        ),
        R
        AS
        (
        SELECT O.ENTERPRISEID
            ,O.SERVICEUNIT
            ,O.ENDDATE
            ,C.EMPLOYEE_ENTERPRISEID
            ,C.EMPLOYEE_NAME
            ,C.MANAGER_NAME
        FROM O
        LEFT JOIN PRD_NFDT.NFDT_PERM_TBLS.TECHNICIAN_ROSTER_CURRENT C
        ON O.ENTERPRISEID = C.EMPLOYEE_ENTERPRISEID
        GROUP BY 1,2,3,4,5,6
        ),
        M1
        AS
        (
        SELECT M.SVC_UN_NO
            ,M.SO_NO
            ,M.PRT_TOT_AM PRT_TOT_AM_PT
            ,M.PRT_NET_AM PRT_NET_AM_PT
            ,M.LAB_TOT_AM LAB_TOT_AM_PT
            ,M.LAB_NET_AM LAB_NET_AM_PT
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTMO M
        JOIN D
        ON D.SVC_UN_NO = M.SVC_UN_NO AND D.SO_NO = M.SO_NO
        WHERE M.CVG_CD = 'PT'
        ),
        M2
        AS
        (
        SELECT M.SVC_UN_NO
            ,M.SO_NO
            ,M.PRT_TOT_AM PRT_TOT_AM_CC
            ,M.PRT_NET_AM PRT_NET_AM_CC
            ,M.LAB_TOT_AM LAB_TOT_AM_CC
            ,M.LAB_NET_AM LAB_NET_AM_CC
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTMO M
        JOIN D
        ON D.SVC_UN_NO = M.SVC_UN_NO AND D.SO_NO = M.SO_NO
        WHERE M.CVG_CD = 'CC'
        ),
        M3
        AS
        (
        SELECT M.SVC_UN_NO
            ,M.SO_NO
            ,M.PRT_TOT_AM PRT_TOT_AM_IW
            ,M.PRT_NET_AM PRT_NET_AM_IW
            ,M.LAB_TOT_AM LAB_TOT_AM_IW
            ,M.LAB_NET_AM LAB_NET_AM_IW
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTMO M
        JOIN D
        ON D.SVC_UN_NO = M.SVC_UN_NO AND D.SO_NO = M.SO_NO
        WHERE M.CVG_CD = 'IW'
        ),
        M4
        AS
        (
        SELECT M.SVC_UN_NO
            ,M.SO_NO
            ,M.PRT_TOT_AM PRT_TOT_AM_SP
            ,M.PRT_NET_AM PRT_NET_AM_SP
            ,M.LAB_TOT_AM LAB_TOT_AM_SP
            ,M.LAB_NET_AM LAB_NET_AM_SP
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTMO M
        JOIN D
        ON D.SVC_UN_NO = M.SVC_UN_NO AND D.SO_NO = M.SO_NO
        WHERE M.CVG_CD = 'SP'
        ),
        N
        AS
        (
        SELECT R.SVC_UN_NO
            ,R.SO_NO
            ,R.MOD_EXT_DT
            ,R.MOD_ID
        FROM (
            SELECT *
            ,ROW_NUMBER() OVER (PARTITION BY SVC_UN_NO||SO_NO ORDER BY MOD_EXT_DT ASC) AS ROW_NUM
            FROM PRD_DB2.CDC.NPSXTSR
            WHERE SO_STS_CD IN('CA','CO','ED')
            QUALIFY ROW_NUM = 1
            ) AS R
        JOIN D
        ON D.SVC_UN_NO = R.SVC_UN_NO AND D.SO_NO = R.SO_NO
        ),
        N2
        AS
        (
        SELECT R.SVC_UN_NO
            ,R.SO_NO
            ,R.MOD_EXT_DT
            ,R.MOD_ID CANCELLED_BY
        FROM (
            SELECT *
            ,ROW_NUMBER() OVER (PARTITION BY SVC_UN_NO||SO_NO	ORDER BY MOD_EXT_DT ASC) AS ROW_NUM
            FROM PRD_DB2.CDC.NPSXTSR
            WHERE SO_STS_CD = 'CA'
            QUALIFY ROW_NUM = 1
            ) AS R
        JOIN D
        ON D.SVC_UN_NO = R.SVC_UN_NO AND D.SO_NO = R.SO_NO
        ),
        N3
        AS
        (
        SELECT N2.SVC_UN_NO
            ,N2.SO_NO
            ,N2.CANCELLED_BY
            ,L.SHCDISPLAYNAME
            ,L.TITLE
            ,L.MANAGER 
            ,N2.MOD_EXT_DT CANC_DT
            --,MAX(L.UPDATED_TS_DW) LST_DT
        FROM N2
        LEFT JOIN (
                    SELECT SHCDISPLAYNAME
                        ,TITLE
                        ,MANAGER
                        ,UPPER(UID) UID
                        ,ROW_NUMBER() OVER (PARTITION BY UID ORDER BY JOBDATE DESC) AS ROW_NUM
                    FROM PRD_HR.HR.LDAP
                    QUALIFY ROW_NUM = 1
                ) AS L
        ON L.UID = TRIM(N2.CANCELLED_BY)
        GROUP BY 1,2,3,4,5,6,7
        ),
        N4
        AS
        (
        SELECT N3.SVC_UN_NO
            ,N3.SO_NO
            ,N3.CANC_DT
            ,N3.CANCELLED_BY
            ,CASE WHEN R.PARTNER_NAME IS NULL THEN N3.SHCDISPLAYNAME ELSE R.PARTNER_NAME END CANCELLED_BY_NAME
            ,N3.TITLE CANCELLED_BY_TITLE
            ,N3.MANAGER CANCELLED_BY_MANAGER
        FROM N3
        JOIN D
        ON D.SVC_UN_NO = N3.SVC_UN_NO AND D.SO_NO = N3.SO_NO
        LEFT JOIN DEV_NFDT.NFDT_PERM_TBLS.REF_CREATEID_LIST R
        ON N3.CANCELLED_BY = R.PARTNER_AGENT_ID
        ),
        F
        AS
        (
        SELECT F.SVC_UN_NO
            ,F.SO_NO
            ,LISTAGG(F.USR_ID,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC)
            ,LISTAGG(CASE
                            WHEN UDF_FLD_VAL_TX = 'CD' THEN 'Customer Declined Repair'
                            WHEN UDF_FLD_VAL_TX = 'CR' THEN 'Customer Refused to pay'
                            WHEN UDF_FLD_VAL_TX = 'MA' THEN 'Multiple Appliance'
                            WHEN UDF_FLD_VAL_TX = 'PW' THEN 'Pre-Paid Warranty'
                            WHEN UDF_FLD_VAL_TX = 'RR' THEN 'Repeat Repair'
                            WHEN UDF_FLD_VAL_TX = 'SS' THEN '2nd Servicer'
                            ELSE 'None' END,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) SERVICE_FEE_OVERIDE_RSN
            ,LISTAGG(DATE(F.CRT_TS),' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) SERVICE_FEE_OVERIDE_DATE
            ,LISTAGG(D.SERVICE_FEE_AM,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) SERVICE_FEE_AM
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTFS F
        JOIN D
        ON D.SVC_UN_NO = F.SVC_UN_NO AND D.SO_NO = F.SO_NO
        WHERE F.UDF_FLD_NM = 'deducOverrideRsn'
        GROUP BY 1,2
        ),
        FC
        AS
        (
        SELECT F.SVC_UN_NO
            ,F.SO_NO
            ,LISTAGG(F.USR_ID||'-'||DATE(F.CRT_TS)||'-'||F.UDF_FLD_VAL_TX,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) ADDITIONAL_COMMENTS
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTFS F
        JOIN T
        ON T.SVC_UN_NO = F.SVC_UN_NO AND T.SO_NO = F.SO_NO
        WHERE F.UDF_FLD_NM = 'additionalComments'
        GROUP BY 1,2
        ),
        FA
        AS
        (
        SELECT F.SVC_UN_NO
            ,F.SO_NO
            ,LISTAGG(F.USR_ID||'-'||DATE(F.CRT_TS)||'-'||F.UDF_FLD_VAL_TX,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) AUTH_CODE
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTFS F
        JOIN T
        ON T.SVC_UN_NO = F.SVC_UN_NO AND T.SO_NO = F.SO_NO
        WHERE F.UDF_FLD_NM = 'authCode'
        GROUP BY 1,2
        ),
        CP
        AS
        (
        SELECT T.SVC_UN_NO
            ,T.SO_NO
            --,PM.SO_PY_MET_CD
            ,SUM(PM.SVC_ORD_CLL_AM) CUST_PMTS
        FROM PRD_DB2.HS_DAILY_VIEWS.NPSXTPM PM
        JOIN T
        ON T.SVC_UN_NO = PM.SVC_UN_NO AND T.SO_NO = PM.SO_NO
        GROUP BY 1,2--,3
        ),
        DO
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,LISTAGG(N.EMP_RAC_ID_NO,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) RACFID
            ,LISTAGG(N.EMP_RAC_ID_NO,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) eID
            ,LISTAGG(N.EMP_FST_NM||' '||EMP_LST_NM,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) EMPLOYEE_NAME
            ,LISTAGG(E.MANAGER_NAME,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) MANAGER_NAME
            ,LISTAGG(N.EMP_ID_NO,' | ') WITHIN GROUP (ORDER BY F.CRT_TS ASC) EMP_ID_NO
        FROM PRD_DB2.HS_DW_VIEWS.NPSXTFS F
        JOIN D
        ON D.SVC_UN_NO = F.SVC_UN_NO AND D.SO_NO = F.SO_NO
        JOIN PRD_DB2.HS_DW_VIEWS.NPSXTEI N
        ON N.EMP_RAC_ID_NO = F.USR_ID AND N.SVC_UN_NO = F.SVC_UN_NO
        LEFT JOIN PRD_NFDT.NFDT_PERM_TBLS.TECHNICIAN_ROSTER_CURRENT E
        ON N.EMP_ID_NO = E.EMPLOYEE_NPSID AND N.SVC_UN_NO = E.SERVICE_UNIT
        WHERE F.UDF_FLD_NM = 'deducOverrideRsn'
        GROUP BY 1,2--,3,4,5,6,7,8
        ),
        SA  /***** AMANA *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9]{8}[a-zA-Z]{2}') = TRUE THEN 'Y' ELSE 'N' END SRL
        FROM D
        WHERE D.MFG_BND_NM = 'AMANA'
        ),
        SMC  /***** MAGIC CHEF *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9]{8}[a-zA-Z]{2}') = TRUE THEN 'Y' ELSE 'N' END SRL
        FROM D
        WHERE D.MFG_BND_NM = 'MAGIC CHEF'
        ),
        SM  /***** MAYTAG *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9]{8}[a-zA-Z]{2}') = TRUE THEN 'Y' ELSE 'N' END SRL
        FROM D
        WHERE D.MFG_BND_NM = 'MAYTAG'
        ),
        SS  /***** SAMSUNG *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9a-zA-Z]{6}[a-zA-Z]{2}[1-9ABC]{1}[0-9]{5}[a-zA-Z]{1}') = TRUE THEN 'Y' ELSE 'N' END SRL
        FROM D
        WHERE D.MFG_BND_NM LIKE 'SAMSU%'
        ),
        SW  /***** WHIRLPOOL *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[a-zA-Z]{1}[0-9a-zA-Z]{1}[0-5]{1}[0-3]{1}[0-9]{5}') = TRUE THEN 'Y' ELSE 'N' END SRL
        FROM D
        WHERE D.MFG_BND_NM = 'WHIRLPOOL'
        ),
        SG  /***** GE *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9]{8}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{6}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{8}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{7}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{3}[A-Za-z]{2}[0-9]{5}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[ADFGHLMRSTVZ]{1}[a-zA-Z]{1}[0-9]{6}[a-zA-Z ]{1}') = TRUE THEN 'Y'
                    ELSE 'N' 
            END SRL
        FROM D
        WHERE D.MFG_BND_NM = 'GE' OR D.MFG_BND_NM = 'GE MONOGRAM' OR D.MFG_BND_NM = 'GE PROFILE' OR D.MFG_BND_NM = 'GE CAFE'
        ),
        SJ  /***** JENN AIR *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9]{8}[a-zA-Z]{2}') = TRUE THEN 'Y' ELSE 'N' END SRL
        FROM D
        WHERE D.MFG_BND_NM LIKE 'JENN%'
        ),
        SL  /***** LG *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9]{3}\\w{2}[0-9]{5}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{3}\\w{3}[A-Za-z0-9]{3}[0-9]{3}') = TRUE THEN 'Y'
                    ELSE 'N'
            END SRL
        FROM D
        WHERE D.MFG_BND_NM LIKE 'LG%'
        ),
        SKS  /***** KENMORE SAMSUNG *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9a-zA-Z]{6}[a-zA-Z]{2}[1-9ABC]{1}[0-9]{5}[a-zA-Z]{1}') = TRUE THEN 'Y' ELSE 'N' END SRL
        FROM D
        WHERE (D.MFG_BND_NM LIKE 'KENM%' OR D.MFG_BND_NM = 'SEARS') AND LEFT(D.PSV_ITM_MDL_NO,3) = '401'
        ),
        SKW  /***** KEMORE WHIRLPOOL *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[a-zA-Z]{1}[0-9a-zA-Z]{1}[0-5]{1}[0-3]{1}[0-9]{5}') = TRUE THEN 'Y' ELSE 'N' END SRL
        FROM D
        WHERE (D.MFG_BND_NM LIKE 'KENM%' OR D.MFG_BND_NM = 'SEARS') AND LEFT(D.PSV_ITM_MDL_NO,3) IN('106','110','664','665')
        ),
        SKG  /***** KENMORE GE *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE WHEN RLIKE(D.SERIAL_NO,'[0-9]{8}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{6}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{8}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{7}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{3}[A-Za-z]{2}[0-9]{5}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[ADFGHLMRSTVZ]{1}[a-zA-Z]{1}[0-9]{6}[a-zA-Z ]{1}') = TRUE THEN 'Y'
                    ELSE 'N' 
            END SRL
        FROM D
        WHERE (D.MFG_BND_NM LIKE 'KENM%' OR D.MFG_BND_NM = 'SEARS') AND LEFT(D.PSV_ITM_MDL_NO,3) IN('363','364')
        ),
        SKL  /***** KENMORE LG *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE 
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{3}\\w{2}[0-9]{5}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{3}\\w{3}[A-Za-z0-9]{3}[0-9]{3}') = TRUE THEN 'Y'
                    ELSE 'N'
            END SRL
        FROM D
        WHERE (D.MFG_BND_NM LIKE 'KENM%' OR D.MFG_BND_NM = 'SEARS') AND LEFT(D.PSV_ITM_MDL_NO,3) IN('721','722','795','796')
        ),
        SKD  /***** KENMORE DAEWOO *****/
        AS
        (
        SELECT D.SVC_UN_NO
            ,D.SO_NO
            ,CASE
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{11}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{8}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{1}[0-9]{21}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{8}[A-Za-z0-9]{2}[0-9]{6}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{9}[A-Za-z0-9]{1}[0-9]{6}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{13}[A-Za-z0-9]{3}[0-9]{4}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{2}[A-Za-z]{1}[0-9]{5}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{1}[A-Za-z]{2}[0-9]{8}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{2}[A-Za-z]{2}[0-9]{8}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{1}[A-Za-z]{2}[0-9]{9}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[0-9]{5}[A-Za-z0-9]{1}[0-9]{7}[A-Za-z]{1}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z0-9]{8}[0-9]{1}[A-Za-z0-9]{1}[0-9]{6}') = TRUE THEN 'Y'
                    WHEN RLIKE(D.SERIAL_NO,'[A-Za-z]{2}[0-9]{2}[A-Za-z0-9]{1}[A-Za-z]{1}[0-9]{8}') = TRUE THEN 'Y'
                    ELSE 'N'
            END SRL
        FROM D
        WHERE (D.MFG_BND_NM LIKE 'KENM%' OR D.MFG_BND_NM = 'SEARS') AND LEFT(D.PSV_ITM_MDL_NO,3) = '111'
        )/*,
        AR
        AS
        (
        SELECT TRIM(R.SVC_UN_NO) SVC_UN_NO
            ,TRIM(R.SO_NO) SO_NO
            ,LISTAGG(R.MOD_DT,' | ') WITHIN GROUP (ORDER BY R.MOD_TM,R.SEQ_NO ASC) AR_MOD_DT
            ,LISTAGG(R.EMP_ID_NO,' | ') WITHIN GROUP (ORDER BY R.MOD_TM,R.SEQ_NO ASC) AR_EMP_ID_NO
            ,LISTAGG(R.SO_TXT_DS,' | ') WITHIN GROUP (ORDER BY R.MOD_TM,R.SEQ_NO ASC) AR_SO_TXT_DS
        FROM PRD_DB2.HS_DW_TBLS.NPSXTAR R
        JOIN D
        ON TRIM(D.SVC_UN_NO) = TRIM(R.SVC_UN_NO) AND TRIM(D.SO_NO) = TRIM(R.SO_NO)
        --WHERE (R.SO_TXT_DS LIKE '%L1-%' OR R.SO_TXT_DS LIKE '%PRQ%')
        GROUP BY 1,2
        )*/
        SELECT D.FISCAL_YR_MTH_CREATED
            ,D.FISCAL_YR_MTH_CLOSED
            ,D.SVC_UN_NO
            ,D.SO_NO
            ,D.SERVICER
            ,D.PROVIDER_ORDER_NO
            ,D.SERVICETYPE
            ,D.TIER
            ,D.RGN_NO
            ,D.SVC_CUS_ID_NO
            ,D.ITM_SUF_NO
            ,D.AGR_SUF_NO
            ,D.SVC_PRD_PLN_CD
            ,D.AP_PCHS_DATE
            ,D.AP_CURR_ST_DATE
            ,D.AP_CURR_EXP_DATE
            ,D.AP_CANC_IND
            ,D.SG_CANC_DATE
            ,D.PAY_MET
            ,D.PAYMENT_TYP_GRP
            ,D.NAI_CVG_CD
            ,D.PAID_TYP
            ,D.THD_PTY_FLG
            ,D.CLIENT_NM
            ,D.CLIENT_CVG_CD
            ,D.PAY_MET_OBL
            ,D.CTA_NO
            ,D.ATH_NO
            ,D.PO_NO
            ,D.ORI_THD_PTY_ID
            ,D.THD_PTY_ID
            ,D.CMB_THD_PTY_ID
            ,D.PROCID_DS 
            ,RP.PARENT_SO_CLO_DT
            ,RP.PARENT_RCL_SO_NO
            ,RP.PARENT_RCL_DAYS
            ,RC.CHILD_SO_CREATE_DT
            ,RC.CHILD_RCL_SO_NO
            ,RC.CHILD_RCL_DAYS
            ,D.PM_CHK_CD_INIT
            ,D.PM_CHK_CD
            ,D.PM_CHK_FLG
            ,D.SO_HPR_NED_FL
            ,D.HS_SP_CD
            ,D.NPS_SP_CD
            ,D.MDS_CD
            ,D.SPECIALTY
            ,D.PSV_DIV_OGP_NO
            ,D.MFG_BND_NM
            ,D.PSV_ITM_MDL_NO
            ,D.SERIAL_NO
            ,CASE
                    WHEN SA.SRL = 'Y' OR SMC.SRL = 'Y' OR SM.SRL = 'Y' OR SS.SRL = 'Y' OR SW.SRL = 'Y' OR SG.SRL = 'Y' OR SJ.SRL = 'Y' OR SL.SRL = 'Y' OR SKS.SRL = 'Y' OR SKW.SRL = 'Y' OR SKG.SRL = 'Y' OR SKL.SRL = 'Y' OR SKD.SRL = 'Y' THEN 'Yes'
                    WHEN SA.SRL = 'N' OR SMC.SRL = 'N' OR SM.SRL = 'N' OR SS.SRL = 'N' OR SW.SRL = 'N' OR SG.SRL = 'N' OR SJ.SRL = 'N' OR SL.SRL = 'N' OR SKS.SRL = 'N' OR SKW.SRL = 'N' OR SKG.SRL = 'N' OR SKL.SRL = 'N' OR SKD.SRL = 'N' THEN 'No'
                    ELSE 'Validation Unknown'
            END SERIAL_VALIDATES
            ,D.SL_DT
            ,D.WHR_BGT_FTY_ID_NO
            ,D.SO_PY_MET_CD_INIT
            ,D.SO_PY_MET_CD
            ,D.SVC_RQ_DS
            ,D.SO_INS_1_DS
            ,D.SO_INS_2_DS
            ,D.CHANNEL
            ,D.CRT_UN_TYP_CD
            ,D.CRT_UN_NM
            ,D.SO_CRT_UN_NO
            ,D.SO_CRT_EMP_NO
            ,D.SO_CRT_DT
            ,D.SVC_ORI_SCH_DT
            ,D.SVC_SCH_DT
            ,D.SO_STS_CD
            ,D.SO_STS_DT
            ,D.FNL_SVC_CAL_DT
            ,D.SVC_ORD_CLO_DT
            ,D.COMPLETE_CODE
            ,D.SUB_ACN_CD
            ,D.SVC_CAL_TYPE
            ,D.PRY_JOB_CD
            ,D.JOB_CD_CRG_CD
            ,D.JOB_CD_DS
            ,J.ALL_JOB_CODES_DS
            ,CASE 
                    WHEN (N.MOD_ID = 'NPS772A' AND D.SO_STS_CD IN('CA','CO','ED','CV','CP')) THEN 'TechHub'
                    WHEN (N.MOD_ID <> 'NPS772A' AND D.SO_STS_CD IN('CA','CO','ED','CV','CP')) THEN 'NPS'
                    ELSE ''
            END SYS_CLOSED
            ,D.CANCEL_CODE
            ,D.CANCEL_RSN
            ,N4.CANCELLED_BY
            ,CASE WHEN N4.CANCELLED_BY = 'NPS772A' THEN E.EMPLOYEE_NAME ELSE UPPER(N4.CANCELLED_BY_NAME) END CANCELLED_BY_NAME
            ,N4.CANCELLED_BY_TITLE
            ,N4.CANCELLED_BY_MANAGER
            ,D.N_ATP
            ,D.N_NH
            ,D.N_OTH
            ,D.TRANSIT_TM
            ,D.SVC_ATP_TOT_MIN
            ,D.PRT_GRS_AM_CC
            ,D.LAB_GRS_AM_CC
            ,D.TOT_GRS_AM_CC
            ,D.SVC_ORD_CLL_AM
            ,D.PARTS_USED
            ,D.PARTS_INSTALLED
            ,D.PARTS_UNINSTALLED
            ,D.RESCHEDULES
            ,D.FST_ATP_DT
            ,D.LST_ATP_DT
            ,D.CYCLE_TIME
            ,D.LST_ATP_TECH_ID_NO
            ,E.EID RACFID
            ,E.EMPLOYEE_NAME --CASE WHEN E.EMPLOYEE_NAME IS NULL THEN R.EMPLOYEE_NAME ELSE E.EMPLOYEE_NAME END EMPLOYEE_NAME
            ,E.TITLE
            ,E.MANAGER_NAME --CASE WHEN E.MANAGER_NAME IS NULL THEN R.MANAGER_NAME ELSE E.MANAGER_NAME END MANAGER_NAME
            ,D.LST_TEC_CMTS
            ,A.SVC_ATMPT_NO
            ,A.SVC_ATP_TOT_MIN
            ,A.ATMPT_SVC_CAL_DT
            ,A.SVC_TEC_EMP_ID_NO
            ,A.SVC_CAL_CD
            ,A.SVC_TEC_CMMTS
            ,FC.ADDITIONAL_COMMENTS
            ,FA.AUTH_CODE
            ,IFNULL(CP.CUST_PMTS,0) CUST_PMTS
            --,CP.SO_PY_MET_CD
            ,F.SERVICE_FEE_OVERIDE_RSN
            ,F.SERVICE_FEE_AM
            ,F.SERVICE_FEE_OVERIDE_DATE
            ,DO.eID
            ,DO.EMPLOYEE_NAME
            ,DO.MANAGER_NAME
            ,P.SKU
            ,P.PRT_DS
            ,P.PRT_SEQ_NO
            ,P.PRT_ORD_QT
            ,P.PRT_CVG_CD
            ,P.PRT_CST_PRC_AM
            ,P.PRT_INS_COST_SUM
            --,ROUND(P.PRT_INS_COST_SUM + (P.PRT_INS_COST_SUM * 0.18),2) PRT_INS_COST_PLUS
            ,P.PRT_SEL_PRC_AM
            ,P.PRT_INS_SELL_SUM
            ,P.LAWSON_COST
            ,P.LAWSON_INS_COST_SUM
            ,P.LAWSON_SELL
            ,P.LAWSON_INS_SELL_SUM
            ,P.PRT_STS_CD			
            ,P.SVC_PRT_TYP_CD
            ,P.PRT_STS_DT
            ,P.PRT_ORD_DT
            ,P.ORD_PRT_EMP_ID_NO
            ,PO.PRT_ORD_STS_CD
            ,C2.CLM_REF_NO
            ,C2.CLM_THD_PTY_ID
            ,C2.CLM_OBL_NO
            ,C2.CLM_STS_CD
            ,C2.SBM_DT
            ,C2.REJ_DT
            ,C2.PD_DT
            ,C2.CLM_CLO_CD
            ,C2.LAB_REQ_AM
            ,C2.LAB_APV_AM
            ,C2.PRT_REQ_AM
            ,C2.PRT_APV_AM
            ,C2.TAX_REQ_AM
            ,C2.TAX_APV_AM
            ,C2.TOT_REQ_AM
            ,C2.TOT_APV_AM
            ,C2.CLO_DT
            ,M3.PRT_TOT_AM_IW
            ,M3.PRT_NET_AM_IW
            ,M3.LAB_TOT_AM_IW
            ,M3.LAB_NET_AM_IW
            ,M1.PRT_TOT_AM_PT
            ,M1.PRT_NET_AM_PT
            ,M1.LAB_TOT_AM_PT
            ,M1.LAB_NET_AM_PT
            ,M4.PRT_TOT_AM_SP
            ,M4.PRT_NET_AM_SP
            ,M4.LAB_TOT_AM_SP
            ,M4.LAB_NET_AM_SP
            ,M2.PRT_TOT_AM_CC
            ,M2.PRT_NET_AM_CC
            ,M2.LAB_TOT_AM_CC
            ,M2.LAB_NET_AM_CC
            --,AR.AR_MOD_DT
            --,AR.AR_EMP_ID_NO
            --,AR.AR_SO_TXT_DS
            ,D.CN_NAME_1ST
            ,D.CN_NAME_LAST
            ,D.CN_HOUSE_RTE_NO||' '||D.CN_STREET_NME||' '||D.CN_STREET_SFX ADDRESS
            ,D.CITY
            ,D.STATE
            ,D.CN_ZIP_PC
            ,D.CN_TEL_NO
            --,DATEDIFF(dd,D.SO_CRT_DT,CURRENT_DATE) DAYS_CREATE_CURRENT
        FROM D
        LEFT JOIN A
        ON TRIM(A.NPSUNITNUMBER) = TRIM(D.SVC_UN_NO) AND TRIM(A.NPSSERVICEORDER) = TRIM(D.SO_NO)
        LEFT JOIN P
        ON D.SVC_UN_NO = P.SVC_UN_NO AND D.SO_NO = P.SO_NO
        LEFT JOIN PO
        ON D.SVC_UN_NO = PO.SVC_UN_NO AND D.SO_NO = PO.SO_NO
        LEFT JOIN C2
        ON D.SVC_UN_NO = C2.SVC_UN_NO AND D.SO_NO = C2.SO_NO
        LEFT JOIN CP
        ON D.SVC_UN_NO = CP.SVC_UN_NO AND D.SO_NO = CP.SO_NO
        LEFT JOIN J
        ON D.SVC_UN_NO = J.SVC_UN_NO AND D.SO_NO = J.SO_NO
        LEFT JOIN E
        ON D.LST_ATP_TECH_ID_NO = E.EMP_ID_NO AND D.SVC_UN_NO = E.SVC_UN_NO
        LEFT JOIN M1
        ON D.SVC_UN_NO = M1.SVC_UN_NO AND D.SO_NO = M1.SO_NO
        LEFT JOIN M2
        ON D.SVC_UN_NO = M2.SVC_UN_NO AND D.SO_NO = M2.SO_NO
        LEFT JOIN M3
        ON D.SVC_UN_NO = M3.SVC_UN_NO AND D.SO_NO = M3.SO_NO
        LEFT JOIN M4
        ON D.SVC_UN_NO = M4.SVC_UN_NO AND D.SO_NO = M4.SO_NO
        LEFT JOIN N
        ON D.SVC_UN_NO = N.SVC_UN_NO AND D.SO_NO = N.SO_NO
        LEFT JOIN N4
        ON D.SVC_UN_NO = N4.SVC_UN_NO AND D.SO_NO = N4.SO_NO
        LEFT JOIN F
        ON D.SVC_UN_NO = F.SVC_UN_NO AND D.SO_NO = F.SO_NO
        LEFT JOIN FA
        ON D.SVC_UN_NO = FA.SVC_UN_NO AND D.SO_NO = FA.SO_NO
        LEFT JOIN FC
        ON D.SVC_UN_NO = FC.SVC_UN_NO AND D.SO_NO = FC.SO_NO
        LEFT JOIN DO
        ON D.SVC_UN_NO = DO.SVC_UN_NO AND D.SO_NO = DO.SO_NO
        LEFT JOIN RP
        ON D.SVC_UN_NO = RP.SVC_UN_NO AND D.SO_NO = RP.SO_NO
        LEFT JOIN RC
        ON D.SVC_UN_NO = RC.SVC_UN_NO AND D.SO_NO = RC.SO_NO
        LEFT JOIN SA
        ON D.SVC_UN_NO = SA.SVC_UN_NO AND D.SO_NO = SA.SO_NO
        LEFT JOIN SMC
        ON D.SVC_UN_NO = SMC.SVC_UN_NO AND D.SO_NO = SMC.SO_NO
        LEFT JOIN SM
        ON D.SVC_UN_NO = SM.SVC_UN_NO AND D.SO_NO = SM.SO_NO
        LEFT JOIN SS
        ON D.SVC_UN_NO = SS.SVC_UN_NO AND D.SO_NO = SS.SO_NO
        LEFT JOIN SW
        ON D.SVC_UN_NO = SW.SVC_UN_NO AND D.SO_NO = SW.SO_NO
        LEFT JOIN SG
        ON D.SVC_UN_NO = SG.SVC_UN_NO AND D.SO_NO = SG.SO_NO
        LEFT JOIN SJ
        ON D.SVC_UN_NO = SJ.SVC_UN_NO AND D.SO_NO = SJ.SO_NO
        LEFT JOIN SL
        ON D.SVC_UN_NO = SL.SVC_UN_NO AND D.SO_NO = SL.SO_NO
        LEFT JOIN SKS
        ON D.SVC_UN_NO = SKS.SVC_UN_NO AND D.SO_NO = SKS.SO_NO
        LEFT JOIN SKW
        ON D.SVC_UN_NO = SKW.SVC_UN_NO AND D.SO_NO = SKW.SO_NO
        LEFT JOIN SKG
        ON D.SVC_UN_NO = SKG.SVC_UN_NO AND D.SO_NO = SKG.SO_NO
        LEFT JOIN SKL
        ON D.SVC_UN_NO = SKL.SVC_UN_NO AND D.SO_NO = SKL.SO_NO
        LEFT JOIN SKD
        ON D.SVC_UN_NO = SKD.SVC_UN_NO AND D.SO_NO = SKD.SO_NO
        ORDER BY D.FISCAL_YR_MTH_CREATED,D.SVC_UN_NO,D.SO_NO --D.CLIENT_NM
        --ORDER BY D.SERIAL_NO,D.SO_NO,D.SVC_CUS_ID_NO,D.ITM_SUF_NO
        ;
    """

    # Run the query and save the result to a DataFrame
    df = pd.read_sql(query, conn)
    
    # Close the connection
    conn.close()
    
    # Save the DataFrame to a CSV file
    # Get the previous date
    previous_date = datetime.now() - timedelta(days=1)
    
    # Format the date as YYYY-MM-DD
    formatted_date = previous_date.strftime('%Y-%m-%d')
    
    # Create the file name with the desired convention
    csv_file = f'/tmp/cancel_report_{formatted_date}.csv'
    df.to_csv(csv_file, index=False)
    
    return csv_file

# Step 2: Send the CSV via Email
def send_email(csv_file):
    receiver_emails = ['joseph.liechty@transformco.com', 'sheila.andrews@transformco.com', 'sunday.abolaji@transformco.com', 'pedro.rodriguez@transformco.com']
    
    # Email details
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    subject = 'Daily Cancels Report - Shows Previous day data'
    body = 'Please find attached the daily cancel report.'
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT"))
    smtp_password = os.getenv("SMTP_PASSWORD")

    # Set up the MIME
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)
    msg['Subject'] = subject

    # Attach the body with the msg instance
    msg.attach(MIMEText(body, 'plain'))

    # Open the CSV file in binary mode and attach it
    with open(csv_file, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename= {csv_file}')
        msg.attach(part)

    try:
        # Create SMTP session for sending the email
        with smtplib.SMTP(smtp_server, smtp_port, local_hostname='localhost') as server:
            server.starttls()  # Enable security
            server.ehlo()  # Explicitly send EHLO command
            server.login(sender_email, smtp_password)  # Login to the SMTP server
            server.sendmail(sender_email, receiver_emails, msg.as_string())  # Send email
        print("Email sent successfully.")
    except smtplib.SMTPAuthenticationError as e:
        print(f"SMTP Authentication Error: {e}")
        print("Ensure that you are using the correct email and password.")
        print("If you have 2FA enabled, generate an app-specific password from Zoho Accounts.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    csv_file = run_query()
    send_email(csv_file)