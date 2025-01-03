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
        WITH
            SO_DATA AS (
            SElECT 
                SVC_UN_NO||'-'||SO_NO AS UID,
                SVC_UN_NO,
                SO_NO,
                SERVICEPROVIDER,
                SVC_TYP_CD,
                NAI_SVC_LOC,
                CMB_THD_PTY_ID,
                CALL_TYPE,
                SO_CRT_DT,CN_ZIP_PC
                SO_STS_CD,
                SO_STS_DT,
                NPS_SP_CD,
                PSV_ITM_MDL_NO AS MODEL_NO,
                PSV_MDS_SRL_NO AS SERIAL_NO,
                MFG_BND_NM,
                MDS_CD,
                SVC_CUS_ID_NO AS CUST_ID,
                GEO.CTY_NM,
                GEO.DISTRICT_NAME,
                GEO.PLANNING_AREA_NAME,
                GEO.TIER,
                GEO.STATEFULLNAME,
                T.SVC_SCH_DT AS SERVICE_SCHEDULE_DATE
            FROM 
                PRD_DB2.HS_PERM_TBLS.IH_SERVICEORDER_FULLDETAIL_TBL T
            LEFT JOIN
                IH_DATASCIENCE.HS_PRICING.GEO_HIERARCHY geo
            ON
                T.CN_ZIP_PC = geo.zip_code
            WHERE
                1 = 1
                AND UPPER(CALL_TYPE) LIKE '%ASSURANT%'
                -- AND SO_NO = '11253883'
                AND SO_CRT_DT >= DATEADD(MONTH, -3, CURRENT_DATE)
                AND SO_CRT_DT < CURRENT_DATE
            ),
        
            DISTINCT_CONTRACT AS (
            SELECT 
                DISTINCT 
                C.NPJ_CUSTOMER_KEY AS CUST_ID,
                C.SERVICE_CONTRACT_ID,
                C.NPJ_CUSTOMER_KEY||'-'||C.SERVICE_CONTRACT_ID AS KEY_
            FROM 
                SC_ANALYTICS.SC_DATA.PA_CONTRACTS_UPDATED C
            WHERE 
                1 = 1
                AND UPPER(C.CONTRACT_STATUS) != 'CANCELLED'
            ),
        
            CONTRACT_DATA AS (
            SELECT 
                C.NPJ_CUSTOMER_KEY||'-'||C.SERVICE_CONTRACT_ID AS KEY_,
                C.SERVICE_CONTRACT_ID,
                C.SERIAL_NUMBER,
                C.NPJ_CUSTOMER_KEY AS CUST_ID,
                C.CURRENT_ITEMS_COVERED,
                C.OBLIGOR,
                C.CONTRACT_STATUS,
                C.SERVICE_PRODUCT_PLAN_CODE,
                C.SERVICE_PRODUCT_CODE,
                C.PURCHASE_DATE,
                P.PRODUCT_CONTRACT_START_DATE AS ORIGINAL_START_DATE,
                P.PRODUCT_CONTRACT_CURRENT_END_DATE AS CURRENT_END_DATE,
                P.PRODUCT_CONTRACT_ORIGINAL_END_DATE AS ORIGINAL_END_DATE
            FROM 
                SC_ANALYTICS.SC_DATA.PA_CONTRACTS_UPDATED C 
            LEFT JOIN
                SC_ANALYTICS.SC_DATA.PA_CONTRACTS_PRODUCTS_UPDATED P
            ON
                C.NPJ_CUSTOMER_KEY = P.NPJ_CUSTOMER_KEY
                AND C.SERVICE_CONTRACT_ID = P.SERVICE_CONTRACT_ID
            WHERE 
                1 = 1
            ),
        
            CONTRACT_CUSTOMER AS (
            SELECT
                NPJ_CUSTOMER_KEY||'-'||SERVICE_CONTRACT_ID AS KEY_,
                NPJ_CUSTOMER_KEY AS CUST_ID,
                SERVICE_CONTRACT_ID,
                MODEL_NUMBER,
                PSV_MDS_CD_DS
            FROM 
                SC_ANALYTICS.SC_DATA.CSHS 
            -- WHERE
                -- SERVICE_CONTRACT_ID = '67966306900001'
                -- NPJ_CUSTOMER_KEY = '679663069'
            ),
        
            CONTRACT_AGG AS (
            SELECT
                DC.CUST_ID,
                DC.SERVICE_CONTRACT_ID,
                -- CD.SERVICE_CONTRACT_ID,
                CD.SERIAL_NUMBER,
                -- CD.CUST_ID,
                CD.CURRENT_ITEMS_COVERED,
                CD.OBLIGOR,
                CD.CONTRACT_STATUS,
                CD.SERVICE_PRODUCT_PLAN_CODE,
                CD.SERVICE_PRODUCT_CODE,
                CD.PURCHASE_DATE,
                CD.ORIGINAL_START_DATE,
                CD.CURRENT_END_DATE,
                CD.ORIGINAL_END_DATE,
                CC.MODEL_NUMBER,
                CC.PSV_MDS_CD_DS
            FROM
                DISTINCT_CONTRACT DC
            LEFT JOIN
                CONTRACT_DATA CD
            ON
                DC.KEY_ = CD.KEY_
            LEFT JOIN
                CONTRACT_CUSTOMER CC
            ON
                DC.KEY_ = CC.KEY_
            QUALIFY ROW_NUMBER() OVER (PARTITION BY DC.KEY_, CC.MODEL_NUMBER ORDER BY CD.PURCHASE_DATE) = 1
            )
        
        SELECT
            S.UID,
            S.SVC_UN_NO,
            S.SO_NO,
            S.SERVICEPROVIDER,
            S.SVC_TYP_CD,
            S.NAI_SVC_LOC,
            S.CMB_THD_PTY_ID,
            S.CALL_TYPE,
            S.SO_CRT_DT,
            S.SO_STS_CD,
            S.SO_STS_DT,
            S.NPS_SP_CD,
            S.MODEL_NO,
            C.MODEL_NUMBER,
            S.SERIAL_NO,
            S.MFG_BND_NM,
            S.MDS_CD,
            S.CUST_ID,
            C.SERVICE_CONTRACT_ID,
            C.SERIAL_NUMBER,
            C.CURRENT_ITEMS_COVERED,
            C.OBLIGOR,
            C.CONTRACT_STATUS,
            C.SERVICE_PRODUCT_PLAN_CODE,
            C.SERVICE_PRODUCT_CODE,
            C.PURCHASE_DATE,
            C.ORIGINAL_START_DATE,
            C.CURRENT_END_DATE,
            C.ORIGINAL_END_DATE,
            CASE
                WHEN C.CUST_ID IS NOT NULL THEN 1
                ELSE 0
            END AS HAS_CONTRACT,
            S.CTY_NM,
            S.DISTRICT_NAME,
            S.PLANNING_AREA_NAME,
            S.TIER,
            S.STATEFULLNAME,
            C.PSV_MDS_CD_DS,
            S.SERVICE_SCHEDULE_DATE
        FROM
            SO_DATA S
        LEFT JOIN
            CONTRACT_AGG C
        ON
            S.CUST_ID = C.CUST_ID
            AND S.SO_CRT_DT BETWEEN C.ORIGINAL_START_DATE AND C.CURRENT_END_DATE
            AND (REPLACE(S.MODEL_NO, '.', '') LIKE CONCAT('%', C.MODEL_NUMBER, '%') OR C.MODEL_NUMBER LIKE CONCAT('%', S.MODEL_NO, '%'))
        WHERE 
            S.SO_CRT_DT = DATEADD(DAY, -1, CURRENT_DATE)
            AND C.CUST_ID IS NULL
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
    csv_file = f'/tmp/assurant_pa_no_contract_{formatted_date}.csv'
    df.to_csv(csv_file, index=False)
    
    return csv_file

# Step 2: Send the CSV via Email
def send_email(csv_file):
    # Get the previous date
    previous_date = datetime.now() - timedelta(days=1)
    
    # Format the date as YYYY-MM-DD
    formatted_date = previous_date.strftime('%Y-%m-%d')

    receiver_emails = ['sunday.abolaji@transformco.com', 'joseph.liechty@transformco.com', 'katrina.means@transformco.com', 'victor.smith@transformco.com', 'erwin.estrada@transformco.com']
    
    # Email details
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    subject = 'FROM DAMI: ASSURANT-PA-Service Order with no Contracts'
    body = f'''ASSURANT-PA: Service Order with no Contracts for {formatted_date}
    Please find attached the list of Service Orders with no Contracts for {formatted_date}. 
    This report is generated daily and sent to the relevant stakeholders. It contains  the list of Service order created in the previous day with no contracts.

    Please reach out to sunday.damilare@transformco.com to include recipients or for any questions or concerns.
    '''
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
