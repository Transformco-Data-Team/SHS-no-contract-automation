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
        
select * from  

        (select svc_cus_id_no,  

                itm_suf_no, 

                agr_suf_no,  

                SVC_UN_NO, 

                SO_NO, 

                svc_prd_pln_cd, 

                ap_pchs_date, 

                ap_curr_st_date, 

                ap_curr_exp_date, 

                ori_thd_pty_id, 

                CMB_THD_PTY_ID, 

                mds_cd,  

                CALL_TYPE, ori_cd, 

                SO_CRT_DT, so_crt_un_no, so_crt_emp_no, 

                SO_STS_CD, 

                SO_STS_DT, 

                NPS_SP_CD, 

                MODEL_NO, 

                SERIAL_NO, 

                MFG_BND_NM, 

                svc_sch_dt, 

                ProductContractID, product_contract_start_date,  

                product_contract_current_end_date, 

                npj_customer_key, min(agreement_suffix) 

              --  c.start_date, min(c.current_expire_date) 

       from 

       (select svc_cus_id_no,  

                itm_suf_no, 

                agr_suf_no,  

                SVC_UN_NO, 

                SO_NO, 

                svc_prd_pln_cd, 

                ap_pchs_date, 

                ap_curr_st_date, 

                ap_curr_exp_date, 

                ori_thd_pty_id, 

                CMB_THD_PTY_ID, 

                mds_cd,  

                CALL_TYPE, ori_cd, 

                SO_CRT_DT, so_crt_un_no, so_crt_emp_no, 

                SO_STS_CD, 

                SO_STS_DT, 

                NPS_SP_CD, 

                MODEL_NO, 

                SERIAL_NO, 

                MFG_BND_NM, 

                svc_sch_dt, 

                service_contract_id as ProductContractID, product_contract_start_date,  

                product_contract_current_end_date 

    from        

            (SElECT  

                svc_cus_id_no,  

                itm_suf_no, 

                agr_suf_no,  

                SVC_UN_NO, 

                SO_NO, 

                svc_prd_pln_cd, 

                ap_pchs_date, 

                ap_curr_st_date, 

                ap_curr_exp_date, 

                ori_thd_pty_id, 

                CMB_THD_PTY_ID, 

                mds_cd,  

                CALL_TYPE, ori_cd,  

                SO_CRT_DT, so_crt_un_no, so_crt_emp_no, 

                SO_STS_CD, 

                SO_STS_DT, 

                NPS_SP_CD, 

                PSV_ITM_MDL_NO AS MODEL_NO, 

                PSV_MDS_SRL_NO AS SERIAL_NO, 

                MFG_BND_NM, 

                svc_sch_dt 

            FROM PRD_DB2.HS_PERM_TBLS.IH_SERVICEORDER_FULLDETAIL_TBL T  

            where UPPER(CALL_TYPE) LIKE '%ASSURANT%' 

                AND SO_CRT_DT = DATEADD(DAY, -1, CURRENT_DATE) 

                AND SO_CRT_DT < CURRENT_DATE ) as file1 

                left join 

                "PRD_HSAGREEMENT"."AGREEMENT"."SERVICE_CONTRACT_PRODUCTS" a 

                on npj_customer_key = svc_cus_id_no 

                  and item_suffix = itm_suf_no 

                  and so_crt_dt >= product_contract_start_date 

                  and so_crt_dt <= product_contract_current_end_date) as file2 

                left join 

               "PRD_HSAGREEMENT"."AGREEMENT"."SERVICE_CONTRACT" c 

               on svc_cus_id_no = npj_customer_key 

               and so_crt_dt between start_date and current_expire_date 

               and ProductContractID is null 

               group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28) as file3 

            where ProductContractid is null 

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

    receiver_emails = ['sunday.abolaji@transformco.com', 'joseph.liechty@transformco.com', 'katrina.means@transformco.com', 'victor.smith@transformco.com', 'erwin.estrada@transformco.com', 'kelly.nance@transformco.com']
    
    # Email details
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    subject = '[AUTOMATED] ASSURANT-PA-Service Order with no Contracts'
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
