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
        SELECT * FROM PRD_NFDT.NFDT_METRIC_TBLS.TBL_ARCHIVESOATTEMPTDETAILS LIMIT 1;
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
    receiver_emails = ['sunday.abolaji@transformco.com']
    
    # Email details
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    subject = 'auto test'
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