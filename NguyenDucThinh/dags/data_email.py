# import json
# import pandas as pd
# import numpy as np
# import ssl, os
# import csv
# from datetime import datetime, timedelta
# from airflow.models import Variable
# from urllib.request import Request, urlopen
# import base64
# from sendgrid import SendGridAPIClient
# from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
# from datetime import datetime
# ssl._create_default_https_context = ssl._create_default_https_context

# def email():
#     out_csv_file_path = "/opt/airflow/data/clean_data/2024/5/12/nguyenducthinh_filtered.csv"
#     recipients = ['phamthixuanhien@iuh.edu.vn']
#     message = Mail(
#         from_email='ducthinh140202@gmail.com',
#         to_emails=recipients,
#         subject='Nguyễn Đức Thịnh - Crawl data FPT Jobs',
#         html_content='Nguyễn Đức Thịnh'
#     )
#     with open(out_csv_file_path,'rb') as f:
#         data = f.read()
#     encoded_file = base64.b64encode(data).decode()
    
#     attachedFile = Attachment(
#         FileContent(encoded_file),
#         FileName('nguyenducthinh_filtered.csv'),
#         FileType('text/csv'),
#         Disposition('attachment')
#     )
#     message.attachment = attachedFile
    
#     try:
#         sg = SendGridAPIClient("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")
#         response = sg.send(message)
#         print(response.status_code)
#         print(response.body)
#         print(response.headers)
#         print(datetime.now())
#     except Exception as e:
#         print(str(e))
        
#     return True
