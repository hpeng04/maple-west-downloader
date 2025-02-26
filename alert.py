import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
from color import color
from log import Log

# Need to connect to UAlberta VPN or be on UAlberta network to send email.
# https://universityofalberta.freshservice.com/support/solutions/articles/19000109142

with open('email_cred.txt', 'r') as f:
    email = f.readline().strip()
    password = f.readline().strip()

def send_email(subject:str, body:str, attachment=None, from_:str = email, password:str = password) -> bool:
    
    try:
        to = []
        with open('email_list.txt', 'r') as f:
            for line in f:
                to.append(line.strip())
        
        # Create message container
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = from_
        msg['To'] = ', '.join(to)

        # Add body
        msg.attach(MIMEText(body, 'plain'))

        # Add attachment if provided
        if attachment:
            if isinstance(attachment, str) and os.path.exists(attachment):
                # If attachment is a file path
                with open(attachment, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(attachment))
            else:
                # If attachment is file-like object or bytes
                part = MIMEApplication(attachment, Name='attachment')
            
            part['Content-Disposition'] = f'attachment; filename="{part.get_param("Name")}"'
            msg.attach(part)

        # Set up SMTP server
        if 'ualberta' in from_:
            user = from_.split('@')[0]
            server = smtplib.SMTP_SSL('smtp.ualberta.ca', 465)
        else:
            user = from_
            # print(user)
            # print(password)
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(user, password)

        # Send email
        server.send_message(msg)
        server.quit()
        print(f"{color.GREEN}Email sent to {', '.join(to)}{color.END}")
        return True

    except Exception as e:
        print(f"{color.RED}Failed to send email: {str(e)}{color.END}")
        return False

def alert_failed_downloads(path):
    # Monthly alert for failed downloads by emailing failed_downloads.txt to recipients
    # then clearing the file.
    with open(path, 'r+') as f:
        subject = 'Maple West Failed Downloads'
        body = f.read()
        if body.strip() != "":
            send_email(subject, body, attachment=path)
        f.truncate(0)
    return

if __name__ == "__main__":
    send_email('Test', 'This is a test email', Log.get_path(), ['hhpeng@ualberta.ca'])