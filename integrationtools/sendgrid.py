
import logging

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

class SendGridTools:
    @staticmethod
    def send_message(**kwargs):
        api_key = kwargs.get("api_key","")
        to_emails = kwargs.get("to_emails",[])
        from_email = kwargs.get("from_email","afrank+data_default@mozilla.com")
        message = kwargs.get("message","")
        subject = kwargs.get("subject","Error Alert from cloudalerts")
        message = Mail(
            from_email=from_mail,
            to_emails=to_mails,
            subject=subject,
            html_content=message)
        try:
            sg = SendGridAPIClient(api_key)
            response = sg.send(message)
            logging.info(response)
        except Exception as e:
            logging.warning(e.message)

