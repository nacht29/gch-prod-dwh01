import sys
from pygcp.utils import send_email

send_email(
	subject='Alert: landlord_report_pipeline_dev failed',
	message=sys.argv[1],
	smtp_server='smtp.gmail.com',
	smtp_port=587,
	sender_email='yanzhe@cloud-space.co',
	app_password='',
	receiver_email='wilken@cloud-space.co',
	cc_emails=['amanda@cloud-space.co']
)
