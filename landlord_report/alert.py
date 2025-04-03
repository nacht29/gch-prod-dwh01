import sys
from pygcp.utils import send_email

subject = '[ALERT - WARNING] [GCH] Pipeline Failed'

message = f"""
Pipeline: landlord_report_pipeline failed to execute.
File export is incomplete.

Error message:

{sys.argv[1]}

Try:
1. Check file paths defined in the Python script
2. Check for dependencies (import xyz)
3. Check for syntax errors (missing symbols, double single quote (' '' '))

Contact Data Team for further checking.
"""

send_email(
	subject=subject,
	message=message,
	smtp_server='smtp.gmail.com',
	smtp_port=587,
	sender_email='yanzhe@cloud-space.co',
	app_password='isss lyjp jyni wfej',
	receiver_email='wilken@cloud-space.co',
	cc_emails=['amanda@cloud-space.co', 'yanzhe@cloud-space.co']
)
