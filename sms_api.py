import json

from twilio.rest import Client

from pathlib import Path

twilio_config_path = Path(__file__).parent / "twilio_config.json"

if twilio_config_path.exists():
    with open(twilio_config_path, "r") as f:
        twilio_config = json.load(f)
    client = Client(twilio_config["account_sid"], twilio_config["auth_token"])
else:
    client = False


def send_sms(text="Hello world"):
    if client:
        message = client.messages.create(
            to=twilio_config["to"], from_=twilio_config["from_"], body=text
        )
        print(message.sid)
    else:
        print("Cannot send sms: no sms configuration.")
