import json

from twilio.rest import Client

with open("twilio_config.json", "r") as f:
    twilio_config = json.load(f)

client = Client(twilio_config["account_sid"], twilio_config["auth_token"])


def send_sms(text="Hello world"):
    message = client.messages.create(
        to=twilio_config["to"], from_=twilio_config["from_"], body=text
    )
    print(message.sid)
