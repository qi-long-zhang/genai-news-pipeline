from gradio_client import Client

client = Client("zhang-qilong/ModernBERT-News")

payload = [
    "Headline: \nSubhead: \nLead: ",
    "Headline: Nanyang Optical undergoes voluntary liquidation after 65 years, customer's S$1,800 order stuck in limbo\nSubhead: The company also provided the customer with a number to call but her calls went unanswered.\nLead: A long-time customer of Nanyang Optical has found her S$1,800 contact lens order stuck in limbo, after the company informed customers that it is undergoing voluntary liquidation.",
]

result = client.predict(
    payload=payload,
    api_name="/predict",
)
print(result)
