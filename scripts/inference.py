from gradio_client import Client

client = Client("zhang-qilong/ModernBERT-News")

payload = [
    {
        "text": "Former US Vice President Dick Cheney dies at 84",
        "text_pair": "He served as Vice-President to 43rd US President George W. Bush.",
    },
    {
        "text": "Man dies on SIA flight bound for Milan, wife tears & thanks doctor for trying to save husband",
        "text_pair": "Three doctors and almost all the cabin crew attempted CPR.",
    },
    {
        "text": "S'porean girl, 2, dons kebaya to become youngest & cutest SIA stewardess for a day",
        "text_pair": "Interns are preparing from such a young age for jobs these days.",
    },
]

result = client.predict(
    payload=payload,
    api_name="/predict",
)
print(result)
