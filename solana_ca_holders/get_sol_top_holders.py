import requests

url = "https://pro-api.solscan.io/v2.0/token/holders?address=6AVAUKa9uxQpruHZUinFECpXEh1usRVtzQWK8N2wpump&page=3&page_size=40"    


response = requests.get(url)

print(response.text)