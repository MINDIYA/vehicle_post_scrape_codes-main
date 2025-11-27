import requests
# Define FlareSolverr URL
url = "http://localhost:8191/v1"
headers = {"Content-Type": "application/json"}
# Define request parameters
data = {
"cmd": "request.get",
"url": "https://riyasewana.com/search/cars/toyota",
"maxTimeout": 60000
}
# Send request to FlareSolverr
response = requests.post(url, headers=headers, json=data)
# Print response details
print("Status:", response.json().get('status', {}))
print("Status Code:", response.status_code)
print("FlareSolverr Message:", response.json().get('message', {}))
# Extract page content
page_content = response.json().get('solution', {}).get('response', '')
print(page_content)