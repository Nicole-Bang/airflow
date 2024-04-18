import requests
# client_id, authorize_code 노출 주의, 실제 값은 임시로만 넣고 Git에 올라가지 않도록 유의
# 값 넣고 깃에 올리지 말고 상단의 ▷을 눌러 실행하여 토큰 발급받기

client_id = '{client_id}'
redirect_uri = 'https://example.com/oauth'
authorize_code = '{authorize_code}'


token_url = 'https://kauth.kakao.com/oauth/token'
data = {
    'grant_type': 'authorization_code',
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'code': authorize_code,
    }

response = requests.post(token_url, data=data)
tokens = response.json()
print(tokens)