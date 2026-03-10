import requests, re
h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36'}
for url in [
    'https://moskva.cian.ru/realtors/',
    'https://moskva.cian.ru/realtors/?page=1',
    'https://www.cian.ru/realtors/?regionSlug=moskva',
    'https://moskva.cian.ru/cian-agent-search/',
    'https://www.cian.ru/cian-agent-search/',
]:
    r = requests.get(url, headers=h, timeout=10, allow_redirects=True)
    a = len(re.findall(r'/agents/\d+', r.text))
    cap = 'captcha' in r.url
    print(r.status_code, 'agents=' + str(a), 'cap=' + str(cap), r.url)
