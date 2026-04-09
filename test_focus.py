import urllib.request, json

url = ("https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
       "ExpectativasMercadoAnuais"
       "?%24filter=Indicador%20eq%20%27IPCA%27"
       "&%24orderby=Data%20desc&%24top=20&%24format=json")

req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
with urllib.request.urlopen(req, timeout=10) as resp:
    raw = json.loads(resp.read())

records = raw.get("value", [])
print("Todos os campos do primeiro registro:")
print(json.dumps(records[0], indent=2, ensure_ascii=False))
print()
print("Registros DataReferencia=2026:")
for r in records:
    if r["DataReferencia"] == "2026":
        print(json.dumps(r, indent=2, ensure_ascii=False))
