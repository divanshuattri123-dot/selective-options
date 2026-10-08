import asyncio, aiohttp, hashlib, json, shutil, time
from pathlib import Path
from urllib.parse import urlsplit
ROOT=Path.cwd()
OUT=ROOT/'evidence/news-endpoint-discovery-01'
OLD='https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml'
DOC='https://www.coindesk.com/coindesk-news/2021/09/17/coindesk-rss'
def safe(url):
    p=urlsplit(url)
    return p.scheme=='https' and p.hostname in ('www.coindesk.com','coindesk.com') and p.username is None and p.password is None and p.port in (None,443) and not p.fragment
async def main():
    if OUT.exists(): raise FileExistsError(OUT)
    OUT.mkdir()
    source=ROOT/'src/selective_options/crypto_news.py'
    shutil.copyfile(source,OUT/'source-0340.py')
    rows=[]
    async with aiohttp.ClientSession(headers={'User-Agent':'SelectiveOptionsResearch/0.3 (finite RSS endpoint discovery)'}) as session:
        async def one(url,label):
            started=time.time_ns()//1000000
            row={'url':url,'label':label,'started_at_ms':started,'redirects_allowed':False,'tls_verification':'default','timeout_s':15}
            try:
                async with session.get(url,allow_redirects=False,timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    row['http_status']=resp.status
                    row['location_header']=resp.headers.get('Location')
                    row['content_type_header']=resp.headers.get('Content-Type')
                    body=bytearray()
                    async for chunk in resp.content.iter_chunked(65536):
                        body.extend(chunk)
                        if len(body)>2097152: raise ValueError('BYTE_BUDGET')
                    row['received_at_ms']=time.time_ns()//1000000
                    row['body_file']=label+'.entity'
                    (OUT/row['body_file']).write_bytes(body)
                    row['body_bytes']=len(body)
                    row['body_sha256']=hashlib.sha256(body).hexdigest()
            except (aiohttp.ClientError,TimeoutError,ValueError) as exc:
                row['error_type']=type(exc).__name__
                row['failure_at_ms']=time.time_ns()//1000000
            rows.append(row)
            (OUT/'discovery.json').write_text(json.dumps({'request_budget':3,'real_public_requests_attempted':len(rows),'retries':0,'automatic_redirect_follows':0,'source_0340_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'responses':rows},indent=2)+'\n',encoding='utf8')
            print(json.dumps(row))
            return row
        first=await one(OLD,'legacy')
        await one(DOC,'publisher-doc')
        loc=first.get('location_header')
        if first.get('http_status') in (301,302,303,307,308) and isinstance(loc,str) and safe(loc) and loc != OLD:
            await one(loc,'publisher-location-target')
    shutil.copyfile(Path(__file__),OUT/'discovery-verifier.py')
asyncio.run(main())
