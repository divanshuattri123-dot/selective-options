import asyncio,aiohttp,hashlib,json,time
from pathlib import Path
from urllib.parse import urljoin,urlsplit
OUT=Path('evidence/news-endpoint-discovery-01')
async def main():
    proof=json.loads((OUT/'discovery.json').read_text(encoding='utf8'))
    assert proof['real_public_requests_attempted']==2
    old=proof['responses'][0]
    target=urljoin(old['url'],old['location_header'])
    p=urlsplit(target)
    assert p.scheme=='https' and p.netloc=='www.coindesk.com' and p.path=='/arc/outboundfeeds/rss' and p.query=='outputType=xml' and not p.fragment
    row={'url':target,'label':'publisher-location-target','started_at_ms':time.time_ns()//1000000,'redirects_allowed':False,'tls_verification':'default','timeout_s':15,'target_basis':'explicit same-origin resolution of saved native legacy Location header; independent direct request'}
    async with aiohttp.ClientSession(headers={'User-Agent':'SelectiveOptionsResearch/0.3 (finite RSS endpoint discovery)'}) as session:
        try:
            async with session.get(target,allow_redirects=False,timeout=aiohttp.ClientTimeout(total=15)) as resp:
                row['http_status']=resp.status; row['location_header']=resp.headers.get('Location'); row['content_type_header']=resp.headers.get('Content-Type')
                body=bytearray()
                async for chunk in resp.content.iter_chunked(65536):
                    body.extend(chunk)
                    if len(body)>2097152: raise ValueError('BYTE_BUDGET')
                row['received_at_ms']=time.time_ns()//1000000; row['body_file']='publisher-location-target.entity'
                (OUT/row['body_file']).write_bytes(body); row['body_bytes']=len(body); row['body_sha256']=hashlib.sha256(body).hexdigest()
        except (aiohttp.ClientError,TimeoutError,ValueError) as exc:
            row['error_type']=type(exc).__name__; row['failure_at_ms']=time.time_ns()//1000000
    proof['responses'].append(row); proof['real_public_requests_attempted']=3
    proof['relative_location_resolution']='standard URL resolution on fixed HTTPS www.coindesk.com origin; validated exact RSS path/query before one independent direct request'
    (OUT/'discovery.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf8')
    (OUT/'relative-location-verifier.py').write_bytes(Path(__file__).read_bytes())
    print(json.dumps(row))
asyncio.run(main())
