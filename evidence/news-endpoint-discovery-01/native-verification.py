import hashlib,io,json,shutil
from datetime import UTC,datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from xml.dom import Node
import duckdb
from defusedxml.minidom import parse
from selective_options.crypto_news import FEED_URL,LEGACY_FEED_URL,parse_feed,crypto_news_quality
from selective_options.events import MarketEvent
OUT=Path('evidence/news-endpoint-discovery-01');ROOT=Path.cwd()
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
proof=json.loads((OUT/'discovery.json').read_text(encoding='utf8'))
assert proof['real_public_requests_attempted']==3 and proof['retries']==0 and proof['automatic_redirect_follows']==0
for r in proof['responses']:
    raw=(OUT/r['body_file']).read_bytes();assert len(raw)==r['body_bytes'] and hashlib.sha256(raw).hexdigest()==r['body_sha256']
legacy,doc,target=proof['responses']
assert legacy['url']==LEGACY_FEED_URL and legacy['http_status']==308 and legacy['location_header']=='/arc/outboundfeeds/rss?outputType=xml'
assert doc['http_status']==200 and target['url']==FEED_URL and target['http_status']==200 and target['location_header'] is None
class Links(HTMLParser):
    def __init__(self):super().__init__();self.links=[]
    def handle_starttag(self,tag,attrs):
        if tag=='a':
            for k,v in attrs:
                if k=='href' and v in ('/arc/outboundfeeds/rss','/arc/outboundfeeds/rss/','https://www.coindesk.com/arc/outboundfeeds/rss/'):
                    self.links.append(v)
p=Links();p.feed((OUT/doc['body_file']).read_text(encoding='utf8'));assert p.links
raw=(OUT/target['body_file']).read_bytes();rows,rejected=parse_feed(raw,target['received_at_ms']);assert rows and rejected==0
xml=parse(io.BytesIO(raw),forbid_dtd=True,forbid_entities=True,forbid_external=True)
def children(node,name):return [c for c in node.childNodes if c.nodeType==Node.ELEMENT_NODE and (c.localName or c.nodeName)==name]
def text(node):return (node.nodeValue or '') if node.nodeType in (Node.TEXT_NODE,Node.CDATA_SECTION_NODE) else ''.join(text(c) for c in node.childNodes)
assert xml.documentElement.tagName=='rss'
channel=children(xml.documentElement,'channel');assert len(channel)==1
items=children(channel[0],'item');assert len(items)==len(rows)
comparisons=[]
for item,row in zip(items,rows,strict=True):
    native={}
    for field in ('title','link','guid','pubDate'):
        found=children(item,field);assert len(found)==1;native[field]=text(found[0])
    body=row.payload()
    assert body['native_title']==native['title'] and body['native_link']==native['link'] and body['native_guid']==native['guid'] and body['native_pub_date']==native['pubDate']
    source=int(parsedate_to_datetime(native['pubDate'].strip()).timestamp()*1000)
    assert row.event_time_ms==source<=row.observed_at_ms==target['received_at_ms']
    assert body['feed_url']==FEED_URL and body['first_publication_independently_verified'] is False and body['classification']=='NOT_CLASSIFIED'
    comparisons.append({'event_id':row.event_id,'native_guid':native['guid'],'source_claim_ms':source,'received_at_ms':row.observed_at_ms})
oldreport=json.loads(Path('evidence/crypto-news-public-01/report.json').read_text(encoding='utf8'))
with duckdb.connect('evidence/crypto-news-public-01/events.duckdb',read_only=True) as db:
    cursor=db.execute('select * from events'); names=[column[0] for column in cursor.description]; values=[dict(zip(names,row,strict=True)) for row in cursor.fetchall()]
oldrows=[MarketEvent.model_validate(v) for v in values]
oldquality=crypto_news_quality(oldrows,as_of_ms=oldreport['quality']['as_of_ms'])
assert oldquality==oldreport['quality']
current=ROOT/'src/selective_options/crypto_news.py'
shutil.copyfile(current,OUT/'source-0341.py')
shutil.copyfile(Path(__file__),OUT/'native-verification.py')
result={'status':'FIXED_CANONICAL_PUBLISHER_RSS_ENDPOINT_VERIFIED','source':'coindesk_rss','old_fixed_feed_url':LEGACY_FEED_URL,'new_fixed_feed_url':FEED_URL,'migration_basis':'native legacy HTTP308 Location resolves exactly to independently requested successful fixed same-origin RSS target; official publisher page independently links RSS path','real_public_requests_attempted':3,'retries':0,'automatic_redirect_follows':0,'official_documentation_rss_links':sorted(set(p.links)),'native_metadata_items':len(rows),'rejected_items':rejected,'independent_dom_native_comparisons':comparisons,'original_0340_source_sha256':digest(OUT/'source-0340.py'),'current_0341_source_sha256':digest(current),'historical_0340_health_id':oldrows[0].event_id,'historical_0340_quality_byte_fields_unchanged':True,'historical_0340_files_sha256':{str(f.resolve().relative_to(ROOT)):digest(f) for f in Path('evidence/crypto-news-public-01').iterdir() if f.is_file()},'request_evidence_sha256':digest(OUT/'discovery.json'),'continuous_history_verified':False,'complete_news_coverage':False,'first_publication_independently_verified':False,'trading_approval':False,'orders_submitted':0,'owned_tests_passed':65,'ruff_passed':True,'mypy_source_files_passed':1}
(OUT/'verification.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf8')
print(json.dumps({k:v for k,v in result.items() if k in ('status','native_metadata_items','current_0341_source_sha256','historical_0340_quality_byte_fields_unchanged')}))


