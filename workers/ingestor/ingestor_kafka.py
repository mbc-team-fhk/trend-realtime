import time, hashlib, os, feedparser, json
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOT = os.getenv("KAFKA", "localhost:9092")
TOPIC = "trend-news"
p = Producer({"bootstrap.servers": BOOT})

RSS = [
  "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko",
  "https://rss.donga.com/total.xml",
  "https://www.hani.co.kr/rss/"
]

def sha1(s): return hashlib.sha1(s.encode("utf-8")).hexdigest()

def send(rec: dict):
  p.produce(TOPIC, key=rec["id"], value=json.dumps(rec, ensure_ascii=False).encode("utf-8"))

if __name__ == "__main__":
  print("[ingestor→kafka] start")
  while True:
    new = 0
    for url in RSS:
      feed = feedparser.parse(url)
      for e in feed.entries[:30]:
        rec = {
          "id": sha1(e.title + getattr(e, "link", "")),
          "source": url,
          "title": e.title,
          "url": getattr(e, "link", ""),
          "lang": "ko",
          "ts": int(datetime.now(timezone.utc).timestamp())
        }
        send(rec); new += 1
    p.flush(5.0)
    if new: print(f"produced={new}")
    time.sleep(20)