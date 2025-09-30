import json, re, os
from confluent_kafka import Consumer, Producer

BOOT = os.getenv("KAFKA","localhost:9092")
IN_TOPICS = ["trend-news","trend-sns"]
OUT_TOPIC = "trend-result"

c = Consumer({"bootstrap.servers": BOOT, "group.id":"filter-nlp", "auto.offset.reset":"latest"})
p = Producer({"bootstrap.servers": BOOT})
c.subscribe(IN_TOPICS)

re_html, re_space = re.compile(r"<[^>]+>"), re.compile(r"\s+")
STOP = {"단독","속보","영상","사진"}

POS = {w.lower() for w in ["상승","호재","인상","확대","돌파","급증"]}
NEG = {w.lower() for w in ["하락","악재","감소","축소","급락","하한"]}
TOPICS = [
  ("t_stock", {w.lower() for w in ["증시","주가","코스닥","코스피"]}),
  ("t_ai",    {w.lower() for w in ["AI","인공지능","GPT","오픈AI","챗GPT","생성형"]}),
  ("t_kakao", {w.lower() for w in ["카카오","톡","카톡"]}),
]

def norm_title(t):
  t = re_html.sub("", t).replace("\u200b","")
  return re_space.sub(" ", t).strip()

def tokenize_ko(t):
  t = re.sub(r"[^0-9A-Za-z가-힣 ]"," ", t)
  toks = [w for w in t.split() if len(w)>=2 and w not in STOP]
  return toks[:20]

def sentiment(tokens):
  p = sum(1 for t in tokens if t.lower() in POS)
  n = sum(1 for t in tokens if t.lower() in NEG)
  return "pos" if p-n>0 else ("neg" if p-n<0 else "neu")

def topic_id(tokens):
  s = {w.lower() for w in tokens}
  for tid, keys in TOPICS:
    if s & keys: return tid
  return "t_misc"

print("[filter+nlp] trend-news/sns → trend-result")
while True:
  msg = c.poll(1.0)
  if not msg:
    continue
  if msg.error():
    continue
  try:
    rec = json.loads(msg.value().decode("utf-8"))
    title = norm_title(rec["title"])
    tokens = tokenize_ko(title)
    if not tokens:
      c.commit(msg);
      continue

    out = {
      "id": rec["id"], "title_normalized": title, "url": rec.get("url",""),
      "tokens": tokens, "sentiment": sentiment(tokens), "topic_id": topic_id(tokens),
      "ts": rec.get("ts",0), "source": rec.get("source",""), "lang": rec.get("lang","ko")
    }
    p.produce(OUT_TOPIC, key=out["id"], value=json.dumps(out, ensure_ascii=False).encode("utf-8"))
    c.commit(msg)
  except Exception:
    c.commit(msg)
  p.poll(0)