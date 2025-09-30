import os, json, time
from datetime import datetime, timezone
from confluent_kafka import Consumer
from redis import Redis

BOOT = os.getenv("KAFKA","localhost:9092")
TOPIC = os.getenv("TOPIC_IN","trend-result")
GROUP = os.getenv("GROUP_ID","agg-baseline")
R = Redis(host=os.getenv("REDIS_HOST","localhost"), port=int(os.getenv("REDIS_PORT","6379")), decode_responses=True)

def zkey(w): return f"trend:zset:global:{w}"
def meta(i): return f"trend:hash:item:{i}"

def main():
    c = Consumer({
        "bootstrap.servers": BOOT, "group.id": GROUP,
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
    })
    c.subscribe([TOPIC])
    print(f"[agg] start ← {TOPIC} group={GROUP}")

    while True:
        m = c.poll(1.0)
        if not m: continue
        if m.error(): print("[agg] error:", m.error()); continue
        f = json.loads(m.value().decode("utf-8"))
        item = f["id"]; sc = 1.0
        pipe = R.pipeline()
        pipe.hset(meta(item), mapping={
            "title": f.get("title_normalized",""), "sentiment": f.get("sentiment","neu"),
            "topic_id": f.get("topic_id","t_misc"), "url": f.get("url",""),
            "last_seen": datetime.now(timezone.utc).isoformat()
        })
        pipe.zincrby(zkey("5m"), sc, item)
        pipe.execute()
        c.commit(m)
        print(f"[agg] z+ {item} score={sc}")

if __name__ == "__main__":
    main()