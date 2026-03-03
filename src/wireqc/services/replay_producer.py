import time
from wireqc.common.config import load_config
from wireqc.io.offline_loader import load_process_json
from wireqc.io.kafka.producer import KafkaJsonProducer

def main():
    cfg = load_config()
    bs = cfg["kafka"]["bootstrap_servers"]
    topic = cfg["topics"]["raw"]

    # --- Replay Input (.gz) ---
    df_raw = load_process_json("data/process_data.json.gz")

    # deterministisch: nach Time sortieren (wie realer Stream)
    df_raw = df_raw.sort_values("Time")

    producer = KafkaJsonProducer(bs)

    sent = 0
    t0 = time.time()

    # Tipp: iterrows ist ok bei 864k; für speed könnte man itertuples nehmen.
    for _, row in df_raw.iterrows():
        msg = row.to_dict()

        # Key: MachineId, damit Partitionierung stabil ist
        key = str(msg.get("MachineId", ""))

        producer.produce(topic=topic, value=msg, key=key)
        sent += 1

        # Flush in Batches (wichtig, sonst wächst Buffer)
        if sent % 5000 == 0:
            producer.flush(2.0)
            dt = time.time() - t0
            print(f"sent={sent}  rate={sent/dt:.1f} msg/s")

        # Wenn du wirklich 10Hz "Echtzeit" simulieren willst, entkommentieren:
        # time.sleep(0.1)

    producer.flush(10.0)
    dt = time.time() - t0
    print(f"DONE. sent={sent}  avg_rate={sent/dt:.1f} msg/s")

if __name__ == "__main__":
    main()