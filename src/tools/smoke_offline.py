from wireqc.io.offline_loader import load_process_json
from wireqc.io.processdata_parser import parse_raw_message

def main():
    df_raw = load_process_json("data/process_data.json.gz")
    print("Loaded:", df_raw.shape)
    print("Columns:", list(df_raw.columns))

    first = df_raw.iloc[0].to_dict()
    rec = parse_raw_message(first)
    print("Parsed record:", rec)

if __name__ == "__main__":
    main()