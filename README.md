# Wire Inline QC – Test-README

Dieses README beschreibt die wichtigsten Schritte, um die Lösung lokal zu 
starten und die zentralen Funktionen zu testen.

Das Lösungstemplate mit ausführlicher Beschreibung der Vorgehensweise ist
in `analyse.md` zu finden

## Voraussetzungen

- Python 3.9
- laufender lokaler Kafka-Broker
- virtuelle Umgebung vorhanden unter `./venv`
- Projektwurzel ist das Verzeichnis mit `src/`, `config/`, `data/`

## Wichtige Topics

Die Anwendung verwendet mehrere Kafka-Topics:

- `raw` – Rohdatenstrom aus dem Replay
- `corr` – Korrelationswerte Geschwindigkeit vs. Härtetemperatur
- `alarms` – Six-Sigma-Alarmereignisse
- `profiles` – gültig erkannte Profile
- `profiles_nio` – n.i.O.-Profile

## Python-Umgebung einrichten

Falls noch keine virtuelle Umgebung vorhanden ist:

```powershell
py -3.9 -m venv venv
```

Projekt über `pyproject.toml` installieren:

```powershell
pip install -e .
```

## Kafka / Docker starten
```powershell
docker compose up -d
```

## Datensatz vorbereiten

Falls der Datensatz im Verzeichnis 
`data/` **nur als `.gz`-Datei** vorliegt, muss er vor dem Replay entpackt werden.

```powershell
.\venv\Scripts\python.exe src\tools\decompress_data.py
```

Danach sollte die Datei `data/process_data.json` vorhanden sein.

## Topics anlegen

```powershell
.\venv\Scripts\python.exe -m wireqc create-topics
```

## Test 1: Profilerkennung + Visualisierung + n.i.O.-Topic

### Terminal 1 – Profilerkennung starten

```powershell
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
.\venv\Scripts\python.exe -m wireqc profiles --offset latest --group ("wireqc-profiles-" + $ts)
```

### Terminal 2 – Visualisierung starten

```powershell
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
.\venv\Scripts\python.exe -m wireqc viz-plot --offset latest --group ("wireqc-viz-" + $ts) --update-hz 5 --downsample 1 --points 800 --x-window-mm 5000 --xlim-update-every 3
```

### Terminal 3 – n.i.O.-Topic mitlesen

```powershell
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
.\venv\Scripts\python.exe src/tools/tail_topic.py --topic 1031103_profiles_nio --group ("wireqc-tail-nio-" + $ts) --offset latest
```

### Terminal 4 – Replay leicht beschleunigt starten

```powershell
.\venv\Scripts\python.exe -m wireqc replay --sleep 0.02 --max-messages 50000
```

Hinweis:
- `--sleep 0.1` entspricht ungefähr Echtzeit bei 10 Hz
- `--sleep 0.02` ist ungefähr 5x schneller als Echtzeit
- `--sleep 0.01` ist ungefähr 10x schneller als Echtzeit

## Test 2: Six-Sigma-Alarmierung

### Terminal 1 – Alarm-Service starten

```powershell
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
.\venv\Scripts\python.exe -m wireqc alarms --offset latest --group ("wireqc-alarms-" + $ts) --window-s 300 --sample-hz 10 --min-n 3000 --persist-n 20
```

### Terminal 2 – Alarm-Topic mitlesen

```powershell
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
.\venv\Scripts\python.exe src/tools/tail_topic.py --topic 1031103_801 --group ("wireqc-tail-alarms-" + $ts) --offset latest
```

### Terminal 3 – Replay starten

```powershell
.\venv\Scripts\python.exe -m wireqc replay --sleep 0.02 --max-messages 50000
```

## Test 3: Korrelationsanalyse

### Terminal 1 – Korrelationsservice starten

```powershell
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
.\venv\Scripts\python.exe -m wireqc corr --offset latest --group ("wireqc-corr-" + $ts) --window-samples 1000 --emit-every-s 10 --min-samples 100 --min-speed-std 0.2
```

### Terminal 2 – Korrelationstopic mitlesen

```powershell
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
.\venv\Scripts\python.exe src/tools/tail_topic.py --topic 1031103_corr --group ("wireqc-tail-corr-" + $ts) --offset latest
```

### Terminal 3 – Replay starten

```powershell
.\venv\Scripts\python.exe -m wireqc replay --sleep 0.02 --max-messages 50000
```

## Projektstruktur (vereinfacht)

- `replay` – simuliert den Rohdatenstrom
- `corr` – Korrelationsservice
- `alarms` – Six-Sigma-Alarmservice
- `profiles` – Profilerkennung + n.i.O.-Klassifizierung
- `viz-plot` – Live-Visualisierung
- `tail-topic` – Mitlesen einzelner Kafka-Topics
