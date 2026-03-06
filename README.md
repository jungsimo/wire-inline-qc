# wire-inline-qc — README (Windows / PowerShell)

Diese README ist ausschließlich dafür da, das Projekt lokal **zum Laufen zu bekommen** (Kafka + Services + Visualisierung).

---

## Voraussetzungen
- **Docker Desktop**
- **Python >= 3.9**
- Für `viz-plot`: lokaler Desktop/GUI (Matplotlib-Fenster)

---

## Quickstart

### 1) Kafka starten
Im Repo-Root:

```powershell
docker compose up -d
```

### 2) Python Umgebung einrichten
```powershell
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
.\venv\Scripts\python.exe -m pip install -e .
```

### 3) Kafka Topics anlegen
Damit Consumer nicht an “Unknown topic or partition” hängen 
und Visualisierung sofort reagiert:

```powershell
.\venv\Scripts\python.exe -m tools.create_topics
```

### 4) Services starten (jeweils in separaten Terminals)
Reihenfolge ist grundsätzlich egal. 
Für Visualisierung mit `--offset latest` am besten Plot vor Replay starten

#### Replay Producer (simuliert Datenstrom)
```powershell
.\venv\Scripts\python.exe -m wireqc replay
```

#### Teil 1: Korrelation (Output: `1031103_corr`)
```powershell
.\venv\Scripts\python.exe -m wireqc corr
```

#### Teil 2: Six-Sigma Alarmierung (Output: `1031103_801`)
```powershell
.\venv\Scripts\python.exe -m wireqc alarms
```

#### Teil 3: Profilerkennung + n.i.O. (Outputs: `1031103_profiles`, `1031103_profiles_nio)`
```powershell
.\venv\Scripts\python.exe -m wireqc profiles
```

### 5) Visualisierung

### 5.1) Counter (profiles, n.i.O.)
```powershell
.\venv\Scripts\python.exe -m wireqc viz
```

### 5.2) Live-Plots (Durchmesser, Härte- und Anlasstemperatur über Drahtlänge)
Live ab Zeitpunkt "jetzt":

```powershell
.\venv\Scripts\python.exe -m wireqc viz-plot --offset latest --group wireqc-viz-plot-demo
```

Alles ab Anfang (liest Backlog):

```powershell
.\venv\Scripts\python.exe -m wireqc viz-plot --offset earliest --group wireqc-viz-plot-all
```

#### Debug / Topic Tail
Ein Topic live mitlesen (wartet automatisch, bis Topic verfügbar ist):

Beispiele:
```powershell
.\venv\Scripts\python.exe -m tools.tail_topic --topic 1031103_801
.\venv\Scripts\python.exe -m tools.tail_topic --topic 1031103_profiles
.\venv\Scripts\python.exe -m tools.tail_topic --topic 1031103_profiles_nio
```

#### CLI Übersicht 
Alles Subcommands anzeigen:

```powershell
.\venv\Scripts\python.exe -m wireqc -h
```

Vefügbare Topics:
- `create-topics`
- `replay`
- `corr`
- `alarms`
- `profiles`
- `viz`
- `viz-plot`