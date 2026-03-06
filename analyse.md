Lösung: Inline Prozesskontrolle
Name: Simon Jung  Matrikelnummer: 30400203

# 1. Vorgehensweise und Architektur
Beschreiben Sie hier kurz wie Sie vorgegangen sind und Ihre gewählte Lösung (verwendete Bibliotheken, Struktur der Services).


# 2. Korrelationsanalyse
Begründen Sie die Wahl Ihres Zeitfensters. Was waren die Herausforderungen bei der Berechnung im Stream?


# 3. Schwellwerterkennung (Six Sigma)
Wie haben Sie die "Running Statistics" (Mittelwert/StdDev) implementiert? Wie gehen Sie mit der Initialisierung um?


# 4. Profilerkennung
Beschreiben Sie Ihren Algorithmus zur Segmentierung der Daten. Wie erkennen Sie "Knees" (Start/Ende von Plateaus)?

Zur Erkennung der Drahtprofile wurde ein zustandsbasierter Algorithmus 
(Finite State Machine) verwendet. Ein vollständiges Profil besteht aus 
den vier Segmenten LOW, RISE, HIGH und FALL und wird erst dann als erkannt 
gezählt, wenn die Folge LOW -> RISE -> HIGH -> FALL -> LOW vollständig 
durchlaufen wurde. Dadurch können die Segmentlängen L1 bis L4 eindeutig 
aus den Drahtlängen an den Zustandswechseln berechnet werden.

Die Erkennung der Knees (Beginn und Ende der Plateaus bzw. Übergänge)
erfolgt über Schwellwerte mit Hysterese auf dem geglätteten Durchmessersignal.
Verwendet wurden die Grenzen `low_entry`, `low_exit`, `high_entry` 
und `high_exit`. Der Übergang von LOW nach RISE wird nur dann zugelassen, 
wenn zuvor ein stabiler LOW-Bereich erkannt wurde und zusätzlich 
eine positive lokale Steigung vorliegt. Dadurch wird verhindert, 
dass ein Profilstart mitten in einem Übergang fälschlich erkannt wird.

Ausgangspunkt war eine einfache FSM mit festen Schwellwerten auf dem Rohsignal.
Die Offline-Analyse zeigte jedoch, dass diese Variante an Start- und 
Randbereichen fehleranfällig war. Es traten unplausible Segmentierungen auf, 
etwa `L2 = 0 mm`. Ursache war, dass der Stream an 
beliebiger Stelle beginnen kann und die FSM dadurch nicht immer korrekt auf 
einen LOW-Zustand synchronisiert war. Deshalb wurde die Profilerkennung 
erweitert um:
- eine Synchronisationsphase (UNSYNCED)
- eine Glättung des Durchmessersignals
- eine Steigungsbedingung für den Start des Anstiegs
- einen Plausibilitätsfilter für erkannte Profile

Die Offline-Analyse ergab für die plausiblen Profile sehr stabile Werte:
- L1 ca. 5723 mm
- L2 ca. 962 mm
- L3 ca. 226 mm
- L4 ca 229 mm
- Gesamtprofil ca. 7140 mm
- Dauer ca. 101 s
- ca. 1012 Rohwerte pro Profil

Damit erwies sich der FSM-Ansatz als gut geeignet für die Streaming-Aufgabe. 
Die größte Herausforderung lag nicht in der Rechenzeit, sondern in der 
robusten Initialisierung und Synchronisation des Zustands.

# 5. Ergebnisse
Fügen Sie hier Logs ein, die zeigen, dass Ihr System Anomalien erkennt. Wie bewerten Sie die Umsetzbarkeit von Echtzeit-Analysen im Bereich der Inline-Kontrolle für Mubea. Welche Änderungen müssten an der Ihnen bekannten Architektur vorgenommen werden? Welche Hürden sehen Sie?