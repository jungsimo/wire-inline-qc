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
einen LOW-Zustand synchronisiert war. Im Folgenden ist ein 
fehlerhaft segmentiertes Profil dargestellt. 
![Fehlerhaft segmentiertes Profil](pictures/Profil_detection_simple.png)

Deshalb wurde die Profilerkennung 
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

## n.i.O-Klassifizierung der Profile
Nach robuster Profilerkennung und Plausibilitätsfilterung wurden 828 
gültige Profile identifiziert. Nach einer Anlaufphase von 20 Profilen 
wurden 808 Profile gegen den laufenden Mittelwert geprüft. 
31 Profile wurden als n.i.O. klassifiziert. In allen n.i.O.-Fällen war 
ausschließlich die Länge L1 außerhalb der Toleranz von ±30 mm, 
während L2, L3 und L4 innerhalb der zulässigen Abweichung blieben. 
Daraus lässt sich schließen, dass die größte Variabilität im Datensatz 
im Bereich des Plateaus liegt. Im Folgenden ist die Toleranzabweichung 
über die Profil-IDs dargestellt.
![n.i.O über Profil-ID getrennt nach Segmenten](pictures/niO_over_ID.png)

Die n.i.O.-Klassifizierung wurde direkt in den Streaming-Service zur 
Profilerkennung integriert. Nachdem ein vollständiges Profil erkannt und 
die Segmentlängen L1 bis L4 berechnet wurden, wird das Profil zunächst auf 
Plausibilität geprüft. Nur gültige Profile werden anschließend für die 
weitere Bewertung verwendet. Für die Bewertung wird je Segmentlänge ein 
laufender Mittelwert über die bisher erkannten gültigen Profile geführt 
(5 Werte zur leichten Glättung).
Eine Anlaufphase von 20 Profilen verhindert, dass instabile Anfangswerte 
zu Fehlklassifikationen führen. Ein Profil wird als n.i.O.
eingestuft, wenn mindestens eine der vier Längen um mehr als +-30 mm vom 
erwarteten Mittelwert abweicht. In der Live-Anwendung werden dabei die 
Abweichungen für L1, L2, L3 und L4 einzeln berechnet und ausgewertet.
Alle als gültig erkannten Profile werden in das Kafka-Topic `1031103_profiles` 
geschrieben. 
Wird zusätzlich eine Abweichung erkannt, erzeugt der Service ein eigenes 
n.i.O.-Ereignis und sendet dieses in das separate Kafka-Topic:
`1031103_profiles_nio`

Das n.i.O.-Ereignis enthält unter anderem den Zeitstempel, 
die Maschinen-ID, die Profil-ID, die gemessenen Segmentlängen, 
die erwarteten Mittelwerte sowie die berechneten Abweichungen. 
Dadurch können nachgelagerte Module diese Ereignisse direkt aus 
Kafka konsumieren, ohne eng an die Profilerkennung gekoppelt zu sein.

# 5. Ergebnisse
Fügen Sie hier Logs ein, die zeigen, dass Ihr System Anomalien erkennt. 
Wie bewerten Sie die Umsetzbarkeit von Echtzeit-Analysen im Bereich 
der Inline-Kontrolle für Mubea. Welche Änderungen müssten an der 
Ihnen bekannten Architektur vorgenommen werden? 
Welche Hürden sehen Sie?

## Profilerkennung und n.i.O.-Klassifizierung
Zur Demonstration der Funktionalität wurde der Replay mit maximaler Geschwindigkeit
durchlaufen und die Topics `1031103_profiles` und `1031103_profiles_nio` ausgegeben.

![Profil Topic](pictures/test_run_Profil_detection.png)

![Profil n.i.O Topic](pictures/test_run_Profil_niO_detection.png)