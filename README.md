# LinkCheck

Ein Tool zum Überprüfen und Bereinigen von URLs in JSON-Lines Dateien.

## Installation

```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

## Verwendung

Das Tool wird über die Kommandozeile gesteuert und verarbeitet JSON-Lines Dateien (eine JSON-Objekt pro Zeile):

```bash
python linkcheck.py input.jsonl feldname1 feldname2 [feldname3 ...] [optionen]
```

### Parameter

- `input_file`: Pfad zur JSON-Lines Eingabedatei (ein JSON-Objekt pro Zeile)
- `fields`: Eine oder mehrere Feldnamen, die auf URLs überprüft werden sollen
- `--suffix`: Optional: Suffix für die Ausgabedatei (Standard: "_cleaned")
- `--chunk-size`: Optional: Größe der zu verarbeitenden Chunks (Standard: 1000)
- `-v, --verbose`: Optional: Aktiviert ausführliche Ausgaben zur Verarbeitung
- `--timeout`: Optional: Timeout in Sekunden für URL-Überprüfungen (Standard: 10.0)
- `--timeout-file`: Optional: Datei zum Speichern von URLs, die ein Timeout verursacht haben
- `--keep-timeout`: Optional: URLs bei Timeout behalten statt zu löschen

### Beispiele

Standard-Modus:
```bash
python linkcheck.py daten.jsonl url_feld beschreibungs_url --suffix _bereinigt
```

Ausführlicher Modus mit detaillierten Ausgaben:
```bash
python linkcheck.py daten.jsonl url_feld beschreibungs_url -v
```

Mit angepasstem Timeout und Timeout-Logging:
```bash
python linkcheck.py daten.jsonl url_feld --timeout 5.0 --timeout-file timeouts.txt
```

Timeout-URLs behalten und loggen:
```bash
python linkcheck.py daten.jsonl url_feld --timeout 5.0 --timeout-file timeouts.txt --keep-timeout
```

## Funktionsweise

1. Das Tool liest die JSON-Lines Datei zeilenweise ein
2. Verarbeitet die Daten in Chunks für optimale Performance
3. Prüft die angegebenen Felder auf gültige URLs
4. Überprüft die Erreichbarkeit der URLs:
   - Status 200: URL wird beibehalten
   - Status 301: Neue URL wird verwendet
   - Status 404: Feld wird gelöscht
   - Timeout: Feld wird gelöscht (oder behalten mit --keep-timeout) und URL wird optional in Timeout-Datei geschrieben
   - Ungültige URL: Feld wird gelöscht
5. Speichert das Ergebnis zeilenweise in der Ausgabedatei

Die Verarbeitung erfolgt parallel für optimale Leistung bei großen Dateien. Fehlerhafte JSON-Lines werden übersprungen und geloggt.

### Verbose-Modus (-v)

Im Verbose-Modus gibt das Tool detaillierte Informationen aus:
- Start und Ende der Verarbeitung
- Einlesen der Datei und Chunk-Informationen
- Details zu jeder URL-Überprüfung
- Status-Codes und Weiterleitungen
- Timeout-Ereignisse und betroffene URLs
- Information ob Timeout-URLs behalten oder gelöscht werden
- Änderungen an den Feldern
- Fortschritt der Verarbeitung
