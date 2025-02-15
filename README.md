# JsonLinkCheck

Ein Tool zum Überprüfen und Bereinigen von URLs in JSON-Lines Dateien.

## Installation

Sie können das Tool auf zwei Arten installieren:

### Mit uv (empfohlen)

Zuerst müssen Sie uv installieren:

#### Linux/MacOS
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### Windows (PowerShell)
```powershell
(Invoke-WebRequest -Uri "https://astral.sh/uv/install.ps1" -UseBasicParsing).Content | pwsh -Command -
```

#### Alternativ mit pip
```bash
pip install uv
```

Nach der Installation von uv können Sie JsonLinkCheck installieren:
```bash
uv tool install slubjsonlinkcheck
```

### Mit pip

```bash
pip install slubjsonlinkcheck
```

## Verwendung

Das Tool wird über die Kommandozeile gesteuert und verarbeitet JSON-Lines Dateien (eine JSON-Objekt pro Zeile):

```bash
jsonlinkcheck input.jsonl feldname1 feldname2 [feldname3 ...] [optionen]
```

### Parameter

- `input_file`: Pfad zur JSON-Lines Eingabedatei (ein JSON-Objekt pro Zeile)
- `fields`: Eine oder mehrere Feldnamen, die auf URLs überprüft werden sollen
- `--suffix`: Optional: Suffix für die Ausgabedatei (Standard: "_cleaned")
- `--chunk-size`: Optional: Größe der zu verarbeitenden Chunks (Standard: 1000)
- `-v, --verbose`: Optional: Aktiviert ausführliche Ausgaben zur Verarbeitung
- `--timeout`: Optional: Timeout in Sekunden für URL-Überprüfungen (Standard: 10.0)
- `--timeout-file`: Optional: Datei zum Speichern von URLs, die ein Timeout verursacht haben
- `--delete-timeouts`: Optional: URLs bei Timeout löschen (Standard: URLs werden behalten)
- `--follow-redirects`: Optional: Bei Weiterleitungen (301/302) die neue URL übernehmen
- `--redirects-file`: Optional: Datei zum Speichern von Weiterleitungen im Format 'quelle;ziel'
- `--visual`: Optional: Zeigt eine visuelle Fortschrittsanzeige statt Logging-Ausgaben
- `--threads`: Optional: Anzahl der parallel arbeitenden Threads (Standard: 1)

### Beispiele

Standard-Modus (behält Timeout-URLs):
```bash
jsonlinkcheck daten.jsonl url_feld beschreibungs_url --suffix _bereinigt
```

Timeout-URLs löschen:
```bash
jsonlinkcheck daten.jsonl url_feld --delete-timeouts
```

Weiterleitungen verfolgen und in Datei speichern:
```bash
jsonlinkcheck daten.jsonl url_feld --follow-redirects --redirects-file redirects.txt
```

Ausführlicher Modus mit detaillierten Ausgaben:
```bash
jsonlinkcheck daten.jsonl url_feld beschreibungs_url -v
```

Mit angepasstem Timeout und Timeout-Logging:
```bash
jsonlinkcheck daten.jsonl url_feld --timeout 5.0 --timeout-file timeouts.txt
```

Parallele Verarbeitung mit mehreren Threads:
```bash
jsonlinkcheck daten.jsonl url_feld --threads 5 --visual
```

## Entwicklung

### Setup der Entwicklungsumgebung

```bash
# Repository klonen
git clone https://github.com/thomsbe/slubjsonlinkcheck.git
cd slubjsonlinkcheck

# Python 3.12 oder höher wird benötigt
python --version

# Virtuelle Umgebung erstellen und aktivieren
uv venv
source .venv/bin/activate

# Entwicklungsabhängigkeiten installieren
uv pip install -e .
```

## Funktionsweise

1. Das Tool liest die JSON-Lines Datei zeilenweise ein
2. Verarbeitet die Daten in Chunks für optimale Performance
3. Verteilt die Chunks auf die angegebene Anzahl von Threads
4. Prüft die angegebenen Felder auf gültige URLs:
   - Einzelne URL-Strings werden direkt geprüft
   - Arrays von URLs werden Element für Element geprüft
5. Überprüft die Erreichbarkeit der URLs:
   - Status 200: URL wird beibehalten
   - Status 301/302: URL wird beibehalten oder aktualisiert (mit --follow-redirects)
   - Status 404: URL wird gelöscht (bei Arrays: aus dem Array entfernt)
   - Timeout: URL wird gelöscht oder behalten (mit --keep-timeout) und optional in Timeout-Datei geschrieben
   - Ungültige URL: URL wird gelöscht (bei Arrays: aus dem Array entfernt)
6. Speichert das Ergebnis:
   - Einzelne URLs: Feld wird gelöscht wenn URL ungültig
   - Arrays: Leere Arrays werden komplett gelöscht, sonst bleiben gültige URLs erhalten

Die Verarbeitung erfolgt parallel für optimale Leistung bei großen Dateien. Fehlerhafte JSON-Lines werden übersprungen und geloggt.

### Verbose-Modus (-v)

Im Verbose-Modus gibt das Tool detaillierte Informationen aus:
- Start und Ende der Verarbeitung
- Einlesen der Datei und Chunk-Informationen
- Details zu jeder URL-Überprüfung (einzeln oder im Array)
- Status-Codes und Weiterleitungen
- Timeout-Ereignisse und betroffene URLs
- Information ob Timeout-URLs behalten oder gelöscht werden
- Änderungen an den Feldern und Arrays
- Fortschritt der Verarbeitung

### Visueller Modus (--visual)

Im visuellen Modus zeigt das Tool Fortschrittsbalken:
- Gesamtfortschritt: Zeigt die Verarbeitung aller Zeilen der Eingabedatei
- Thread-Fortschritte: Zeigt die Verarbeitung der URLs in jedem aktiven Thread

Der visuelle Modus deaktiviert die normalen Logging-Ausgaben für eine übersichtlichere Darstellung.

### Parallele Verarbeitung (--threads)

Die parallele Verarbeitung mit mehreren Threads beschleunigt die Verarbeitung großer Dateien:
- Jeder Thread verarbeitet einen eigenen Chunk von URLs
- Die Ergebnisse werden in temporäre Dateien geschrieben
- Am Ende werden alle Teilergebnisse zusammengeführt
- Die Fortschrittsanzeige zeigt den Status jedes Threads separat

---

# English Quick Start Guide

JsonLinkCheck is a tool for checking and cleaning URLs in JSON Lines files.

## Installation

```bash
# Using uv (recommended)
curl -LsSf https://astral.sh/uv/install.sh | sh  # Linux/MacOS
uv tool install slubjsonlinkcheck

# Or using pip
pip install slubjsonlinkcheck
```

## Basic Usage

```bash
# Check URLs in specific fields
jsonlinkcheck input.jsonl url_field [other_fields...]

# Common options
--delete-timeouts     # Delete URLs that timeout (default: keep them)
--follow-redirects    # Follow and update redirected URLs
--redirects-file FILE # Save redirects to file (source;target format)
--timeout-file FILE   # Save timeout URLs to file
--threads N          # Use N parallel threads
--visual             # Show progress bars
```

### Examples

```bash
# Basic usage (keeps timeout URLs)
jsonlinkcheck data.jsonl url_field

# Delete timeout URLs and follow redirects
jsonlinkcheck data.jsonl url_field --delete-timeouts --follow-redirects

# Process with 4 threads and track redirects
jsonlinkcheck data.jsonl url_field --threads 4 --redirects-file redirects.txt --visual
```

For detailed documentation in German, see above.
