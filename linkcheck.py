#!/usr/bin/env python3

"""
LinkCheck - Ein Werkzeug zur Überprüfung und Bereinigung von URLs in JSON-Lines Dateien

Dieses Programm wurde entwickelt, um große Mengen von URLs in JSON-Dateien zu überprüfen
und zu bereinigen. Es ist besonders nützlich für:
- Datenmigration: Wenn alte Datenbestände auf Aktualität geprüft werden müssen
- Qualitätssicherung: Um tote Links in Datenbanken zu finden und zu entfernen
- Aktualisierung: Um veraltete URLs auf ihre neuen Ziele umzuleiten

Das Programm verarbeitet die Daten in Chunks (Teilstücken), um auch sehr große Dateien
effizient verarbeiten zu können. Dabei werden mehrere URLs parallel geprüft, um Zeit
zu sparen.
"""

import json
import asyncio  # Für parallele Verarbeitung
import aiohttp  # Für effiziente HTTP-Anfragen
import urllib.parse
import logging
from typing import Dict, List, Any, Optional, Iterator, Set, DefaultDict
from pathlib import Path
import sys
import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@dataclass
class FieldStats:
    """
    Sammelt Statistiken für ein einzelnes Feld in den JSON-Daten.

    Diese Klasse hilft dabei, Probleme zu erkennen, z.B.:
    - Ob bestimmte Domains nicht erreichbar sind (möglicherweise Server-Probleme)
    - Wie viele URLs ungültig sind (mögliche Datenqualitätsprobleme)
    - Wie viele Weiterleitungen es gibt (Hinweis auf veraltete URLs)
    """

    total_urls: int = 0  # Gesamtzahl der geprüften URLs
    valid_urls: int = 0  # Anzahl gültiger URLs
    invalid_urls: int = 0  # Anzahl ungültiger URLs
    redirects: int = 0  # Anzahl der Weiterleitungen
    not_found: int = 0  # Anzahl 404-Fehler
    timeouts: int = 0  # Anzahl Timeout-Fehler
    errors: int = 0  # Anzahl sonstiger Fehler
    domains: DefaultDict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )  # Zählt URLs pro Domain


@dataclass
class Statistics:
    """
    Zentrale Statistik-Sammlung für alle verarbeiteten Felder.

    Diese Klasse ist wichtig für:
    - Qualitätskontrolle: Erkennen von Mustern in fehlerhaften URLs
    - Problemdiagnose: Identifizieren von nicht erreichbaren Servern
    - Fortschrittskontrolle: Überblick über die Gesamtverarbeitung
    """

    field_stats: DefaultDict[str, FieldStats] = field(
        default_factory=lambda: defaultdict(FieldStats)
    )

    def add_url_check(
        self,
        field: str,
        url: str,
        is_valid: bool,
        new_url: Optional[str],
        is_timeout: bool,
        status_code: Optional[int] = None,
    ):
        """
        Fügt das Ergebnis einer URL-Überprüfung zur Statistik hinzu.

        Diese Funktion kategorisiert die Ergebnisse, um später Muster
        in problematischen URLs erkennen zu können.
        """
        stats = self.field_stats[field]
        stats.total_urls += 1

        # Wir sammeln Statistiken pro Domain, um Server-Probleme erkennen zu können
        domain = urlparse(url).netloc
        stats.domains[domain] += 1

        if is_timeout:
            stats.timeouts += 1
        elif not is_valid:
            if status_code == 404:
                stats.not_found += 1
            else:
                stats.invalid_urls += 1
        elif new_url and new_url != url:
            stats.redirects += 1
            stats.valid_urls += 1
        else:
            stats.valid_urls += 1

    def print_summary(self, verbose: bool = False):
        """
        Gibt eine übersichtliche Zusammenfassung der gesammelten Statistiken aus.

        Diese Übersicht hilft bei der Entscheidung, ob:
        - Die Verarbeitung erfolgreich war
        - Bestimmte Server Probleme haben
        - Eine erneute Prüfung notwendig ist
        """
        logger.info("\nVerarbeitungsstatistik:")
        logger.info("=====================")

        for field_name, stats in sorted(self.field_stats.items()):
            logger.info(f"\nFeld: {field_name}")
            logger.info(f"  Gesamt URLs geprüft: {stats.total_urls}")
            logger.info(f"  Gültige URLs: {stats.valid_urls}")
            if stats.redirects > 0:
                logger.info(f"  Weiterleitungen: {stats.redirects}")
            if stats.invalid_urls > 0:
                logger.info(f"  Ungültige URLs: {stats.invalid_urls}")
            if stats.not_found > 0:
                logger.info(f"  Nicht gefunden (404): {stats.not_found}")
            if stats.timeouts > 0:
                logger.info(f"  Timeouts: {stats.timeouts}")
            if stats.errors > 0:
                logger.info(f"  Fehler: {stats.errors}")

            # Im ausführlichen Modus zeigen wir auch die häufigsten Domains
            if verbose and stats.domains:
                logger.info("\n  Top Domains:")
                sorted_domains = sorted(
                    stats.domains.items(), key=lambda x: (-x[1], x[0])
                )[:5]
                for domain, count in sorted_domains:
                    logger.info(f"    {domain}: {count} URLs")

        # Gesamtübersicht für schnelle Einschätzung der Verarbeitung
        logger.info("\nGesamtstatistik:")
        total_urls = sum(s.total_urls for s in self.field_stats.values())
        total_valid = sum(s.valid_urls for s in self.field_stats.values())
        total_timeouts = sum(s.timeouts for s in self.field_stats.values())
        total_404 = sum(s.not_found for s in self.field_stats.values())

        logger.info(f"  Geprüfte URLs: {total_urls}")
        logger.info(f"  Erfolgreiche Prüfungen: {total_valid}")
        if total_timeouts > 0:
            logger.info(f"  Timeouts: {total_timeouts}")
        if total_404 > 0:
            logger.info(f"  404 Fehler: {total_404}")


def setup_logging(verbose: bool):
    """
    Richtet die Protokollierung ein.

    Zwei Modi werden unterstützt:
    - Normal: Nur wichtige Informationen werden angezeigt
    - Ausführlich: Detaillierte Informationen für die Fehlersuche
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = (
        "%(asctime)s - %(levelname)s - %(message)s" if verbose else "%(message)s"
    )
    logging.basicConfig(level=log_level, format=log_format)


def is_valid_url(url: str) -> bool:
    """
    Prüft, ob eine URL syntaktisch korrekt ist.

    Dies ist der erste Schritt der Validierung, bevor wir versuchen,
    die URL tatsächlich aufzurufen. So sparen wir Zeit bei offensichtlich
    ungültigen URLs.
    """
    try:
        result = urllib.parse.urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


async def check_url(
    session: aiohttp.ClientSession, url: str, verbose: bool, timeout: float
) -> tuple[bool, Optional[str], bool, Optional[int]]:
    """
    Überprüft eine URL auf Erreichbarkeit.

    Diese Funktion ist das Herzstück der URL-Prüfung. Sie:
    - Versucht die URL aufzurufen
    - Erkennt Weiterleitungen (301, 302)
    - Behandelt Timeouts
    - Identifiziert verschiedene Fehlerzustände

    Returns: (is_valid, new_url, is_timeout, status_code)
    - is_valid: Gibt an, ob die URL gültig und erreichbar ist
    - new_url: Bei Weiterleitung die neue URL
    - is_timeout: Ob ein Timeout aufgetreten ist
    - status_code: Der HTTP-Status-Code für detailliertere Fehleranalyse
    """
    try:
        if verbose:
            logger.debug(f"Prüfe URL: {url}")
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        async with session.get(
            url, allow_redirects=False, timeout=timeout_obj
        ) as response:
            status = response.status
            if status == 200:
                if verbose:
                    logger.debug(f"URL {url} ist erreichbar (Status 200)")
                return True, url, False, status
            elif status in (301, 302):
                new_location = response.headers.get("Location")
                if new_location:
                    if verbose:
                        logger.debug(
                            f"URL {url} wurde zu {new_location} weitergeleitet (Status {status})"
                        )
                    return True, new_location, False, status
                if verbose:
                    logger.debug(
                        f"URL {url} hat Status {status}, aber keine neue Location"
                    )
                return (
                    True,
                    url,
                    False,
                    status,
                )  # Weiterleitung ohne Ziel gilt trotzdem als gültig
            elif status == 404:
                if verbose:
                    logger.debug(f"URL {url} ist nicht erreichbar (Status 404)")
                return False, None, False, status
            else:
                if verbose:
                    logger.debug(f"URL {url} hat unerwarteten Status {status}")
                return False, None, False, status
    except asyncio.TimeoutError:
        if verbose:
            logger.debug(f"Timeout bei URL {url}")
        return False, None, True, None
    except Exception as e:
        logger.error(f"Fehler beim Prüfen der URL {url}: {str(e)}")
        return False, None, False, None


def read_jsonl_chunks(
    file_path: Path, chunk_size: int, verbose: bool
) -> Iterator[List[Dict]]:
    """
    Liest die JSON-Lines Datei in Chunks (Teilstücken).

    Warum in Chunks?
    - Speichereffizienz: Nicht die ganze Datei muss auf einmal geladen werden
    - Fortschrittskontrolle: Regelmäßige Statusmeldungen sind möglich
    - Fehlertoleranz: Ein Fehler betrifft nur den aktuellen Chunk
    """
    if verbose:
        logger.debug(f"Beginne Einlesen der Datei {file_path}")
    with open(file_path, "r") as f:
        chunk = []
        for line_num, line in enumerate(f, 1):
            try:
                obj = json.loads(line.strip())
                chunk.append(obj)
                if len(chunk) >= chunk_size:
                    if verbose:
                        logger.debug(
                            f"Chunk mit {len(chunk)} Objekten geladen (bis Zeile {line_num})"
                        )
                    yield chunk
                    chunk = []
            except json.JSONDecodeError as e:
                logger.error(f"Fehler beim Parsen der JSON-Line {line_num}: {str(e)}")
                continue
        if chunk:
            if verbose:
                logger.debug(f"Letzter Chunk mit {len(chunk)} Objekten geladen")
            yield chunk


async def process_chunk(
    chunk: List[Dict[str, Any]],
    fields: List[str],
    verbose: bool,
    timeout: float,
    timeout_urls: Set[str],
    keep_timeout_urls: bool,
    stats: Statistics,
    follow_redirects: bool,
) -> List[Dict[str, Any]]:
    """
    Verarbeitet einen Chunk von JSON-Objekten parallel.

    Diese Funktion ist für die eigentliche Verarbeitung zuständig:
    - Prüft URLs in den angegebenen Feldern
    - Aktualisiert oder löscht URLs basierend auf den Ergebnissen
    - Sammelt Statistiken für die Auswertung
    - Behandelt Timeout-Fälle nach Benutzereinstellung
    - Folgt Weiterleitungen wenn gewünscht (--follow-redirects)
    """
    processed_chunk = []
    if verbose:
        logger.debug(f"Verarbeite Chunk mit {len(chunk)} Objekten")
    async with aiohttp.ClientSession() as session:
        for item_num, item in enumerate(chunk, 1):
            processed_item = item.copy()
            if verbose:
                logger.debug(f"Verarbeite Objekt {item_num}/{len(chunk)}")
            for field in fields:
                if field in processed_item:
                    value = processed_item[field]
                    if isinstance(value, str) and is_valid_url(value):
                        if verbose:
                            logger.debug(f"Prüfe Feld '{field}' mit URL: {value}")
                        is_valid, new_url, is_timeout, status_code = await check_url(
                            session, value, verbose, timeout
                        )
                        stats.add_url_check(
                            field, value, is_valid, new_url, is_timeout, status_code
                        )

                        if is_timeout:
                            timeout_urls.add(value)
                            if verbose:
                                logger.debug(f"URL {value} zum Timeout-Log hinzugefügt")
                                if keep_timeout_urls:
                                    logger.debug(
                                        f"URL {value} wird trotz Timeout behalten"
                                    )
                                else:
                                    logger.debug(
                                        f"URL {value} wird wegen Timeout gelöscht"
                                    )
                            if not keep_timeout_urls:
                                del processed_item[field]
                        elif not is_valid:
                            if verbose:
                                logger.debug(
                                    f"Lösche ungültiges Feld '{field}' mit URL: {value}"
                                )
                            del processed_item[field]
                        elif new_url != value and follow_redirects:
                            if verbose:
                                logger.debug(
                                    f"Aktualisiere URL in Feld '{field}' von {value} zu {new_url}"
                                )
                            processed_item[field] = new_url
                    else:
                        if verbose and isinstance(value, str):
                            logger.debug(
                                f"Lösche Feld '{field}' mit ungültiger URL: {value}"
                            )
                        del processed_item[field]
            processed_chunk.append(processed_item)
    return processed_chunk


async def process_json_file(
    input_file: Path,
    output_file: Path,
    fields: List[str],
    chunk_size: int = 1000,
    verbose: bool = False,
    timeout: float = 10.0,
    timeout_file: Optional[Path] = None,
    keep_timeout_urls: bool = False,
    follow_redirects: bool = False,
):
    """
    Hauptfunktion zur Verarbeitung der JSON-Lines Datei.

    Diese Funktion koordiniert den gesamten Verarbeitungsprozess:
    - Liest die Eingabedatei
    - Steuert die Chunk-Verarbeitung
    - Sammelt Timeout-URLs
    - Schreibt die Ergebnisse
    - Erstellt die Statistik

    Die Verarbeitung erfolgt in Chunks, um auch sehr große Dateien
    effizient verarbeiten zu können. Fehler in einzelnen URLs oder
    Chunks beeinflussen nicht die Gesamtverarbeitung.
    """
    try:
        if verbose:
            logger.debug(f"Starte Verarbeitung von {input_file}")
            logger.debug(f"Zu prüfende Felder: {', '.join(fields)}")
            logger.debug(f"Chunk-Größe: {chunk_size}")
            logger.debug(f"Timeout: {timeout} Sekunden")
            logger.debug(
                f"Timeout-URLs werden {'behalten' if keep_timeout_urls else 'gelöscht'}"
            )
            logger.debug(
                f"Weiterleitungen werden {'verfolgt' if follow_redirects else 'beibehalten'}"
            )
            if timeout_file:
                logger.debug(f"Timeout-URLs werden in {timeout_file} gespeichert")

        timeout_urls: Set[str] = set()
        stats = Statistics()

        with open(output_file, "w") as out_f:
            chunks_processed = 0
            total_items = 0
            for chunk in read_jsonl_chunks(input_file, chunk_size, verbose):
                chunks_processed += 1
                total_items += len(chunk)
                processed_chunk = await process_chunk(
                    chunk,
                    fields,
                    verbose,
                    timeout,
                    timeout_urls,
                    keep_timeout_urls,
                    stats,
                    follow_redirects,
                )
                for item in processed_chunk:
                    out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                logger.info(
                    f"Chunk {chunks_processed} mit {len(chunk)} Einträgen verarbeitet (Gesamt: {total_items})"
                )

        # Timeout-URLs in separate Datei schreiben, falls gewünscht
        if timeout_file and timeout_urls:
            with open(timeout_file, "w") as tf:
                for url in sorted(timeout_urls):
                    tf.write(f"{url}\n")
            if verbose:
                logger.debug(
                    f"{len(timeout_urls)} Timeout-URLs in {timeout_file} gespeichert"
                )

        # Statistik ausgeben
        stats.print_summary(verbose)

    except Exception as e:
        logger.error(f"Fehler bei der Verarbeitung: {str(e)}")
        sys.exit(1)


def main():
    """
    Haupteinstiegspunkt des Programms.

    Hier werden:
    - Kommandozeilenargumente verarbeitet
    - Grundlegende Prüfungen durchgeführt
    - Die Verarbeitung gestartet

    Das Programm ist so gestaltet, dass es sowohl für kleine als auch
    für sehr große Dateien effizient arbeitet und dabei möglichst
    benutzerfreundlich bleibt.
    """
    parser = argparse.ArgumentParser(
        description="Prüft und bereinigt URLs in JSON-Lines Dateien"
    )
    parser.add_argument("input_file", type=str, help="Eingabe JSON-Lines Datei")
    parser.add_argument("fields", nargs="+", help="Zu prüfende Felder")
    parser.add_argument(
        "--suffix", type=str, default="_cleaned", help="Suffix für die Ausgabedatei"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Größe der zu verarbeitenden Chunks",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Aktiviert ausführliche Ausgaben"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Timeout in Sekunden für URL-Überprüfungen (Standard: 10.0)",
    )
    parser.add_argument(
        "--timeout-file",
        type=str,
        help="Datei zum Speichern von URLs, die ein Timeout verursacht haben",
    )
    parser.add_argument(
        "--keep-timeout",
        action="store_true",
        help="URLs bei Timeout behalten statt zu löschen",
    )
    parser.add_argument(
        "--follow-redirects",
        action="store_true",
        help="Bei Weiterleitungen (301/302) die neue URL übernehmen",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    input_path = Path(args.input_file)
    if not input_path.exists():
        logger.error(f"Eingabedatei {input_path} existiert nicht.")
        sys.exit(1)

    output_path = (
        input_path.parent / f"{input_path.stem}{args.suffix}{input_path.suffix}"
    )

    timeout_path = Path(args.timeout_file) if args.timeout_file else None

    if args.verbose:
        logger.debug("Programmstart")
        logger.debug(f"Eingabedatei: {input_path}")
        logger.debug(f"Ausgabedatei: {output_path}")
        if timeout_path:
            logger.debug(f"Timeout-Datei: {timeout_path}")

    asyncio.run(
        process_json_file(
            input_path,
            output_path,
            args.fields,
            args.chunk_size,
            args.verbose,
            args.timeout,
            timeout_path,
            args.keep_timeout,
            args.follow_redirects,
        )
    )

    if args.verbose:
        logger.debug("Programmende")
    logger.info(
        f"Verarbeitung abgeschlossen. Ergebnis wurde in {output_path} gespeichert."
    )


if __name__ == "__main__":
    main()
