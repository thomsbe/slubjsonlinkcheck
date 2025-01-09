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
from typing import (
    Dict,
    List,
    Any,
    Optional,
    Set,
    DefaultDict,
    Tuple,
    AsyncIterator,
)
from pathlib import Path
import sys
import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from urllib.parse import urlparse
from tqdm import tqdm
import aiofiles

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


def count_lines(file_path: Path) -> int:
    """Zählt die Anzahl der Zeilen in einer Datei."""
    with open(file_path, "r") as f:
        return sum(1 for _ in f)


async def read_jsonl_chunks_async(
    file_path: Path,
    chunk_size: int,
    verbose: bool,
    visual: bool = False,
    total_lines: Optional[int] = None,
) -> AsyncIterator[Tuple[List[Dict], Optional[tqdm]]]:
    """
    Asynchroner Generator für JSON-Lines Chunks.
    Lädt Chunks erst, wenn sie benötigt werden, um Speicher zu sparen.
    """
    if verbose and not visual:
        logger.debug(f"Beginne Einlesen der Datei {file_path}")

    chunk: List[Dict] = []
    line_num = 0

    async with aiofiles.open(file_path, mode="r") as f:
        async for line in f:
            line_num += 1
            try:
                obj = json.loads(line.strip())
                chunk.append(obj)
                if len(chunk) >= chunk_size:
                    if verbose and not visual:
                        logger.debug(
                            f"Chunk mit {len(chunk)} Objekten geladen (bis Zeile {line_num})"
                        )
                    yield chunk, None
                    chunk = []
            except json.JSONDecodeError as e:
                if not visual:
                    logger.error(
                        f"Fehler beim Parsen der JSON-Line {line_num}: {str(e)}"
                    )
                continue

        if chunk:  # Letzter Chunk
            if verbose and not visual:
                logger.debug(f"Letzter Chunk mit {len(chunk)} Objekten geladen")
            yield chunk, None


async def process_chunk(
    chunk: List[Dict[str, Any]],
    fields: List[str],
    verbose: bool,
    timeout: float,
    timeout_urls: Set[str],
    keep_timeout_urls: bool,
    stats: Statistics,
    follow_redirects: bool,
    chunk_progress: Optional[tqdm] = None,
) -> List[Dict[str, Any]]:
    """
    Verarbeitet einen Chunk von JSON-Objekten parallel.
    Unterstützt sowohl einzelne URLs als auch Arrays von URLs.
    """
    processed_chunk = []
    if verbose and not chunk_progress:
        logger.debug(f"Verarbeite Chunk mit {len(chunk)} Objekten")
    async with aiohttp.ClientSession() as session:
        for item_num, item in enumerate(chunk, 1):
            processed_item = item.copy()
            if verbose and not chunk_progress:
                logger.debug(f"Verarbeite Objekt {item_num}/{len(chunk)}")
            for field in fields:
                if field in processed_item:
                    value = processed_item[field]

                    # Verarbeite Arrays von URLs
                    if isinstance(value, list):
                        valid_urls = []
                        for url in value:
                            if isinstance(url, str) and is_valid_url(url):
                                if verbose and not chunk_progress:
                                    logger.debug(
                                        f"Prüfe URL aus Array in Feld '{field}': {url}"
                                    )
                                (
                                    is_valid,
                                    new_url,
                                    is_timeout,
                                    status_code,
                                ) = await check_url(
                                    session,
                                    url,
                                    verbose and not chunk_progress,
                                    timeout,
                                )
                                stats.add_url_check(
                                    field,
                                    url,
                                    is_valid,
                                    new_url,
                                    is_timeout,
                                    status_code,
                                )

                                if is_timeout:
                                    timeout_urls.add(url)
                                    if verbose and not chunk_progress:
                                        logger.debug(
                                            f"URL {url} zum Timeout-Log hinzugefügt"
                                        )
                                        if keep_timeout_urls:
                                            logger.debug(
                                                f"URL {url} wird trotz Timeout behalten"
                                            )
                                            valid_urls.append(url)
                                        else:
                                            logger.debug(
                                                f"URL {url} wird wegen Timeout gelöscht"
                                            )
                                elif not is_valid:
                                    if verbose and not chunk_progress:
                                        logger.debug(
                                            f"Lösche ungültige URL aus Array: {url}"
                                        )
                                elif new_url != url and follow_redirects:
                                    if verbose and not chunk_progress:
                                        logger.debug(
                                            f"Aktualisiere URL im Array von {url} zu {new_url}"
                                        )
                                    valid_urls.append(new_url)
                                else:
                                    valid_urls.append(url)

                        # Entferne das Feld wenn das Array leer ist
                        if valid_urls:
                            processed_item[field] = valid_urls
                        else:
                            del processed_item[field]

                    # Verarbeite einzelne URL
                    elif isinstance(value, str) and is_valid_url(value):
                        if verbose and not chunk_progress:
                            logger.debug(
                                f"Prüfe einzelne URL in Feld '{field}': {value}"
                            )
                        is_valid, new_url, is_timeout, status_code = await check_url(
                            session, value, verbose and not chunk_progress, timeout
                        )
                        stats.add_url_check(
                            field, value, is_valid, new_url, is_timeout, status_code
                        )

                        if is_timeout:
                            timeout_urls.add(value)
                            if verbose and not chunk_progress:
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
                            if verbose and not chunk_progress:
                                logger.debug(
                                    f"Lösche ungültiges Feld '{field}' mit URL: {value}"
                                )
                            del processed_item[field]
                        elif new_url != value and follow_redirects:
                            if verbose and not chunk_progress:
                                logger.debug(
                                    f"Aktualisiere URL in Feld '{field}' von {value} zu {new_url}"
                                )
                            processed_item[field] = new_url
                    else:
                        if verbose and not chunk_progress and isinstance(value, str):
                            logger.debug(
                                f"Lösche Feld '{field}' mit ungültiger URL: {value}"
                            )
                        del processed_item[field]
            processed_chunk.append(processed_item)
            if chunk_progress:
                chunk_progress.update(1)
    return processed_chunk


async def process_chunk_in_thread(
    chunk_data: Tuple[List[Dict], int],
    fields: List[str],
    timeout: float,
    keep_timeout_urls: bool,
    follow_redirects: bool,
    output_base: Path,
    thread_id: int,
    timeout_urls: Set[str],
    stats: Statistics,
    visual: bool = False,
) -> None:
    """
    Verarbeitet einen Chunk in einem separaten Thread.
    """
    chunk, chunk_num = chunk_data
    chunk_progress = None
    if visual:
        chunk_progress = tqdm(
            total=len(chunk),
            desc=f"Thread {thread_id}",
            unit="URLs",
            position=thread_id + 1,
            leave=False,
        )

    processed_chunk = await process_chunk(
        chunk,
        fields,
        False,  # verbose ist immer False in Threads
        timeout,
        timeout_urls,
        keep_timeout_urls,
        stats,
        follow_redirects,
        chunk_progress,
    )

    # Schreibe Ergebnisse in eine Thread-spezifische Datei
    output_file = (
        output_base.parent / f"{output_base.stem}_{chunk_num:05d}{output_base.suffix}"
    )
    with open(output_file, "w") as out_f:
        for item in processed_chunk:
            out_f.write(json.dumps(item, ensure_ascii=False) + "\n")


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
    visual: bool = False,
    num_threads: int = 1,
):
    """
    Hauptfunktion zur Verarbeitung der JSON-Lines Datei.
    """
    try:
        if verbose and not visual:
            logger.debug(f"Starte Verarbeitung von {input_file}")
            logger.debug(f"Zu prüfende Felder: {', '.join(fields)}")
            logger.debug(f"Chunk-Größe: {chunk_size}")
            logger.debug(f"Timeout: {timeout} Sekunden")
            logger.debug(f"Anzahl Threads: {num_threads}")
            logger.debug(
                f"Timeout-URLs werden {'behalten' if keep_timeout_urls else 'gelöscht'}"
            )
            logger.debug(
                f"Weiterleitungen werden {'verfolgt' if follow_redirects else 'beibehalten'}"
            )
            if timeout_file:
                logger.debug(f"Timeout-URLs werden in {timeout_file} gespeichert")

        total_lines = count_lines(input_file) if visual else None
        timeout_urls: Set[str] = set()
        stats = Statistics()
        chunk_num = 0

        if visual:
            main_progress = tqdm(
                total=total_lines,
                desc="Gesamtfortschritt",
                unit="Zeilen",
                position=0,
                leave=True,
            )

        # Verarbeite Chunks in Batches von num_threads
        active_tasks: List[asyncio.Task] = []
        async for chunk, _ in read_jsonl_chunks_async(
            input_file, chunk_size, verbose, False, total_lines
        ):
            # Erstelle Task für aktuellen Chunk
            task = asyncio.create_task(
                process_chunk_in_thread(
                    (chunk, chunk_num),
                    fields,
                    timeout,
                    keep_timeout_urls,
                    follow_redirects,
                    output_file,
                    len(active_tasks),
                    timeout_urls,
                    stats,
                    visual,
                )
            )
            active_tasks.append(task)
            chunk_num += 1

            # Wenn wir genug Tasks haben oder es der letzte Chunk ist
            if len(active_tasks) >= num_threads:
                # Warte auf Abschluss aller aktiven Tasks
                await asyncio.gather(*active_tasks)
                if visual:
                    main_progress.update(
                        sum(
                            len(c[0])
                            for c in [t.result() for t in active_tasks if t.done()]
                        )
                    )
                active_tasks = []

        # Warte auf verbleibende Tasks
        if active_tasks:
            await asyncio.gather(*active_tasks)
            if visual:
                main_progress.update(
                    sum(
                        len(c[0])
                        for c in [t.result() for t in active_tasks if t.done()]
                    )
                )

        if visual:
            main_progress.close()

        # Kombiniere die Teildateien
        with open(output_file, "w") as out_f:
            for i in range(chunk_num):
                part_file = (
                    output_file.parent
                    / f"{output_file.stem}_{i:05d}{output_file.suffix}"
                )
                if part_file.exists():
                    with open(part_file, "r") as in_f:
                        out_f.write(in_f.read())
                    part_file.unlink()  # Lösche die Teildatei

        if timeout_file and timeout_urls:
            with open(timeout_file, "w") as tf:
                for url in sorted(timeout_urls):
                    tf.write(f"{url}\n")
            if verbose and not visual:
                logger.debug(
                    f"{len(timeout_urls)} Timeout-URLs in {timeout_file} gespeichert"
                )

        if not visual:
            stats.print_summary(verbose)

    except Exception as e:
        logger.error(f"Fehler bei der Verarbeitung: {str(e)}")
        sys.exit(1)


def main():
    """
    Haupteinstiegspunkt des Programms.
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
    parser.add_argument(
        "--visual",
        action="store_true",
        help="Zeigt eine visuelle Fortschrittsanzeige",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Anzahl der parallel arbeitenden Threads (Standard: 1)",
    )

    args = parser.parse_args()
    setup_logging(args.verbose and not args.visual)

    input_path = Path(args.input_file)
    if not input_path.exists():
        logger.error(f"Eingabedatei {input_path} existiert nicht.")
        sys.exit(1)

    output_path = (
        input_path.parent / f"{input_path.stem}{args.suffix}{input_path.suffix}"
    )

    timeout_path = Path(args.timeout_file) if args.timeout_file else None

    if args.verbose and not args.visual:
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
            args.visual,
            args.threads,
        )
    )

    if args.verbose and not args.visual:
        logger.debug("Programmende")
    if not args.visual:
        logger.info(
            f"Verarbeitung abgeschlossen. Ergebnis wurde in {output_path} gespeichert."
        )


if __name__ == "__main__":
    main()
