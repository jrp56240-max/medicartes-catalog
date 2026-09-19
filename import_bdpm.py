#!/usr/bin/env python3
"""Construit le catalogue SQLite de MediCartes depuis les fichiers BDPM.

Le script utilise uniquement la bibliotheque standard de Python.
Il telecharge les fichiers officiels, verifie qu'ils ne sont pas vides,
puis cree une nouvelle base SQLite de facon atomique.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DIRECTORY = PROJECT_ROOT / "data" / "bdpm"
OUTPUT_DIRECTORY = PROJECT_ROOT / "assets" / "database"
DATABASE_PATH = OUTPUT_DIRECTORY / "medicartes_catalog.db"
MANIFEST_PATH = OUTPUT_DIRECTORY / "catalog_manifest.json"

# Cette adresse est isolee ici pour pouvoir la remplacer facilement si
# l'administration modifie son service de telechargement.
DOWNLOAD_ENDPOINT = (
    "https://rec-bdm.ansm.integra.fr/telechargement.php?fichier={filename}"
)

FILES = {
    "specialties": "CIS_bdpm.txt",
    "presentations": "CIS_CIP_bdpm.txt",
    "compositions": "CIS_COMPO_bdpm.txt",
}

MINIMUM_FILE_SIZE = 1_000


def normalize_search_text(value: str) -> str:
    """Cree une version simple pour les recherches sans accents."""
    normalized = unicodedata.normalize("NFD", value.strip().lower())
    return "".join(
        character
        for character in normalized
        if unicodedata.category(character) != "Mn"
    )


def clean(value: str) -> str:
    return value.replace("\u00a0", " ").strip()


def download_file(filename: str, required: bool = True) -> Path | None:
    RAW_DIRECTORY.mkdir(parents=True, exist_ok=True)

    destination = RAW_DIRECTORY / filename
    temporary_destination = destination.with_suffix(destination.suffix + ".download")
    url = DOWNLOAD_ENDPOINT.format(filename=urllib.parse.quote(filename))

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "MediCartes-BDPM-Importer/1.0",
            "Accept": "text/plain,application/octet-stream,*/*",
        },
    )

    last_error: Exception | None = None

    for attempt in range(1, 4):
        try:
            print(f"Telechargement de {filename} (tentative {attempt}/3)...")

            with urllib.request.urlopen(request, timeout=120) as response:
                data = response.read()

            if len(data) < MINIMUM_FILE_SIZE:
                raise ValueError(
                    f"Le fichier recu est anormalement petit ({len(data)} octets)."
                )

            temporary_destination.write_bytes(data)
            temporary_destination.replace(destination)

            print(f"  OK : {len(data):,} octets")
            return destination
        except (OSError, ValueError, urllib.error.URLError) as error:
            last_error = error
            print(f"  Echec : {error}")
            if attempt < 3:
                time.sleep(2 * attempt)

    if temporary_destination.exists():
        temporary_destination.unlink()

    if required:
        raise RuntimeError(
            f"Impossible de telecharger le fichier obligatoire {filename}."
        ) from last_error

    print(
        f"AVERTISSEMENT : {filename} est temporairement indisponible. "
        "Le catalogue sera cree sans les codes CIP13."
    )
    return None


def decode_bdpm_file(path: Path) -> str:
    data = path.read_bytes()

    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue

    raise RuntimeError(f"Encodage inconnu pour {path.name}")


def read_rows(path: Path) -> list[list[str]]:
    content = decode_bdpm_file(path)
    reader = csv.reader(io.StringIO(content), delimiter="\t", quotechar='"')
    return [row for row in reader if row]


def parse_price(value: str) -> float | None:
    cleaned_value = clean(value).replace(" ", "").replace(",", ".")

    if not cleaned_value:
        return None

    try:
        return float(cleaned_value)
    except ValueError:
        return None


def create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;
        PRAGMA user_version = 1;

        CREATE TABLE specialties (
          cis TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          search_name TEXT NOT NULL,
          pharmaceutical_form TEXT,
          administration_routes TEXT,
          authorization_status TEXT,
          market_status TEXT,
          authorization_date TEXT,
          holder TEXT
        );

        CREATE TABLE presentations (
          cip13 TEXT PRIMARY KEY,
          cis TEXT NOT NULL,
          label TEXT NOT NULL,
          market_status TEXT,
          marketing_date TEXT,
          reimbursement_rate TEXT,
          price REAL,
          FOREIGN KEY (cis) REFERENCES specialties(cis)
        );

        CREATE TABLE compositions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          cis TEXT NOT NULL,
          substance_code TEXT,
          substance_name TEXT NOT NULL,
          search_substance_name TEXT NOT NULL,
          dosage TEXT,
          dosage_reference TEXT,
          component_nature TEXT,
          FOREIGN KEY (cis) REFERENCES specialties(cis)
        );

        CREATE TABLE catalog_information (
          information_key TEXT PRIMARY KEY,
          information_value TEXT NOT NULL
        );
        """
    )


def import_specialties(
    connection: sqlite3.Connection,
    path: Path,
) -> tuple[int, set[str]]:
    values: list[tuple[str, ...]] = []

    for row in read_rows(path):
        if len(row) < 12:
            continue

        cis = clean(row[0])
        name = clean(row[1])

        if not cis or not name:
            continue

        values.append(
            (
                cis,
                name,
                normalize_search_text(name),
                clean(row[2]),
                clean(row[3]),
                clean(row[4]),
                clean(row[6]),
                clean(row[7]),
                clean(row[10]),
            )
        )

    connection.executemany(
        """
        INSERT OR REPLACE INTO specialties (
          cis,
          name,
          search_name,
          pharmaceutical_form,
          administration_routes,
          authorization_status,
          market_status,
          authorization_date,
          holder
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )

    return len(values), {value[0] for value in values}


def import_presentations(
    connection: sqlite3.Connection,
    path: Path | None,
    known_cis: set[str],
) -> int:
    if path is None:
        return 0

    values: list[tuple[object, ...]] = []

    for row in read_rows(path):
        if len(row) < 10:
            continue

        cis = clean(row[0])
        cip13 = clean(row[6])
        label = clean(row[2])

        if cis not in known_cis or not cip13 or not label:
            continue

        values.append(
            (
                cip13,
                cis,
                label,
                clean(row[4]),
                clean(row[5]),
                clean(row[8]),
                parse_price(row[9]),
            )
        )

    connection.executemany(
        """
        INSERT OR REPLACE INTO presentations (
          cip13,
          cis,
          label,
          market_status,
          marketing_date,
          reimbursement_rate,
          price
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )

    return len(values)


def import_compositions(
    connection: sqlite3.Connection,
    path: Path,
    known_cis: set[str],
) -> int:
    values: list[tuple[str, ...]] = []

    for row in read_rows(path):
        if len(row) < 7:
            continue

        cis = clean(row[0])
        substance_name = clean(row[3])

        if cis not in known_cis or not substance_name:
            continue

        values.append(
            (
                cis,
                clean(row[2]),
                substance_name,
                normalize_search_text(substance_name),
                clean(row[4]),
                clean(row[5]),
                clean(row[6]),
            )
        )

    connection.executemany(
        """
        INSERT INTO compositions (
          cis,
          substance_code,
          substance_name,
          search_substance_name,
          dosage,
          dosage_reference,
          component_nature
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )

    return len(values)


def create_indexes(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE INDEX specialties_name_index
          ON specialties(search_name);

        CREATE INDEX presentations_cis_index
          ON presentations(cis);

        CREATE INDEX compositions_cis_index
          ON compositions(cis);

        CREATE INDEX compositions_substance_index
          ON compositions(search_substance_name);
        """
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def build_database(
    specialties_path: Path,
    presentations_path: Path | None,
    compositions_path: Path,
) -> dict[str, object]:
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    temporary_database = DATABASE_PATH.with_suffix(".db.new")

    if temporary_database.exists():
        temporary_database.unlink()

    connection = sqlite3.connect(temporary_database)

    try:
        create_schema(connection)

        print("Importation des specialites...")
        specialty_count, known_cis = import_specialties(
            connection,
            specialties_path,
        )

        print("Importation des presentations...")
        presentation_count = import_presentations(
            connection,
            presentations_path,
            known_cis,
        )

        print("Importation des compositions...")
        composition_count = import_compositions(
            connection,
            compositions_path,
            known_cis,
        )

        if specialty_count < 1_000:
            raise RuntimeError(
                f"Seulement {specialty_count} specialites ont ete importees."
            )

        if composition_count < 1_000:
            raise RuntimeError(
                f"Seulement {composition_count} compositions ont ete importees."
            )

        downloaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        metadata = {
            "database_version": "1",
            "catalog_version": datetime.now(timezone.utc).strftime("%Y-%m"),
            "source": "Base de donnees publique des medicaments",
            "downloaded_at": downloaded_at,
            "specialty_count": str(specialty_count),
            "presentation_count": str(presentation_count),
            "composition_count": str(composition_count),
        }

        connection.executemany(
            """
            INSERT INTO catalog_information (
              information_key,
              information_value
            ) VALUES (?, ?)
            """,
            metadata.items(),
        )

        create_indexes(connection)
        connection.commit()

        integrity_result = connection.execute("PRAGMA quick_check").fetchone()

        if integrity_result is None or integrity_result[0] != "ok":
            raise RuntimeError("La verification SQLite a echoue.")

        connection.execute("VACUUM")
    except Exception:
        connection.close()
        if temporary_database.exists():
            temporary_database.unlink()
        raise
    else:
        connection.close()

    temporary_database.replace(DATABASE_PATH)

    database_hash = sha256(DATABASE_PATH)

    manifest: dict[str, object] = {
        "schema_version": 1,
        "catalog_version": datetime.now(timezone.utc).strftime("%Y-%m"),
        "generated_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "database_file": DATABASE_PATH.name,
        "database_size": DATABASE_PATH.stat().st_size,
        "sha256": database_hash,
        "counts": {
            "specialties": specialty_count,
            "presentations": presentation_count,
            "compositions": composition_count,
        },
    }

    MANIFEST_PATH.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return manifest


def main() -> int:
    print("=== Import BDPM pour MediCartes ===")

    try:
        specialties_path = download_file(FILES["specialties"], required=True)
        presentations_path = download_file(FILES["presentations"], required=False)
        compositions_path = download_file(FILES["compositions"], required=True)

        if specialties_path is None or compositions_path is None:
            raise RuntimeError("Les fichiers obligatoires sont absents.")

        manifest = build_database(
            specialties_path,
            presentations_path,
            compositions_path,
        )
    except Exception as error:
        print(f"\nERREUR : {error}", file=sys.stderr)
        return 1

    counts = manifest["counts"]

    print("\nImport termine avec succes.")
    print(f"Base creee : {DATABASE_PATH}")
    print(f"Specialites : {counts['specialties']:,}")
    print(f"Presentations : {counts['presentations']:,}")
    print(f"Compositions : {counts['compositions']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
