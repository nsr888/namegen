#!/usr/bin/env python3
import gzip
import re
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
import random
from pykakasi import kakasi
import zipfile
import shutil

DATA = Path("./data")
DATA.mkdir(exist_ok=True)

_KKS = kakasi()  # Shared converter instance (thread-safe for CLI)


def to_romaji(text: str) -> str:
    """Convert Japanese text to romaji using hepburn style."""
    parts = _KKS.convert(text)
    romaji = "".join(part.get("hepburn", "") for part in parts)
    result = romaji.strip().title()
    return result


def _run_curl(url: str, out: Path, insecure: bool = False) -> None:
    """Download file using curl with retry logic."""
    args = [
        "curl",
        "-L",
        url,
        "--retry",
        "3",
        "--retry-delay",
        "2",
        "--connect-timeout",
        "15",
        "--max-time",
        "120",
        "-o",
        str(out),
    ]
    if insecure:
        args.insert(1, "--insecure")  # -k
    subprocess.run(args, check=True)


def download_if_missing() -> None:
    """Download required data files if missing."""
    # JMnedict: multiple sources and formats
    jm_gz, jm_xml, jm_zip = (
        DATA / "JMnedict.xml.gz",
        DATA / "JMnedict.xml",
        DATA / "JMnedict.xml.zip",
    )

    # Download JMnedict if missing
    if not any(p.exists() for p in (jm_gz, jm_xml, jm_zip)):
        sources = [
            ("https://ftp.edrdg.org/pub/Nihongo/JMnedict.xml.gz", True, jm_gz),  # EDRDG
            (
                "https://ftp.usf.edu/pub/ftp.monash.edu.au/pub/nihongo/JMnedict.xml.gz",
                False,
                jm_gz,
            ),  # USF mirror
            (
                "http://ftp.edrdg.org/pub/Nihongo/JMnedict.xml.gz",
                False,
                jm_gz,
            ),  # EDRDG HTTP
            (
                "https://raw.githubusercontent.com/echamudi/jp-resources-mirror/"
                "443711d6fab8072f7ec23cdd00f47e8f4d51aa71/EDRDG%20-%202021-06-30/JMnedict.xml.zip",
                False,
                jm_zip,
            ),  # GitHub mirror
        ]
        last_error = None
        for i, (url, insecure, target) in enumerate(sources, 1):
            try:
                _run_curl(url, target, insecure=insecure)
                break
            except subprocess.CalledProcessError as e:
                last_error = e
        else:
            raise RuntimeError("All JMnedict sources failed") from last_error

    # Handle file extraction
    _extract_jmnedict_files(jm_xml, jm_gz, jm_zip)
    _extract_census_data()


def _extract_jmnedict_files(jm_xml: Path, jm_gz: Path, jm_zip: Path) -> None:
    """Extract JMnedict from various formats."""
    if jm_zip.exists() and not jm_xml.exists():
        print(f"Unzipping {jm_zip} -> {jm_xml.parent}")
        with zipfile.ZipFile(jm_zip, "r") as zf:
            member = next(
                (m for m in zf.namelist() if m.lower().endswith("jmnedict.xml")), None
            )
            if not member:
                raise RuntimeError("JMnedict.xml not found inside zip")
            tmp = jm_xml.with_suffix(".xml.tmp")
            with zf.open(member) as src, open(tmp, "wb") as dst:
                shutil.copyfileobj(src, dst)
            tmp.rename(jm_xml)

    if jm_gz.exists() and not jm_xml.exists():
        print(f"Unzipping {jm_gz} -> {jm_xml}")
        with gzip.open(jm_gz, "rb") as src, open(jm_xml, "wb") as dst:
            shutil.copyfileobj(src, dst)


def _extract_census_data() -> None:
    """Extract US Census surnames."""
    zip_path = DATA / "us_surnames.zip"
    csv_path = DATA / "Names_2010Census.csv"
    if not csv_path.exists():
        if not zip_path.exists():
            _run_curl(
                "https://www2.census.gov/topics/genealogy/2010surnames/names.zip",
                zip_path,
            )
        print(f"Extracting US Census CSV -> {csv_path}")
        subprocess.run(["unzip", "-j", str(zip_path), "-d", str(DATA)], check=True)


def _open_jmnedict_stream():
    """Return file-like object for JMnedict regardless of format."""
    if (xml_path := DATA / "JMnedict.xml").exists():
        return xml_path.open("rb")
    if (gz_path := DATA / "JMnedict.xml.gz").exists():
        return gzip.open(gz_path, "rb")
    raise FileNotFoundError("JMnedict not found (xml or xml.gz)")


def parse_japanese_names() -> list[str]:
    """Parse Japanese names from JMnedict XML file."""

    def _ends(tag: str | None, name: str) -> bool:
        return tag is not None and tag.lower().endswith(name.lower())

    def _text_lower(e) -> str:
        return (e.text or "").strip().lower()

    f = _open_jmnedict_stream()
    given_names = set()
    fallback_candidates = set()
    total_entries = 0
    processed_entries = 0

    # Stream parse to keep memory low
    context = ET.iterparse(f, events=("end",))
    for event, elem in context:
        total_entries += 1
        if not _ends(elem.tag, "entry"):
            continue
        processed_entries += 1

        # Collect name_type values (any presence marks as a candidate)
        name_types = {
            _text_lower(nt)
            for nt in elem.iter()
            if _ends(nt.tag, "name_type") and nt.text
        }
        has_name_type = bool(name_types)

        # Treat as "given" if any of these tokens appear
        # Treat any entry with a name_type as a candidate
        is_given = has_name_type

        # Collect readings
        kana_readings = [r.text for r in elem.iter() if _ends(r.tag, "reb") and r.text]
        kanji_readings = [k.text for k in elem.iter() if _ends(k.tag, "keb") and k.text]

        if not kana_readings and not kanji_readings:
            elem.clear()
            continue

        # Choose shortest reading to avoid "surname+given" concatenations
        base = None
        if kana_readings:
            only_kana = [
                r for r in kana_readings if re.fullmatch(r"[\u3040-\u30FF]+", r)
            ]
            base = min(only_kana or kana_readings, key=len)
        else:
            base = min(kanji_readings, key=len)

        romaji = to_romaji(base)
        if not (
            2 <= len(romaji) <= 14 and re.fullmatch(r"[A-Za-z][A-Za-z' -]*", romaji)
        ):
            elem.clear()
            continue

        if is_given:
            given_names.add(romaji)
        else:
            # Heuristic: short kana usually indicates a given name (fallback)
            fallback_candidates.add(romaji)

        elem.clear()

    f.close()

    # If nothing explicitly marked as "given", use fallback candidates
    # Combine both sets (given_names and fallback_candidates)
    combined = given_names | fallback_candidates
    if not combined:
        raise RuntimeError("No Japanese names found after parsing.")
    result = sorted(combined)

    return result


def parse_western_surnames() -> list[str]:
    """Parse western surnames from census."""
    census = pd.read_csv(DATA / "Names_2010Census.csv")
    census_surnames = {str(n).title() for n in census["name"] if isinstance(n, str)}

    result = sorted(census_surnames)
    return result


def generate_name(jp_names: list[str], west_surnames: list[str]) -> str:
    """Generate a Japanese-Western hybrid name."""
    return f"{random.choice(jp_names)} {random.choice(west_surnames)}"


def save_to_jp_names_file(name: str) -> None:
    """Save generated name to jp_names.txt file."""
    jp_names_path = DATA / "jp_names.txt"
    try:
        with open(jp_names_path, "a", encoding="utf-8") as f:
            f.write(f"{name}\n")
    except Exception as e:
        print(f"Failed to save name to {jp_names_path}: {e}")


def load_from_jp_names_file() -> list[str]:
    """Load names from jp_names.txt as a source."""
    jp_names_path = DATA / "jp_names.txt"
    if not jp_names_path.exists():
        return []

    try:
        with open(jp_names_path, "r", encoding="utf-8") as f:
            names = [line.strip() for line in f if line.strip()]
        return names
    except Exception as e:
        print(f"Failed to load names from {jp_names_path}: {e}")
        return []


def ensure_data_prepared() -> None:
    """Ensure all data files exist, fetching them if necessary."""
    # Check if jp_names.txt exists and has content
    jp_names_path = DATA / "jp_names.txt"
    jp_names_available = jp_names_path.exists() and jp_names_path.stat().st_size > 0

    # Always download other required data files
    download_if_missing()

    # If jp_names.txt is missing/empty, populate it
    if not jp_names_available:
        jp_names = parse_japanese_names()

        # Save populated jp_names.txt for future use
        with open(jp_names_path, "w", encoding="utf-8") as f:
            for name in jp_names:
                f.write(f"{name}\n")


def main():
    """Main entry point with automatic data preparation and name generation."""
    import argparse

    ensure_data_prepared()

    # Load Japanese names from jp_names.txt (freshly populated if missing)
    jp_names = load_from_jp_names_file()
    if not jp_names:
        print("No Japanese names available. Please check data files.")
        return 1

    # Load western surnames
    west_surnames = parse_western_surnames()

    # Generate and save names
    result = generate_name(jp_names, west_surnames)
    print(result)
    print(result.lower().replace(" ", "-"))

    return 0


if __name__ == "__main__":
    main()
