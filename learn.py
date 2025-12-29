#!/usr/bin/env python3
"""
English-Russian vocabulary namespace generator.
Interactive learning tool that generates K8s namespace names.
"""

import subprocess
from pathlib import Path

DATA = Path(__file__).parent / "data"
DATA.mkdir(exist_ok=True)

WORDS_FILE = DATA / "english_words.txt"
KNOWN_FILE = DATA / "known.txt"

# Russian to Latin transliteration table
TRANSLIT_TABLE = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
    'е': 'e', 'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i',
    'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
    'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't',
    'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch',
    'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '',
    'э': 'e', 'ю': 'yu', 'я': 'ya',
    # Uppercase
    'А': 'a', 'Б': 'b', 'В': 'v', 'Г': 'g', 'Д': 'd',
    'Е': 'e', 'Ё': 'yo', 'Ж': 'zh', 'З': 'z', 'И': 'i',
    'Й': 'y', 'К': 'k', 'Л': 'l', 'М': 'm', 'Н': 'n',
    'О': 'o', 'П': 'p', 'Р': 'r', 'С': 's', 'Т': 't',
    'У': 'u', 'Ф': 'f', 'Х': 'kh', 'Ц': 'ts', 'Ч': 'ch',
    'Ш': 'sh', 'Щ': 'shch', 'Ъ': '', 'Ы': 'y', 'Ь': '',
    'Э': 'e', 'Ю': 'yu', 'Я': 'ya',
}


def transliterate(text: str) -> str:
    """Convert Russian Cyrillic text to Latin transliteration."""
    result = []
    for char in text:
        result.append(TRANSLIT_TABLE.get(char, char))
    return ''.join(result)


def download_words() -> None:
    """Download English word list if missing."""
    if WORDS_FILE.exists():
        return

    print("Downloading English word list...")
    url = "https://raw.githubusercontent.com/first20hours/google-10000-english/refs/heads/master/google-10000-english-usa-no-swears.txt"
    subprocess.run([
        "curl", "-L", url,
        "--retry", "3",
        "-o", str(WORDS_FILE)
    ], check=True)
    print(f"Saved to {WORDS_FILE}")


def load_words() -> list[str]:
    """Load all English words in order."""
    if not WORDS_FILE.exists():
        return []
    with open(WORDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip().lower() for line in f if line.strip()]


def load_known() -> set[str]:
    """Load known words (to exclude)."""
    if not KNOWN_FILE.exists():
        return set()
    with open(KNOWN_FILE, "r", encoding="utf-8") as f:
        return {line.strip().lower() for line in f if line.strip()}


def save_known(word: str) -> None:
    """Add word to known list."""
    with open(KNOWN_FILE, "a", encoding="utf-8") as f:
        f.write(f"{word}\n")


def get_next_unknown_word() -> tuple[str | None, int]:
    """Get first unknown word from end of file. Returns (word, remaining_count)."""
    all_words = load_words()
    known = load_known()

    # Count total unknown
    unknown_count = sum(1 for w in all_words if w not in known)

    # Find first unknown from end
    for word in reversed(all_words):
        if word not in known:
            return word, unknown_count

    return None, 0


def main():
    """Interactive vocabulary learning loop."""
    download_words()

    while True:
        word, remaining = get_next_unknown_word()

        if not word:
            print("Congratulations! You know all 10,000 words!")
            break

        print(f"\n{'='*40}")
        print(f"English word: {word}")
        print(f"Unknown words remaining: {remaining}")
        print(f"{'='*40}")

        # Ask if user knows this word first
        answer = input("Do you know this word? [y/n]: ").strip().lower()

        if answer in ('y', 'yes'):
            save_known(word)
            print(f"'{word}' added to known. Next word...")
            continue

        # User doesn't know - get translation and exit
        russian = input("Enter Russian translation (Cyrillic): ").strip()

        if not russian:
            print("No translation provided.")
            break

        # Transliterate to Latin
        transliterated = transliterate(russian).lower()

        # Generate namespace name
        namespace = f"{word}-{transliterated}"

        print(f"\n{russian} → {transliterated}")
        print(f"\nNamespace: {namespace}")
        break


if __name__ == "__main__":
    main()
