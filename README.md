# Name Generator

Tools for generating K8s namespace names.

## Usage

```bash
make        # Japanese-Western hybrid name (main.py)
make learn  # English-Russian vocabulary trainer (learn.py)
```

## Tools

### main.py - Japanese-Western Hybrid Names

Generates names like `Haruki-Johnson` for namespace naming.

1. Downloads JMnedict XML (Japanese name dictionary) and US Census surname CSV
2. Parses Japanese names, converts to romaji
3. Combines random Japanese given name with Western surname

### learn.py - English-Russian Vocabulary Trainer

Interactive tool that helps learn English vocabulary while generating namespace names.

1. Downloads 10,000 most common English words
2. Shows words from end of list (less common first)
3. You type Russian translation (Cyrillic) → script transliterates to Latin
4. Generates namespace like `poison-yad`

Flow:
- Word shown → "Do you know this?"
- **Yes**: added to `known.txt`, next word
- **No**: enter Russian translation, namespace generated, exit

Data files:
- `data/english_words.txt` - word list
- `data/known.txt` - words you already know (excluded)
