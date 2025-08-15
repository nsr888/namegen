# Name Generator

This Python script performs the following operations:

1. **Download data files** downloads JMnedict XML (Japanese name dictionary), US Census surname CSV, extracting them as needed.
2. **Parse Japanese names** parses the JMnedict XML, extracts givenname entries, converts them to romaji, and filters for valid names.
3. **Parse western surnames** reads US Census surnames (CSV) file, normalises and combines them.
4. **Generate a hybrid name** randomly combines a Japanese given name with a western surname to produce a JapaneseWestern hybrid name.
