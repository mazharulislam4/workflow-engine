#!/usr/bin/env python3
"""Remove emoji characters from Python files"""
import glob
import re

# Emoji mappings
EMOJI_MAP = {
    "🔀": "[FORK]",
    "✅": "[OK]",
    "❌": "[FAIL]",
    "➡️": "->",
    "⏭️": "[SKIP]",
    "🌐": "[HTTP]",
    "🔁": "[LOOP]",
    "🛤️": "[PATH]",
    "🏁": "[END]",
    "⚠️": "[WARN]",
    "⏸️": "[PAUSE]",
}

# Find all Python files in workflow/executors
files = glob.glob("workflow/executors/**/*.py", recursive=True)

for filepath in files:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Replace all emojis
        modified = content
        for emoji, replacement in EMOJI_MAP.items():
            modified = modified.replace(emoji, replacement)

        # Write back if modified
        if modified != content:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(modified)
            print(f"✓ Updated: {filepath}")
        else:
            print(f"  Skipped: {filepath}")
    except Exception as e:
        print(f"✗ Error processing {filepath}: {e}")

print("\nDone!")
