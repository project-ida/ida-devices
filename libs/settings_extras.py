#!/usr/bin/env python3
"""
settings_extras.py

Utilities to pull extra metadata out of settings.xml.
Now prints a console warning if digitizer info can’t be extracted.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional


def extract_digitizer_info(settings_path: str) -> Optional[str]:
    """
    Reads <board><modelName> and <board><serialNumber> from the given settings.xml
    and returns "modelName (serialNumber)", or None if any step fails.
    Prints a warning to the terminal on failure.
    """
    p = Path(settings_path)
    if not p.exists():
        print(f"⚠️  Digitizer info: file not found: {settings_path}")
        return None

    if p.stat().st_size == 0:
        print(f"⚠️  Digitizer info: file is empty: {settings_path}")
        return None

    try:
        tree = ET.parse(str(p))
    except ET.ParseError:
        print(f"⚠️  Digitizer info: malformed XML: {settings_path}")
        return None

    root = tree.getroot()
    board = root.find('board')
    if board is None:
        print(f"⚠️  Digitizer info: missing <board> element in {settings_path}")
        return None

    model  = (board.findtext('modelName')    or '').strip()
    serial = (board.findtext('serialNumber') or '').strip()
    if not model or not serial:
        print(f"⚠️  Digitizer info: missing modelName or serialNumber in {settings_path}")
        return None

    return f"{model} ({serial})"
