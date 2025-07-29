#!/usr/bin/env python3
"""
settings_extras.py

Utilities to pull extra metadata out of settings.xml.
Now prints a console warning if digitizer info can’t be extracted.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
import logging
from typing import Optional, List
from libs.settings_validator import _load_parameter_map


def extract_digitizer_info(settings_path: str) -> Optional[str]:
    """
    Extract digitizer model and serial number from a settings.xml file.

    Parameters:
    settings_path (str): Path to the settings.xml file.

    Returns:
    Optional[str]: String in the format "modelName (serialNumber)" or None if extraction fails.
    """
    p = Path(settings_path)
    if not p.exists():
        logging.warning(f"⚠️  Digitizer info: file not found: {settings_path}")
        return None

    if p.stat().st_size == 0:
        logging.warning(f"⚠️  Digitizer info: file is empty: {settings_path}")
        return None

    try:
        tree = ET.parse(str(p))
    except ET.ParseError:
        logging.warning(f"⚠️  Digitizer info: malformed XML: {settings_path}")
        return None

    root = tree.getroot()
    board = root.find('board')
    if board is None:
        logging.warning(f"⚠️  Digitizer info: missing <board> element in {settings_path}")
        return None

    model  = (board.findtext('modelName')    or '').strip()
    serial = (board.findtext('serialNumber') or '').strip()
    if not model or not serial:
        logging.warning(f"⚠️  Digitizer info: missing modelName or serialNumber in {settings_path}")
        return None

    return f"{model} ({serial})"


def find_matching_config_files(settings_path: str, config_folder: str) -> List[str]:
    """
    Find XML files in config_folder whose <parameters> exactly match those in settings_path.

    Parameters:
    settings_path (str): Path to the settings.xml file.
    config_folder (str): Path to the folder containing reference config XMLs.

    Returns:
    List[str]: List of matching config filenames.
    """
    sfile = Path(settings_path)
    cdir = Path(config_folder)
    if not sfile.is_file() or not cdir.is_dir():
        return []
    try:
        s_map, _ = _load_parameter_map(sfile)
    except Exception as e:
        logging.warning(f"⚠️  Cannot load parameters from {settings_path}: {e}")
        return []

    matches: List[str] = []
    for ref in sorted(cdir.glob('*.xml')):
        try:
            r_map, _ = _load_parameter_map(ref)
        except Exception:
            continue
        if r_map == s_map:
            matches.append(ref.name)
    return matches