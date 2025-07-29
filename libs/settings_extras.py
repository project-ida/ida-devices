#!/usr/bin/env python3
"""
settings_extras.py

Utilities to pull extra metadata out of settings.xml.
Now prints a console warning if digitizer info can’t be extracted.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
import logging
from typing import Optional, List, Tuple, Dict
from libs.settings_validator import _load_parameter_map

BOARD_TAG = 'board'
MODEL_NAME_TAG = 'modelName'
SERIAL_NUMBER_TAG = 'serialNumber'


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

    root = _parse_settings_xml(str(p))
    if root is None:
        return None

    return _extract_board_info(root, settings_path)

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

    refs = _load_reference_parameter_maps(cdir)
    matches = [ref.name for ref, r_map in refs if _is_exact_match(s_map, r_map)]
    return matches

def _parse_settings_xml(settings_path: str) -> Optional[ET.Element]:
    """
    Parse the settings.xml file and return the root element.

    Parameters:
    settings_path (str): Path to the settings.xml file.

    Returns:
    Optional[ET.Element]: Root XML element or None if parsing fails.
    """
    try:
        tree = ET.parse(settings_path)
        return tree.getroot()
    except ET.ParseError:
        logging.warning(f"⚠️  Digitizer info: malformed XML: {settings_path}")
    except Exception as e:
        logging.warning(f"⚠️  Digitizer info: error reading '{settings_path}': {e}")
    return None

def _extract_board_info(root: ET.Element, settings_path: str) -> Optional[str]:
    """
    Extract model and serial number from the board element.

    Parameters:
    root (ET.Element): Root XML element.
    settings_path (str): Path to the settings.xml file.

    Returns:
    Optional[str]: Formatted digitizer info or None if missing.
    """
    board = root.find(BOARD_TAG)
    if board is None:
        logging.warning(f"⚠️  Digitizer info: missing <{BOARD_TAG}> element in {settings_path}")
        return None

    model = (board.findtext(MODEL_NAME_TAG) or '').strip()
    serial = (board.findtext(SERIAL_NUMBER_TAG) or '').strip()
    if not model or not serial:
        logging.warning(f"⚠️  Digitizer info: missing {MODEL_NAME_TAG} or {SERIAL_NUMBER_TAG} in {settings_path}")
        return None

    return f"{model} ({serial})"

def _load_reference_parameter_maps(config_dir: Path) -> List[Tuple[Path, Dict[str, str]]]:
    """
    Load parameter maps from all reference XML files in the config directory.

    Parameters:
    config_dir (Path): Path to the config directory.

    Returns:
    List[Tuple[Path, Dict[str, str]]]: List of (Path, parameter map) tuples.
    """
    refs = []
    for ref in sorted(config_dir.glob('*.xml')):
        try:
            r_map, _ = _load_parameter_map(ref)
            refs.append((ref, r_map))
        except Exception as e:
            # Log the exception for better traceability and debugging
            logging.warning(
                f"⚠️  Failed to load parameter map from '{ref}': {e}"
            )
    return refs

def _is_exact_match(s_map: Dict[str, str], r_map: Dict[str, str]) -> bool:
    """
    Check if two parameter maps are an exact match.

    Parameters:
    s_map (Dict[str, str]): Settings parameter map.
    r_map (Dict[str, str]): Reference parameter map.

    Returns:
    bool: True if maps are equal, False otherwise.
    """
    return r_map == s_map