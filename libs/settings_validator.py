# libs/settings_validator.py

import xml.etree.ElementTree as ET
from pathlib import Path
import logging
from typing import Dict, List, Tuple


IGNORE_TAG_RUN_ID = 'runId'
PARAMETERS_TAG = 'parameters'
ENTRY_TAG = 'entry'
KEY_TAG = 'key'
VALUE_TAG = 'value'

def report_parameter_diffs(
    settings_path: str,
    config_folder: str,
    max_diffs: int = 3
) -> None:
    """
    Compare <parameters> in settings.xml against each .xml in config_folder.
    Print grouped results and up to max_diffs per file.

    Parameters:
    settings_path (str): Path to the settings.xml file.
    config_folder (str): Path to the folder with reference XMLs.
    max_diffs (int): Maximum number of diff lines to print per file.
    """
    sfile = Path(settings_path)
    cdir  = Path(config_folder)

    # Pre-checks
    if not sfile.is_file() or sfile.stat().st_size == 0:
        logging.warning(f"⚠️  Skipping diff: '{settings_path}' is missing or empty.")
        return
    if not cdir.is_dir():
        logging.warning(f"⚠️  Config folder not found: {config_folder}")
        return

    refs = sorted(cdir.glob('*.xml'))
    if not refs:
        logging.warning(f"⚠️  No reference XMLs in {cdir}")
        return

    # Load master parameter map
    s_map, s_lines = _load_parameter_map(sfile)
    if not s_map:
        logging.warning(f"⚠️  Skipping diff: '{settings_path}' could not be parsed or has no parameters.")
        return

    # Build a reverse lookup: value -> line number(s)
    value_to_lineno = {}
    for i, ln in enumerate(s_lines):
        # Try to extract value from line
        if '</value>' in ln:
            val = ln.split('>')[-2].split('<')[0].strip()
            if val:
                value_to_lineno.setdefault(val, []).append(i + 1)

    matches: List[str] = []
    diffs_map: Dict[str, List[Tuple[int, str, str, str]]] = {}

    for ref in refs:
        r_map, _ = _load_parameter_map(ref)
        diffs: List[Tuple[int, str, str, str]] = []

        # Report parameters present in current but different or missing in reference
        for key, new_val in s_map.items():
            old_val = r_map.get(key)
            if old_val is None:
                # Parameter missing in reference
                lineno = value_to_lineno.get(new_val, [None])[0]
                if lineno:
                    diffs.append((lineno, key, "(missing in reference)", new_val))
            elif old_val != new_val:
                lineno = value_to_lineno.get(new_val, [None])[0]
                if lineno:
                    diffs.append((lineno, key, old_val, new_val))

        # Report parameters present in reference but missing in current
        for key, old_val in r_map.items():
            if key not in s_map:
                # Parameter missing in current file
                diffs.append((None, key, old_val, "(missing in current)"))

        if not diffs:
            matches.append(ref.name)
        else:
            diffs_map[ref.name] = diffs
    # Print grouped results
    if matches:
        logging.info(f"✅ Exact matches: {', '.join(matches)}")
    if diffs_map:
        logging.warning(f"⚠️  Differences detected in: {', '.join(diffs_map.keys())}")
        for ref_name, diffs in diffs_map.items():
            logging.info(f"**{ref_name}**")
            for diff in diffs[:max_diffs]:
                lineno, key, old, new = diff
                if lineno is not None:
                    logging.info(f"  L{lineno:<4} {key:<24}: '{old}' → '{new}'")
                else:
                    logging.info(f"       {key:<24}: '{old}' → '{new}'")
            more = len(diffs) - max_diffs
            if more > 0:
                logging.info(f"  ...and {more} more differences...")

def _extract_sections(xml_path: Path) -> Dict:
    """
    Extract relevant sections from an XML file for comparison.

    Parameters:
    xml_path (Path): Path to the XML file.

    Returns:
    Dict: Dictionary of extracted sections, or empty dict if parsing fails.
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        logging.warning(f"⚠️  Malformed XML in '{xml_path}': {e}")
        return {}
    except Exception as e:
        logging.warning(f"⚠️  Error reading XML in '{xml_path}': {e}")
        return {}

    out: Dict = {}
    out.update(_extract_board_section(root))
    out['parameters'] = _extract_parameters_section(root)
    out['channels'] = _extract_channels_section(root)
    out.update(_extract_subtrees(root, (
        'acquisitionMemento', 'timeCorrelationMemento', 'virtualChannelsMemento'
    )))
    return out

def _extract_board_section(root: ET.Element) -> Dict[str, str]:
    """
    Extract board information from the XML root.

    Parameters:
    root (ET.Element): The root XML element.

    Returns:
    Dict[str, str]: Board fields and their values.
    """
    out = {}
    board = root.find('board')
    if board is not None:
        for tag in ('id', 'modelName', 'serialNumber', 'label'):
            el = board.find(tag)
            out[f'board.{tag}'] = (el.text or '').strip()
    return out

def _extract_parameters_section(root: ET.Element) -> Dict[str, str]:
    """
    Extract parameters from the XML root.

    Parameters:
    root (ET.Element): The root XML element.

    Returns:
    Dict[str, str]: Parameter keys and values.
    """
    params = {}
    for entry in root.findall(f'.//{PARAMETERS_TAG}/{ENTRY_TAG}'):
        key = entry.findtext(KEY_TAG, '').strip()
        val_el = entry.find(VALUE_TAG)
        if val_el is not None:
            nested = val_el.findtext(VALUE_TAG)
            val = nested if nested is not None else (val_el.text or '')
        else:
            val = ''
        params[key] = val.strip()
    return params

def _extract_channels_section(root: ET.Element) -> Dict[str, str]:
    """
    Extract channel information from the XML root.

    Parameters:
    root (ET.Element): The root XML element.

    Returns:
    Dict[str, str]: Channel IDs and their XML string.
    """
    chans = {}
    for ch in root.findall('.//virtualChannelsMemento/channel'):
        ch_id = ch.get('id')
        chans[ch_id] = ET.tostring(ch, encoding='unicode')
    return chans

def _extract_subtrees(root: ET.Element, sections: Tuple[str, ...]) -> Dict[str, str]:
    """
    Extract raw XML for specified subtrees.

    Parameters:
    root (ET.Element): The root XML element.
    sections (Tuple[str, ...]): Section names to extract.

    Returns:
    Dict[str, str]: Section names and their XML string.
    """
    out = {}
    for section in sections:
        el = root.find(section)
        out[section] = ET.tostring(el, encoding='unicode') if el is not None else ''
    return out


def _extract_simple_fields_from_subtree(
    xml_text: str,
    wanted_fields: Tuple[str, ...] = ('runId',)
) -> Dict[str, str]:
    """
    Extract simple fields from an XML subtree.

    Parameters:
    xml_text (str): XML text to parse.
    wanted_fields (Tuple[str, ...]): Fields to extract.

    Returns:
    Dict[str, str]: Mapping of field names to values.
    """
    if not xml_text:
        return {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {}
    out = {}
    for child in root:
        if child.tag in wanted_fields:
            out[child.tag] = (child.text or '').strip()
    return out

def _load_parameter_map(xml_path: Path) -> Tuple[Dict[str, str], List[str]]:
    """
    Parse the XML at xml_path and return a parameter map and raw file lines.

    Parameters:
    xml_path (Path): Path to the XML file.

    Returns:
    Tuple[Dict[str, str], List[str]]: Parameter map and list of file lines.
    """
    text = xml_path.read_text(encoding='utf-8').splitlines()
    try:
        # Use ET.parse for efficiency and reliability
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        logging.warning(f"⚠️  Malformed XML in '{xml_path}': {e}")
        return {}, text
    except Exception as e:
        logging.warning(f"⚠️  Error reading XML in '{xml_path}': {e}")
        return {}, text
    params: Dict[str, str] = {}
    for entry in root.findall(f'.//{PARAMETERS_TAG}/{ENTRY_TAG}'):
        key = entry.findtext(KEY_TAG, '').strip()
        val_el = entry.find(VALUE_TAG)
        if val_el is not None:
            nested = val_el.findtext(VALUE_TAG)
            val = (nested if nested is not None else val_el.text or '').strip()
        else:
            val = ''
        params[key] = val
    return params, text