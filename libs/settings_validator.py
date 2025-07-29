# libs/settings_validator.py

import xml.etree.ElementTree as ET
from pathlib import Path
import logging
from typing import Dict, List, Tuple
import difflib


IGNORE_TAG_RUN_ID = 'runId'
PARAMETERS_TAG = 'parameters'
ENTRY_TAG = 'entry'
KEY_TAG = 'key'
VALUE_TAG = 'value'

def _extract_sections(xml_path: Path) -> Dict:
    """
    Extract relevant sections from an XML file for comparison.

    Parameters:
    xml_path (Path): Path to the XML file.

    Returns:
    Dict: Dictionary of extracted sections.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()
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
    for entry in root.findall('.//parameters/entry'):
        key = entry.findtext('key', '').strip()
        val_el = entry.find('value')
        if val_el is not None:
            nested = val_el.findtext('value')
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


def _extract_simple_fields_from_subtree(xml_text: str, wanted_fields=('runId',)) -> Dict[str,str]:
    """
    Extract simple fields from an XML subtree.

    Parameters:
    xml_text (str): XML text to parse.
    wanted_fields (tuple): Fields to extract.

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


def validate_against_references(current_path: str, config_folder: str) -> List[str]:
    """
    Compare current settings.xml to each reference XML in config_folder.
    Returns only short summary lines for all detected differences.

    Parameters:
    current_path (str): Path to the current settings.xml.
    config_folder (str): Path to the folder with reference XMLs.

    Returns:
    List[str]: List of summary lines for differences.
    """
    curr = Path(current_path)
    refs = list(Path(config_folder).glob('*.xml'))
    if not curr.exists() or not refs:
        return []

    curr_sec = _extract_sections(curr)
    out: List[str] = []

    for ref in refs:
        ref_sec = _extract_sections(ref)
        diffs: List[str] = []

        # Board + parameters
        for key in ('board.id','board.modelName','board.serialNumber','board.label'):
            a = curr_sec.get(key, '')
            b = ref_sec.get(key, '')
            if a != b:
                diffs.append(f"[{ref.name}] {key} differs: {b!r} → {a!r}")

        for k, v in curr_sec['parameters'].items():
            rv = ref_sec['parameters'].get(k)
            if rv is None:
                diffs.append(f"[{ref.name}] parameter '{k}' missing in reference")
            elif rv != v:
                diffs.append(f"[{ref.name}] parameter '{k}' differs: {rv!r} → {v!r}")

        # Channels (only report missing)
        for ch_id in curr_sec['channels']:
            if ch_id not in ref_sec['channels']:
                diffs.append(f"[{ref.name}] channel id={ch_id} missing")

        # Subtree simple fields (e.g. runId)
        for section in ('acquisitionMemento','timeCorrelationMemento','virtualChannelsMemento'):
            ref_fields = _extract_simple_fields_from_subtree(ref_sec.get(section,''))
            cur_fields = _extract_simple_fields_from_subtree(curr_sec.get(section,''))
            for fld, old_val in ref_fields.items():
                new_val = cur_fields.get(fld, '')
                if old_val != new_val:
                    diffs.append(
                        f"[{ref.name}] {section}.{fld} differs: {old_val!r} → {new_val!r}"
                    )

        if diffs:
            out.append(f"=== Diffs vs {ref.name} ===")
            out.extend(diffs)
            out.append("")

    return out


def report_reference_comparison(
    settings_path: str,
    config_folder: str,
    ignore_tag: str = 'runId',
    max_diffs: int = 10
) -> None:
    """
    Compare settings_path to all XMLs in config_folder and print a summary of differences.

    Parameters:
    settings_path (str): Path to the settings.xml file.
    config_folder (str): Path to the folder with reference XMLs.
    ignore_tag (str): XML tag to ignore in comparison.
    max_diffs (int): Maximum number of diff lines to print per reference.
    """
    settings_file = Path(settings_path)
    config_dir    = Path(config_folder)

    if not settings_file.is_file():
        logging.warning(f"⚠️ Settings compare: '{settings_path}' not found.")
        return
    if not config_dir.is_dir():
        logging.warning(f"⚠️ Settings compare: config folder '{config_folder}' not found.")
        return

    refs = sorted(config_dir.glob('*.xml'))
    if not refs:
        logging.warning(f"⚠️ Settings compare: no reference XMLs in '{config_folder}'.")
        return

    # read & filter settings
    try:
        lines = settings_file.read_text(encoding='utf-8').splitlines()
    except Exception as e:
        logging.warning(f"⚠️ Settings compare: cannot read '{settings_path}': {e}")
        return
    filtered = [ln for ln in lines if ignore_tag not in ln]

    exact = []
    for ref in refs:
        ref_lines = []
        try:
            ref_lines = ref.read_text(encoding='utf-8').splitlines()
        except Exception:
            logging.warning(f"⚠️ Settings compare: cannot read '{ref.name}' – skipping.")
            continue
        ref_filtered = [ln for ln in ref_lines if ignore_tag not in ln]

        diffs = list(difflib.unified_diff(
            ref_filtered, filtered,
            fromfile=ref.name, tofile=settings_file.name, lineterm=''
        ))
        if not diffs:
            exact.append(ref.name)
        else:
            logging.info(f"\nDiff vs {ref.name}:")
            for line in diffs[:max_diffs]:
                logging.info(line)
            more = len(diffs) - max_diffs
            if more > 0:
                logging.info(f"...and {more} more differences...")
    if exact:
        logging.info(f"\n✅ Exact match with: {', '.join(exact)}")
    else:
        logging.warning("\n⚠️ No exact matches found.")

def _load_parameter_map(xml_path: Path) -> Tuple[dict, List[str]]:
    """
    Parse the XML at xml_path and return a parameter map and raw file lines.

    Parameters:
    xml_path (Path): Path to the XML file.

    Returns:
    Tuple[dict, List[str]]: Parameter map and list of file lines.
    """
    text = xml_path.read_text(encoding='utf-8').splitlines()
    tree = ET.fromstring("\n".join(text))
    params = {}
    for entry in tree.findall('.//parameters/entry'):
        key = entry.findtext('key','').strip()
        # value might be nested <value><value>…</value></value>
        val_el = entry.find('value')
        if val_el is not None:
            nested = val_el.findtext('value')
            val = (nested if nested is not None else val_el.text or '').strip()
        else:
            val = ''
        params[key] = val
    return params, text

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

    # Pre‐checks
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

    matches: List[str] = []
    diffs_map: dict[str, List[tuple[int, str, str, str]]] = {}

    for ref in refs:
        r_map, _ = _load_parameter_map(ref)
        diffs: List[tuple[int, str, str, str]] = []

        # Report parameters present in current but different or missing in reference
        for key, new_val in s_map.items():
            old_val = r_map.get(key)
            if old_val is None:
                # Parameter missing in reference
                lineno = next(
                    (i+1 for i, ln in enumerate(s_lines)
                     if f">{new_val}</value>" in ln),
                    None
                )
                if lineno:
                    diffs.append((lineno, key, "(missing in reference)", new_val))
            elif old_val != new_val:
                lineno = next(
                    (i+1 for i, ln in enumerate(s_lines)
                     if f">{new_val}</value>" in ln),
                    None
                )
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

