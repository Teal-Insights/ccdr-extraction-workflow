from typing import List, Dict, Any

def classify_download_links(links: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Classifies download links based on a set of predefined rules.
    Adds 'type' and 'to_download' keys to each link dictionary.

    Args:
        links: A list of link dictionaries, each must have a 'text' key.

    Returns:
        The same list of links, with added classification keys.
    """
    if not links:
        return []

    # Rule: First link is a candidate for 'main'
    links[0]['type'] = 'main'
    
    for i, link in enumerate(links):
        link_text = link.get('text', '')

        # Rule for 'type'
        if i > 0: # a 'main' may have already been assigned
             link['type'] = 'supplemental'
        if link_text.lower().startswith("full report"):
            link['type'] = 'main'
        
        # Rule for 'to_download'
        if "english" in link_text.lower() and "text" not in link_text.lower():
            link['to_download'] = True
        else:
            link['to_download'] = False
            
    return links 