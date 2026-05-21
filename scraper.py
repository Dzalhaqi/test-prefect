from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

BASE_URL = "https://peraturan.bpk.go.id"

def parse_list_html(html_str: str) -> list:
    """
    Parses the search/list page and returns a list of detail URLs.
    """
    soup = BeautifulSoup(html_str, 'lxml')
    # find all a tags that link to Details
    links = soup.find_all('a', href=re.compile(r'^/Details/'))
    
    urls = []
    seen = set()
    for a in links:
        href = a.get('href')
        if href and href not in seen:
            urls.append(urljoin(BASE_URL, href))
            seen.add(href)
    return urls

def parse_detail_html(html_str: str, detail_url: str) -> dict:
    """
    Parses the detail page HTML and returns a dictionary of regulation metadata.
    """
    soup = BeautifulSoup(html_str, 'lxml')
    
    # 1. Title and Type Number
    header_div = soup.find('div', class_='bg-gd-bpk-2')
    regulation_title = ""
    regulation_type_number = ""
    if header_div:
        h1 = header_div.find('h1')
        if h1:
            regulation_title = h1.get_text(strip=True)
        h4 = header_div.find('h4')
        if h4:
            regulation_type_number = h4.get_text(strip=True)

    # 2. Abstract / Materi Pokok
    abstract = ""
    # Find h4 with text containing "MATERI POKOK"
    materi_h4 = soup.find(lambda tag: tag.name == 'h4' and 'MATERI POKOK' in tag.text)
    if materi_h4:
        card_body = materi_h4.find_parent('div', class_='card-body')
        if card_body:
            p_tag = card_body.find('p')
            if p_tag:
                abstract = p_tag.get_text(strip=True)

    # 3. Metadata (Categories / Subjek)
    categories = []
    metadata_h4 = soup.find(lambda tag: tag.name == 'h4' and 'METADATA' in tag.text)
    if metadata_h4:
        card_body = metadata_h4.find_parent('div', class_='card-body')
        if card_body:
            # find all rows in metadata
            subjek_div = card_body.find('div', string=re.compile(r'^Subjek$'))
            if subjek_div:
                val_div = subjek_div.find_next_sibling('div')
                if val_div:
                    subjek_text = val_div.get_text(strip=True)
                    if subjek_text:
                        categories.append(subjek_text)
            
            bidang_div = card_body.find('div', string=re.compile(r'^Bidang$'))
            if bidang_div:
                val_div = bidang_div.find_next_sibling('div')
                if val_div:
                    bidang_text = val_div.get_text(strip=True)
                    if bidang_text:
                        categories.append(bidang_text)

    # 4. Status Info
    status_info = []
    status_h4 = soup.find(lambda tag: tag.name == 'h4' and 'STATUS' in tag.text)
    if status_h4:
        card_body = status_h4.find_parent('div', class_='card-body')
        if card_body:
            list_items = card_body.find_all('li')
            for li in list_items:
                status_info.append(li.get_text(separator=" ", strip=True))

    # 5. PDF URL
    pdf_url = ""
    download_link = soup.find('a', class_='download-file', href=re.compile(r'^/Download/'))
    if download_link:
        href = download_link.get('href')
        if href:
            pdf_url = urljoin(BASE_URL, href)

    return {
        "detail_url": detail_url,
        "regulation_title": regulation_title,
        "regulation_type_number": regulation_type_number,
        "categories": categories,
        "abstract": abstract,
        "status_info": status_info,
        "pdf_url": pdf_url
    }

if __name__ == "__main__":
    # For testing parsing manually
    pass
