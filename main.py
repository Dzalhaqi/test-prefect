from enum import Enum
import os

os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"

import cloudscraper
import urllib.parse
from prefect import flow, task
from models import get_session, RegulationMetadata
from scraper import parse_list_html, parse_detail_html

@task(retries=2, retry_delay_seconds=5)
def fetch_url(url: str) -> str:
    """Fetches HTML content from a given URL using cloudscraper to bypass 403 Forbidden."""
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    response = scraper.get(url, timeout=15)
    response.raise_for_status()
    return response.text

@task
def parse_list(html_str: str) -> list:
    """Parses the list page to get detail URLs."""
    urls = parse_list_html(html_str)
    return urls

@task
def check_db_and_save(data: dict):
    """
    Checks if the regulation exists in the DB by detail_url.
    If it exists, updates it. Otherwise, inserts it.
    """
    session = get_session()
    try:
        reg = session.query(RegulationMetadata).filter_by(detail_url=data['detail_url']).first()
        
        if reg:
            reg.regulation_title = data.get('regulation_title')
            reg.regulation_type_number = data.get('regulation_type_number')
            reg.categories = data.get('categories')
            reg.status_info = data.get('status_info')
            reg.pdf_url = data.get('pdf_url')
            reg.abstract = data.get('abstract')
        else:
            reg = RegulationMetadata(
                detail_url=data['detail_url'],
                regulation_title=data.get('regulation_title'),
                regulation_type_number=data.get('regulation_type_number'),
                categories=data.get('categories'),
                status_info=data.get('status_info'),
                pdf_url=data.get('pdf_url'),
                abstract=data.get('abstract')
            )
            session.add(reg)
            
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()


from typing import Literal

@flow(name="Prefetch Regulation Flow")
def prefetch_regulation_flow(
    limit: int = None,
    keywords: str = "",
    tentang: str = "",
    nomor: str = "",
    jenis: Literal["Semua", "Undang-Undang", "Perpu", "Peraturan Pemerintah", "Peraturan Presiden", "Peraturan Menteri", "Peraturan BPK"] = "Semua"
):
    jenis_map = {
        "Semua": "",
        "Undang-Undang": "8",
        "Perpu": "6",
        "Peraturan Pemerintah": "5",
        "Peraturan Presiden": "4",
        "Peraturan Menteri": "11",
        "Peraturan BPK": "10"
    }
    kode_jenis = jenis_map.get(jenis, "")

    base_search_url = "https://peraturan.bpk.go.id/Search"
    query_params = {
        "keywords": keywords,
        "tentang": tentang,
        "nomor": nomor,
        "jenis": kode_jenis
    }
    
    query_string = urllib.parse.urlencode(query_params)
    list_url = f"{base_search_url}?{query_string}"
    
    list_html = fetch_url(list_url)
    detail_urls = parse_list(list_html)
    
    if limit is not None:
        detail_urls = detail_urls[:limit]

    for url in detail_urls:
        try:
            detail_html = fetch_url(url)
            metadata = parse_detail_html(detail_html, url)
            check_db_and_save(metadata)
        except Exception:
            pass

if __name__ == "__main__":
    repo_url = "https://github.com/Dzalhaqi/test-prefect.git"
    
    prefetch_regulation_flow.from_source(
        source=repo_url,
        entrypoint="main.py:prefetch_regulation_flow"
    ).deploy(
        name="Scrape-Perpu",
        work_pool_name="Perpu",
        parameters={"jenis": "Perpu"},
        tags=["bpk", "perpu"]
    )

    prefetch_regulation_flow.from_source(
        source=repo_url,
        entrypoint="main.py:prefetch_regulation_flow"
    ).deploy(
        name="Scrape-UU",
        work_pool_name="UU",
        parameters={"jenis": "Undang-Undang"},
        tags=["bpk", "uu"]
    )
