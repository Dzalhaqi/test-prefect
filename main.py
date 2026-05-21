import os
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"

import cloudscraper
from prefect import flow, task
from models import get_session, RegulationMetadata
from scraper import parse_list_html, parse_detail_html

LIST_URL = "https://peraturan.bpk.go.id/Search?keywords=&tentang=&nomor="

@task(retries=2, retry_delay_seconds=5)
def fetch_url(url: str) -> str:
    """Fetches HTML content from a given URL using cloudscraper to bypass 403 Forbidden."""
    print(f"Fetching {url}...")
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
    print(f"Found {len(urls)} detail URLs.")
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
            print(f"Updating existing record: {data['detail_url']}")
            # Update fields
            reg.regulation_title = data.get('regulation_title')
            reg.regulation_type_number = data.get('regulation_type_number')
            reg.categories = data.get('categories')
            reg.status_info = data.get('status_info')
            reg.pdf_url = data.get('pdf_url')
            reg.abstract = data.get('abstract')
        else:
            print(f"Inserting new record: {data['detail_url']}")
            # Insert new
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
    except Exception as e:
        print(f"Error saving {data['detail_url']}: {e}")
        session.rollback()
    finally:
        session.close()

import urllib.parse

@flow(name="Prefetch Regulation Flow")
def prefetch_regulation_flow(
    limit: int = None,
    keywords: str = "",
    tentang: str = "",
    nomor: str = "",
    jenis: str = ""
):
    """
    Flow untuk melakukan prefetch data peraturan dari BPK JDIH.
    Anda bisa mengatur parameter pencarian di bawah ini melalui Prefect UI.
    """
    
    # 1. Bangun URL secara dinamis berdasarkan parameter dari UI
    base_search_url = "https://peraturan.bpk.go.id/Search"
    query_params = {
        "keywords": keywords,
        "tentang": tentang,
        "nomor": nomor,
        "jenis": jenis
    }
    
    # Menggabungkan URL dengan parameter (misal: ?keywords=pidana&tentang=&nomor=)
    query_string = urllib.parse.urlencode(query_params)
    list_url = f"{base_search_url}?{query_string}"
    print(f"Mulai prefetch dengan URL: {list_url}")
    
    # 2. Fetch list page
    list_html = fetch_url(list_url)
    
    # 3. Extract detail URLs
    detail_urls = parse_list(list_html)
    
    # Optional: limit number of items for testing
    if limit is not None:
        detail_urls = detail_urls[:limit]

    # 3. Process each detail URL
    for url in detail_urls:
        try:
            detail_html = fetch_url(url)
            
            # 4. Parse detail page
            metadata = parse_detail_html(detail_html, url)
            
            # 5. Save to DB (Upsert)
            check_db_and_save(metadata)
        except Exception as e:
            print(f"Failed to process {url}: {e}")

from prefect import serve

if __name__ == "__main__":
    print("Menyiapkan Deployment Perpu dan UU untuk local execution...")
    
    # Membuat konfigurasi deployment untuk Perpu
    deploy_perpu = prefetch_regulation_flow.to_deployment(
        name="Scrape-Perpu",
        parameters={"jenis": "6"},
        tags=["bpk", "perpu"],
        description="Scraping khusus data Perpu (jenis=6)"
    )

    # Membuat konfigurasi deployment untuk UU
    deploy_uu = prefetch_regulation_flow.to_deployment(
        name="Scrape-UU",
        parameters={"jenis": "8"},
        tags=["bpk", "uu"],
        description="Scraping khusus data UU (jenis=8)"
    )

    print("\n✅ Deployments berhasil dibuat!")
    print("Menjalankan worker lokal untuk kedua deployment sekaligus...")
    
    # Menjalankan KEDUA deployment tersebut dalam satu terminal
    serve(deploy_perpu, deploy_uu)
