from __future__ import annotations

import logging
from typing import Dict, List

import pendulum
from airflow.sdk import dag, task

# Import only the components you're using
from src.parsers import combine_papers
from src.storage import save_raw_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dag(
    schedule="0 9 * * *",  # Daily at 9 AM UTC
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["papers", "parser", "minimal"],
)
def data_coordinator():
    """
    ### Minimal DAG: Paper Combiner & Saver

    Combines papers from multiple sources and saves them in raw format.

    Outputs:
    - data/raw/{YYYYMMDD}.csv: Raw scraped papers
    """

    @task
    def combine_papers_task(hf_papers: List[Dict], arxiv_papers: List[Dict], **context) -> List[Dict]:
        """Combine papers from all sources"""
        return combine_papers(hf_papers, arxiv_papers)

    @task
    def save_raw_data_task(papers: List[Dict], **context) -> str:
        """Save raw paper data to CSV file"""
        logical_date = context['ds']  # YYYY-MM-DD format
        return save_raw_data(papers, logical_date)

    hf_papers = []
    arxiv_papers = []

    combined = combine_papers_task(hf_papers, arxiv_papers)
    saved = save_raw_data_task(combined)


data_coordinator()
