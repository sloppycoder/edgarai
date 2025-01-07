import logging

from google.cloud import bigquery

import config
from algo import most_relevant_chunks, relevance_by_appearance, relevance_by_distance

from .filing import SECFiling

logger = logging.getLogger(__name__)


def chunk_filing(cik: str, idx_filename: str, form_type: str) -> int:
    filing = SECFiling(cik, idx_filename)
    html_filename = filing.get_doc_by_type(form_type)[0]
    if not html_filename:
        return 0

    n_chunks = filing.save_chunked_texts("485BPOS")
    return n_chunks


def find_most_relevant_chunks(
    cik: str, access_number: str, method: str = "distance"
) -> list[str]:
    chunk_distances = _query_for_chunk_distances(cik, access_number)

    if method == "distance":
        relevance_scores = relevance_by_distance(chunk_distances)
    else:
        relevance_scores = relevance_by_appearance(chunk_distances)

    selected_chunks = most_relevant_chunks(relevance_scores)
    return selected_chunks


def _query_for_chunk_distances(cik: str, accession_number: str):
    query = rf"""
            SELECT
                base.chunk_num,
                distance
            FROM
            VECTOR_SEARCH(
                (
                SELECT *
                FROM `{config.dataset_id}.filing_text_chunks_embedding`
                WHERE cik = '{cik}'
                AND accession_number = '{accession_number}'
                ),
                'ml_generate_embedding_result',
                TABLE `{config.dataset_id}.search_phrases`,
                top_k => 3,
                distance_type => 'COSINE',
                options => '{{"use_brute_force":true}}'
            )
        """

    with bigquery.Client() as bq_client:
        query_job = bq_client.query(query)
        results = query_job.result()
        elapsed_t = query_job.ended - query_job.started
        logger.info(
            f"relevant_chunks query for {cik},{accession_number} took {elapsed_t.total_seconds()} seconds"  # noqa E501
        )

        chunk_distances = {}
        for row in results:
            chunk_num = row["chunk_num"]
            distance = row["distance"]
            if chunk_num not in chunk_distances:
                chunk_distances[chunk_num] = []
            chunk_distances[chunk_num].append(distance)

        return chunk_distances
