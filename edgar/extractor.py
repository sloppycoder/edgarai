import logging

from google.cloud import bigquery

import config

from .filing import SECFiling

logger = logging.getLogger(__name__)


def chunk_filing(cik: str, idx_filename: str, form_type: str) -> int:
    filing = SECFiling(cik, idx_filename)
    html_filename = filing.get_doc_by_type(form_type)[0]
    if not html_filename:
        return 0

    n_chunks = filing.save_chunked_texts("485BPOS")
    return n_chunks


def find_most_relevant_chunks(cik: str, accession_number: str) -> list[str]:
    chunk_distances = _query_for_chunk_distances(cik, accession_number)
    relevance_scores = _calculate_relevance(chunk_distances)

    # select top 3 chunks, use the first one and the next one if they are adjacent
    top_chunks = [chunk_num for chunk_num, _, _, _ in relevance_scores[:3]]
    selected_chunks = []
    if top_chunks:
        selected_chunks.append(top_chunks[0])
        if len(top_chunks) > 1:
            if abs(top_chunks[0] - top_chunks[1]) == 1:
                selected_chunks.append(top_chunks[1])
        if len(top_chunks) > 2:
            if abs(top_chunks[0] - top_chunks[2]) == 1:
                selected_chunks.append(top_chunks[2])

        selected_chunks = list(set(selected_chunks))
        selected_chunks.sort()
        return selected_chunks

    return []


def _query_for_chunk_distances(cik: str, accession_number: str):
    query = rf"""
            SELECT
                base.chunk_num,
                query.content AS query,
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
                TABLE `{config.dataset_id}.search_phrase`,
                top_k => 3,
                distance_type => 'COSINE',
                options => '{{"use_brute_force":true}}'
            )
        """

    with bigquery.Client() as bq_client:
        query_job = bq_client.query(query)
        results = query_job.result()

        chunk_distances = {}
        for row in results:
            chunk_num = row["chunk_num"]
            distance = row["distance"]
            if chunk_num not in chunk_distances:
                chunk_distances[chunk_num] = []
            chunk_distances[chunk_num].append(distance)

        return chunk_distances


def _calculate_relevance(chunk_distances):
    relevance_scores = []
    for chunk_num, distances in chunk_distances.items():
        frequency = len(distances)
        avg_distance = sum(distances) / frequency
        score = frequency / (1 + avg_distance)
        relevance_scores.append((chunk_num, frequency, avg_distance, score))
    # Sort by score in descending order
    relevance_scores.sort(key=lambda x: x[3], reverse=True)
    return relevance_scores
