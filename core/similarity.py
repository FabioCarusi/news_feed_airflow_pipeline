"""Custom similarity search functions"""

import math
from typing import Sequence, Any
from .store_news import ArticleRepository


def cosine_sim(a: Sequence[float], b: Sequence[float]) -> float:
    """
    Calculate the cosine similarity between two vectors.
    
    Args:
        a: Sequence[float]
        b: Sequence[float]
        
    Returns:
        float: The cosine similarity.
    """
    dot = sum(x*y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x*x for x in a))
    norm_b = math.sqrt(sum(y*y for y in b))
    return dot / (norm_a * norm_b)

def search_similar_articles(repo: ArticleRepository,
                            query_embedding: Sequence[float],
                            top_k: int = 5) -> list[dict[str, Any]]:
    """
    Search for similar articles.
    
    Args:
        repo: ArticleRepository
        query_embedding: Sequence[float]
        top_k: int
        
    Returns:
        list[dict[str, Any]]: List of similar articles.
    """
    all_embs = repo.get_all_embeddings()
    if not all_embs:
        return []

    scored: list[tuple[float, int]] = []
    qe = list(query_embedding)
    for article_id, emb in all_embs:
        score = cosine_sim(qe, emb)
        scored.append((score, article_id))

    scored.sort(reverse=True)
    top = scored[:top_k]
    top_ids = [aid for score, aid in top]
    articles = repo.get_articles_by_ids(top_ids)

    # add score_map to results
    score_map = {aid: score for score, aid in scored[:top_k]}
    for a in articles:
        a["similarity_score"] = score_map.get(a["id"], 0.0)
    return articles