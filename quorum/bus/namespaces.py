"""
quorum/bus/namespaces.py
------------------------
Provides access to the in-process ChromaDB (Moss) collections used as the
semantic bus.  A single EphemeralClient is created once at import time and
shared across the entire process — no HTTP hop, no server required.

Usage::

    from bus.namespaces import get_claims_namespace, get_findings_namespace

    claims_col = get_claims_namespace()
    findings_col = get_findings_namespace()
"""
import hashlib
import numpy as np
import chromadb
from chromadb import Collection
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from config.settings import settings


class FastFeatureEmbeddingFunction(EmbeddingFunction):
    """In-process sub-millisecond feature hashing embedding function.
    
    Generates 384-dimensional normalized dense vectors locally without
    external API calls or heavy neural network overhead on CPU.
    """

    def __init__(self, dim: int = 384):
        self.dim = dim

    def __call__(self, input: Documents) -> Embeddings:
        embeddings = []
        for text in input:
            vec = np.zeros(self.dim, dtype=np.float32)
            words = text.lower().split()
            if words:
                for w in words:
                    h = int(hashlib.md5(w.encode("utf-8")).hexdigest(), 16)
                    idx = h % self.dim
                    sign = 1.0 if ((h >> 8) & 1) else -1.0
                    vec[idx] += sign
                norm = np.linalg.norm(vec)
                if norm > 0:
                    vec /= norm
            embeddings.append(vec.tolist())
        return embeddings


_embedding_function = FastFeatureEmbeddingFunction(dim=384)

# Singleton EphemeralClient — one per process, shared by all modules.
_client: chromadb.EphemeralClient = chromadb.EphemeralClient()


def get_claims_namespace() -> Collection:
    """Return (or create) the ChromaDB collection used for Claim objects."""
    return _client.get_or_create_collection(
        name=settings.MOSS_CLAIMS_NAMESPACE,
        embedding_function=_embedding_function,
        metadata={"hnsw:space": "cosine"},
    )


def get_findings_namespace() -> Collection:
    """Return (or create) the ChromaDB collection used for Finding objects."""
    return _client.get_or_create_collection(
        name=settings.MOSS_FINDINGS_NAMESPACE,
        embedding_function=_embedding_function,
        metadata={"hnsw:space": "cosine"},
    )
