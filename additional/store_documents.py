# Example 1: Adding Integration with RAG (Retrieval Augmented Generation)
# store_documents.py

import json
from utils import get_minio_client
import io
import hashlib
import uuid
from datetime import datetime

def store_document_in_datalake(document_text, metadata=None):
    """
    Store a document in the data lake with appropriate metadata
    for RAG applications.
    """
    client = get_minio_client()
    bucket_name = "raw-zone"

    # Generate a unique ID for the document
    doc_id = str(uuid.uuid4())

    # Create document object with metadata
    document = {
        "id": doc_id,
        "text": document_text,
        "hash": hashlib.md5(document_text.encode()).hexdigest(),
        "created_at": datetime.now().isoformat(),
        "metadata": metadata or {}
    }

    # Convert to JSON
    document_json = json.dumps(document)
    document_bytes = document_json.encode('utf-8')

    # Upload to MinIO
    document_path = f"documents/{doc_id}.json"
    client.put_object(
        bucket_name,
        document_path,
        io.BytesIO(document_bytes),
        len(document_bytes),
        content_type="application/json"
    )

    print(f"Document stored at {bucket_name}/{document_path}")
    return doc_id