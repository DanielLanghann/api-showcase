from __future__ import annotations
import asyncio
import aiohttp
import sys
import os
from dataclasses import dataclass
from typing import List, Optional
import argparse
from datetime import datetime, timezone
from decouple import config

document_details_url = config("STAGE_DOCUMENT_DETAILS_URL")
default_scope = config("DEFAULT_SCOPE")

from ..rest_importer.auth import get_token
from .list_documents import list_documents


@dataclass
class DeleteResult:
    document_id: str
    success: bool
    status: Optional[int] = None
    error: Optional[str] = None
    duration_ms: Optional[int] = None

async def delete_document_export(access_token: str, document_id: str, session: aiohttp.ClientSession, scope: str, document_class_regex: Optional[str]) -> DeleteResult:
    """Delete export information for a single document.

    Adds scope (and optional document_class_regex) as query params mirroring other endpoints.
    Retries once on 401/403 after refreshing token (caller must update access_token externally).
    """
    params = {"scope": scope}
    if document_class_regex:
        params["document_class_regex"] = document_class_regex
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    url = document_details_url.replace(":document_id", document_id)
    # Use timezone.utc instead of datetime.UTC for compatibility with Python versions <3.11
    start = datetime.now(timezone.utc)
    try:
        async with session.delete(url, headers=headers, params=params) as response:
            status = response.status
            if status < 300:
                return DeleteResult(document_id=document_id, success=True, status=status, duration_ms=int((datetime.now(timezone.utc)-start).total_seconds()*1000))
            else:
                # Try extract body for diagnostics
                try:
                    body = await response.text()
                except Exception:
                    body = '<no body>'
                return DeleteResult(document_id=document_id, success=False, status=status, error=f"Unexpected status {status}: {body}", duration_ms=int((datetime.now(timezone.utc)-start).total_seconds()*1000))
    except aiohttp.ClientError as e:
        return DeleteResult(document_id=document_id, success=False, error=str(e), duration_ms=int((datetime.now(timezone.utc)-start).total_seconds()*1000))

async def delete_all_document_exports(scope: str = default_scope, confirm: bool = False, concurrency: int = 10, dry_run: bool = False, document_class_regex: Optional[str] = None) -> List[DeleteResult]:
    """List all documents in given scope and delete export info for each.

    Parameters:
        scope: Document scope (e.g., Production, Training)
        confirm: Must be True to actually perform deletions unless dry_run
        concurrency: Max number of simultaneous delete requests
        dry_run: If True, only prints planned deletions without performing
        document_class_regex: Filter pattern passed to list_documents
    """
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")

    access_token = await get_token()
    print(f"🔐 Access token acquired (first 50 chars): {access_token[:50]}...")

    print(f"📄 Listing documents (scope={scope}, regex={document_class_regex})...")
    documents = await list_documents(access_token, scope=scope, document_class_regex=document_class_regex)

    if not isinstance(documents, list):
        raise RuntimeError(f"Unexpected documents payload type: {type(documents)}; expected list of document IDs")

    total = len(documents)
    print(f"Found {total} documents.")

    if total == 0:
        return []

    if dry_run:
        print("🧪 Dry run: would delete export info for these documents:")
        for doc_id in documents:
            print(f"  - {doc_id}")
        print("Dry run complete. No deletions performed.")
        return []

    if not confirm:
        print("❌ Refusing to proceed: --confirm flag required (or use --dry-run to preview)")
        return []

    semaphore = asyncio.Semaphore(concurrency)
    results: List[DeleteResult] = []

    async with aiohttp.ClientSession() as session:
        async def worker(doc_id: str):
            async with semaphore:
                res = await delete_document_export(access_token, doc_id, session, scope=scope, document_class_regex=document_class_regex)
                if res.status in (401, 403) and 'Not authenticated' in (res.error or ''):
                    # Attempt single token refresh then retry once
                    print(f"🔄 Auth retry for {doc_id} due to {res.status} ({res.error}). Refreshing token...")
                    try:
                        new_token = await get_token()
                        nonlocal_access_token_holder[0] = new_token  # update shared token
                        res = await delete_document_export(new_token, doc_id, session, scope=scope, document_class_regex=document_class_regex)
                    except Exception as refresh_err:
                        print(f"❌ Token refresh failed: {refresh_err}")
                # Print progress line
                if res.success:
                    print(f"✅ Deleted export info for {doc_id} (status={res.status}, {res.duration_ms}ms)")
                else:
                    print(f"⚠️ Failed to delete {doc_id}: {res.error} (status={res.status})")
                results.append(res)

        # mutable holder for token so workers can update (list used for mutability in closure)
        nonlocal_access_token_holder = [access_token]

        tasks = [asyncio.create_task(worker(doc_id)) for doc_id in documents]
        await asyncio.gather(*tasks)

    # Summary
    success_count = sum(1 for r in results if r.success)
    fail_count = total - success_count
    print("\n====== Deletion Summary ======")
    print(f"Total documents: {total}")
    print(f"Successful deletions: {success_count}")
    print(f"Failures: {fail_count}")
    if fail_count:
        print("Failed document IDs:")
        for r in results:
            if not r.success:
                print(f" - {r.document_id}: {r.error}")

    return results

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bulk delete export information for documents.")
    parser.add_argument("--scope", default=default_scope, help=f"Scope to list documents from (default: {default_scope})")
    parser.add_argument("--confirm", action="store_true", help="Actually perform deletions (required unless --dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Preview deletions without performing")
    parser.add_argument("--concurrency", type=int, default=10, help="Max concurrent delete operations (default: 10)")
    parser.add_argument("--document-class-regex", type=str, default=None, help="Filter documents by class regex")
    return parser.parse_args(argv)

async def async_main(args: argparse.Namespace):
    return await delete_all_document_exports(scope=args.scope, confirm=args.confirm, concurrency=args.concurrency, dry_run=args.dry_run, document_class_regex=args.document_class_regex)

def main(argv: Optional[List[str]] = None):
    args = parse_args(argv)
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("Interrupted by user.")
        return 2
    except Exception as e:
        print(f"❌ Unhandled error: {e}")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
