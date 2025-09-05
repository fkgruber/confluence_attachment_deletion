#!/usr/bin/env python3
# confluence_update_attachment.py
# Update (or optionally create) a Confluence attachment using _publish.yml mapping

import os, re, sys, json, requests, typer, yaml
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlsplit
from requests.auth import HTTPBasicAuth

app = typer.Typer(add_completion=False, help="Update a Confluence attachment (figure) from a local file")

# ---- helpers
def env_or_die(k: str) -> str:
    v = os.environ.get(k)
    if not v:
        raise typer.Exit(code=2, message=f"ERROR: environment variable {k} is not set.")
    return v
def infer_base_url_from_page_url(page_url: str) -> Optional[str]:
    if not page_url: return None
    parts = urlsplit(page_url)
    ctx = ""
    for c in ("/wiki", "/confluence"):
        if parts.path.startswith(c): ctx = c; break
    return f"{parts.scheme}://{parts.netloc}{ctx}" if parts.scheme and parts.netloc else None
def _norm(p: str) -> str:
    return os.path.basename(os.path.normpath(p))
def get_page_from_publish(publish_path: Path, source_qmd: str) -> Dict[str, Optional[str]]:
    with publish_path.open("r") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, list):
        raise typer.Exit(code=2, message=f"Unexpected structure in {publish_path}: expected a top-level list.")
    target = _norm(source_qmd)
    for entry in data:
        if not isinstance(entry, dict): continue
        if _norm(entry.get("source", "")) != target: continue
        cfg = entry.get("confluence")
        item = (cfg[0] if isinstance(cfg, list) and cfg else cfg) if cfg else None
        if not isinstance(item, dict):
            raise typer.Exit(code=2, message=f"No 'confluence' config for source {source_qmd} in {publish_path}.")
        page_id = str(item.get("id") or "").strip()
        page_url = item.get("url")
        if not page_id and page_url:
            m = re.search(r"/pages/(\d+)", page_url)
            if m: page_id = m.group(1)
        if not page_id:
            raise typer.Exit(code=2, message=f"Could not find page id for {source_qmd} in {publish_path}.")
        return {"id": page_id, "url": page_url}
    raise typer.Exit(code=2, message=f"Source {source_qmd} not found in {publish_path}.")
def make_auth(email: Optional[str], token: Optional[str]) -> HTTPBasicAuth:
    em = email or env_or_die("CONFLUENCE_EMAIL")
    tk = token or env_or_die("CONFLUENCE_API_TOKEN")
    return HTTPBasicAuth(em, tk)
def resolve_base_url(cli_base_url: Optional[str], page_url: Optional[str]) -> str:
    base = cli_base_url or os.environ.get("CONFLUENCE_BASE_URL") or infer_base_url_from_page_url(page_url or "")
    if not base:
        raise typer.Exit(code=2, message="Cannot determine CONFLUENCE_BASE_URL (pass --base-url or set env, or include url in _publish.yml).")
    return base.rstrip("/")
def get_headers(no_check: bool=True) -> Dict[str, str]:
    h = {}
    if no_check: h["X-Atlassian-Token"] = "nocheck"  # required for multipart upload/update
    return h

# ---- API helpers
def lookup_attachment_by_name(base_url: str, page_id: str, auth: HTTPBasicAuth, filename: str) -> Optional[Dict[str, Any]]:
    """Return the attachment content object for a given filename (latest version if multiple)."""
    url = f"{base_url}/rest/api/content/{page_id}/child/attachment"
    r = requests.get(url, auth=auth, params={"filename": filename, "expand": "version,extensions,metadata"})
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results: return None
    # Pick the highest version if multiple entries somehow exist
    results.sort(key=lambda a: (a.get("version") or {}).get("number", 0), reverse=True)
    return results[0]
def update_attachment(base_url: str, page_id: str, attachment_id: str, upload_name: str, file_path: Path, auth: HTTPBasicAuth, comment: Optional[str]=None) -> Dict[str, Any]:
    """POST new data for an existing attachment ID; increments version."""
    url = f"{base_url}/rest/api/content/{page_id}/child/attachment/{attachment_id}/data"
    params = {}
    if comment: params["comment"] = comment  # optional version comment
    with file_path.open("rb") as f:
        files = {"file": (upload_name, f)}
        r = requests.post(url, auth=auth, headers=get_headers(), params=params, files=files)
    if r.status_code != 200:
        raise typer.Exit(code=1, message=f"Update failed (HTTP {r.status_code}): {r.text}")
    return r.json()
def create_attachment(base_url: str, page_id: str, upload_name: str, file_path: Path, auth: HTTPBasicAuth, comment: Optional[str]=None) -> Dict[str, Any]:
    """POST a new attachment to the page."""
    url = f"{base_url}/rest/api/content/{page_id}/child/attachment"
    params = {}
    if comment: params["comment"] = comment
    with file_path.open("rb") as f:
        files = {"file": (upload_name, f)}
        r = requests.post(url, auth=auth, headers=get_headers(), params=params, files=files)
    if r.status_code not in (200, 201):
        raise typer.Exit(code=1, message=f"Create failed (HTTP {r.status_code}): {r.text}")
    return r.json()

# ---- CLI
@app.command("update", help="Update an existing attachment on the page mapped from the .qmd in _publish.yml")
def update_cmd(
    source: Path = typer.Argument(..., help="Source .qmd (key in _publish.yml)"),
    file: Path = typer.Argument(..., exists=True, readable=True, help="Local file to upload (new figure)"),
    publish: Path = typer.Option(Path("_publish.yml"), "--publish", "-p", help="Path to _publish.yml"),
    attachment_name: Optional[str] = typer.Option(None, "--name", "-n", help="Attachment filename to update on Confluence (defaults to basename of --file)"),
    attachment_id: Optional[str] = typer.Option(None, "--attachment-id", help="Attachment ID to update (skips filename lookup)"),
    comment: Optional[str] = typer.Option(None, "--comment", help="Optional version comment"),
    create_if_missing: bool = typer.Option(False, "--create-if-missing/--fail-if-missing", help="Create the attachment if not found by name"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would happen without uploading"),
    base_url: Optional[str] = typer.Option(None, "--base-url", help="Override CONFLUENCE_BASE_URL"),
    email: Optional[str] = typer.Option(None, "--email", help="Override CONFLUENCE_EMAIL"),
    token: Optional[str] = typer.Option(None, "--token", help="Override CONFLUENCE_API_TOKEN"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose HTTP debug output"),
):
    info = get_page_from_publish(publish, str(source))
    auth = make_auth(email, token)
    b = resolve_base_url(base_url, info["url"])
    upload_name = attachment_name or file.name

    if verbose:
        typer.echo(f"[debug] page_id={info['id']} base_url={b} upload_name={upload_name}")
        if attachment_id: typer.echo(f"[debug] attachment_id (provided)={attachment_id}")

    # Resolve attachment
    att_obj = None
    if attachment_id:
        # Minimal fetch for context/title
        url = f"{b}/rest/api/content/{attachment_id}"
        r = requests.get(url, auth=auth, params={"expand": "version"})
        if r.status_code == 200:
            att_obj = r.json()
            # Prefer server title for upload name to avoid accidental rename
            upload_name = att_obj.get("title") or upload_name
        else:
            raise typer.Exit(code=2, message=f"Attachment id {attachment_id} not accessible (HTTP {r.status_code}).")
    else:
        att_obj = lookup_attachment_by_name(b, info["id"], auth, upload_name)

    if not att_obj:
        if not create_if_missing:
            raise typer.Exit(code=3, message=f"Attachment '{upload_name}' not found on page {info['id']}. Use --create-if-missing to upload new.")
        if dry_run:
            typer.echo(f"[dry-run] Would CREATE new attachment '{upload_name}' on page {info['id']} from '{file}'.")
            raise typer.Exit()
        res = create_attachment(b, info["id"], upload_name, file, auth, comment=comment)
        # API returns an object with 'results' list for create; normalize output
        created = (res.get("results") or [res])[0]
        vnum = ((created.get("version") or {}).get("number"))
        did  = created.get("id")
        typer.echo(f"✅ Created attachment '{upload_name}' as ID {did} (version {vnum}).")
        return

    # We have an existing attachment
    aid = att_obj.get("id")
    current_title = att_obj.get("title") or upload_name
    current_ver = (att_obj.get("version") or {}).get("number")
    if dry_run:
        typer.echo(f"[dry-run] Would UPDATE attachment [{aid}] '{current_title}' on page {info['id']} to new file '{file}'. Current version={current_ver}.")
        raise typer.Exit()

    # Use the existing title for the upload part to avoid unintended rename
    res = update_attachment(b, info["id"], aid, current_title, file, auth, comment=comment)
    # Update endpoint returns the updated attachment object directly
    new_ver = (res.get("version") or {}).get("number")
    size = (res.get("extensions") or {}).get("fileSize")
    links = res.get("_links") or {}
    download = f"{b}{links.get('download')}" if links.get("download") else None
    typer.echo(f"✅ Updated [{aid}] '{current_title}' → version {new_ver} ({size} bytes).")
    if download: typer.echo(f"   Download: {download}")

# ---- entry
def main(): app()
if __name__ == "__main__": main()
