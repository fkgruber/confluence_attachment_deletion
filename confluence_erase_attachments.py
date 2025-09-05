#!/usr/bin/env python3
# confluence_attachments.py
# Script to list/delete Confluence attachments using _publish.yml
# Author: Fred Gruber

import os, re, json, requests, typer, yaml
from typing import Optional, Iterable, Dict, Any, List
from urllib.parse import urlsplit
from requests.auth import HTTPBasicAuth
from pathlib import Path

app = typer.Typer(add_completion=False, help="Manage Confluence page attachments via _publish.yml")

# ---- helpers
def env_or_die(k: str) -> str:
    v = os.environ.get(k)
    if not v: raise typer.Exit(code=2, message=f"ERROR: environment variable {k} is not set.")
    return v
#
def infer_base_url_from_page_url(page_url: str) -> Optional[str]:
    if not page_url: return None
    parts = urlsplit(page_url)
    ctx = ""
    for c in ("/wiki", "/confluence"):
        if parts.path.startswith(c): ctx = c; break
    return f"{parts.scheme}://{parts.netloc}{ctx}" if parts.scheme and parts.netloc else None
#
def _norm(p: str) -> str:
    return os.path.basename(os.path.normpath(p))
#
def get_page_from_publish(publish_path: Path, source_qmd: str) -> Dict[str, Optional[str]]:
    with publish_path.open("r") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, list):
        raise typer.Exit(code=2, message=f"Unexpected structure in {publish_path}: expected a top-level list.")
    target = _norm(source_qmd)
    for entry in data:
        if not isinstance(entry, dict): continue
        if _norm(entry.get("source","")) != target: continue
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
#
def make_auth(email: Optional[str], token: Optional[str]) -> HTTPBasicAuth:
    em = email or env_or_die("CONFLUENCE_EMAIL")
    tk = token or env_or_die("CONFLUENCE_API_TOKEN")
    return HTTPBasicAuth(em, tk)
#
def resolve_base_url(cli_base_url: Optional[str], page_url: Optional[str]) -> str:
    base = cli_base_url or os.environ.get("CONFLUENCE_BASE_URL") or infer_base_url_from_page_url(page_url or "")
    if not base: raise typer.Exit(code=2, message="Cannot determine CONFLUENCE_BASE_URL (pass --base-url or set env, or include url in _publish.yml).")
    return base.rstrip("/")
#
def iter_attachments(base_url: str, page_id: str, auth: HTTPBasicAuth, limit: int = 50) -> Iterable[dict]:
    start = 0
    while True:
        url = f"{base_url}/rest/api/content/{page_id}/child/attachment"
        r = requests.get(url, auth=auth, params={"start": start, "limit": limit, "expand": "version,extensions,metadata"})
        r.raise_for_status()
        data = r.json()
        for att in data.get("results", []): yield att
        if (data.get("_links") or {}).get("next"): start += limit
        else: break
#
def list_page_attachments(base_url: str, page_id: str, auth: HTTPBasicAuth, filename_contains: Optional[str] = None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for att in iter_attachments(base_url, page_id, auth):
        title = att.get("title","")
        if filename_contains and filename_contains not in title: continue
        ext = att.get("extensions") or {}
        ver = att.get("version") or {}
        links = att.get("_links") or {}
        items.append({
            "id": att.get("id"),
            "title": title,
            "mediaType": ext.get("mediaType"),
            "fileSize": ext.get("fileSize"),
            "version": ver.get("number"),
            "created": ver.get("when"),
            "creator": (ver.get("by") or {}).get("displayName"),
            "download": f'{base_url}{links.get("download")}' if links.get("download") else None,
            "webui": f'{base_url}{links.get("webui")}' if links.get("webui") else None,
        })
    return items
#
def delete_attachment(base_url: str, attachment_id: str, auth: HTTPBasicAuth) -> requests.Response:
    return requests.delete(f"{base_url}/rest/api/content/{attachment_id}", auth=auth)
#
def human(n: Optional[int]) -> str:
    if n is None: return ""
    s = float(n)
    for u in ("B","KB","MB","GB","TB"):
        if s < 1024 or u == "TB": return f"{s:.1f} {u}"
        s /= 1024

# ---- commands
@app.command("show-page", help="Resolve page info from _publish.yml for a given source .qmd")
def show_page(
    source: Path = typer.Argument(..., help="Path to the source .qmd listed in _publish.yml"),
    publish: Path = typer.Option(Path("_publish.yml"), "--publish", "-p", help="Path to _publish.yml"),
):
    info = get_page_from_publish(publish, str(source))
    base_url = resolve_base_url(None, info["url"])
    typer.echo(json.dumps({"page_id": info["id"], "page_url": info["url"], "base_url": base_url}, indent=2))

@app.command("list", help="List all attachments on the Confluence page mapped from the .qmd in _publish.yml")
def list_cmd(
    source: Path = typer.Argument(..., help="Source .qmd (key in _publish.yml)"),
    publish: Path = typer.Option(Path("_publish.yml"), "--publish", "-p", help="Path to _publish.yml"),
    filename_contains: Optional[str] = typer.Option(None, "--contains", "-c", help="Filter by substring in filename"),
    output: str = typer.Option("table", "--output", "-o", help="table|tsv|json", case_sensitive=False),
    base_url: Optional[str] = typer.Option(None, "--base-url", help="Override CONFLUENCE_BASE_URL"),
    email: Optional[str] = typer.Option(None, "--email", help="Override CONFLUENCE_EMAIL"),
    token: Optional[str] = typer.Option(None, "--token", help="Override CONFLUENCE_API_TOKEN"),
):
    info = get_page_from_publish(publish, str(source))
    auth = make_auth(email, token)
    b = resolve_base_url(base_url, info["url"])
    atts = list_page_attachments(b, info["id"], auth, filename_contains=filename_contains)
    if output.lower() == "json":
        typer.echo(json.dumps(atts, indent=2))
        raise typer.Exit()
    if output.lower() == "tsv":
        typer.echo("id\ttitle\tfileSize\tmediaType\tversion\tcreator\tcreated\tdownload")
        for a in atts:
            typer.echo(f"{a['id']}\t{a['title']}\t{a['fileSize']}\t{a['mediaType']}\t{a['version']}\t{a['creator']}\t{a['created']}\t{a['download']}")
        raise typer.Exit()
    # table
    if not atts:
        typer.echo("No attachments found.")
        raise typer.Exit()
    w = max(len(a["title"]) for a in atts)
    typer.echo(f"{'ID':<12}  {'Title':<{min(60,max(20,min(w,60)))}}  {'Size':>9}  {'Type':<24}  Ver  Creator")
    for a in atts:
        title = a["title"] if len(a["title"])<=60 else a["title"][:57]+"..."
        typer.echo(f"{a['id']:<12}  {title:<60}  {human(a['fileSize']):>9}  {str(a['mediaType'])[:24]:<24}  {str(a['version']):>3}  {a['creator']}")

@app.command("delete", help="Delete attachments on the mapped page (dry-run by default)")
def delete_cmd(
    source: Path = typer.Argument(..., help="Source .qmd (key in _publish.yml)"),
    publish: Path = typer.Option(Path("_publish.yml"), "--publish", "-p", help="Path to _publish.yml"),
    filename_contains: Optional[str] = typer.Option(None, "--contains", "-c", help="Filter by substring in filename"),
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Preview deletions instead of performing them"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt when not dry-run"),
    base_url: Optional[str] = typer.Option(None, "--base-url", help="Override CONFLUENCE_BASE_URL"),
    email: Optional[str] = typer.Option(None, "--email", help="Override CONFLUENCE_EMAIL"),
    token: Optional[str] = typer.Option(None, "--token", help="Override CONFLUENCE_API_TOKEN"),
):
    info = get_page_from_publish(publish, str(source))
    auth = make_auth(email, token)
    b = resolve_base_url(base_url, info["url"])
    atts = list_page_attachments(b, info["id"], auth, filename_contains=filename_contains)
    if not atts:
        typer.echo("No attachments matched.")
        raise typer.Exit()
    typer.echo(f"Found {len(atts)} attachment(s) on page {info['id']}:")
    for a in atts:
        typer.echo(f" - [{a['id']}] {a['title']} ({human(a['fileSize'])})")
    if dry_run:
        typer.echo("\nDry-run: nothing deleted. Re-run with --no-dry-run to proceed.")
        raise typer.Exit()
    if not yes:
        if not typer.confirm("Proceed to delete the attachments above?"): raise typer.Exit(code=1)
    failures = 0
    for a in atts:
        r = delete_attachment(b, a["id"], auth)
        if r.status_code in (204,200): typer.echo(f"✅ Deleted [{a['id']}] {a['title']}")
        else:
            failures += 1
            typer.echo(f"❌ Failed [{a['id']}] {a['title']} — HTTP {r.status_code}")
            try: typer.echo(r.text)
            except Exception: pass
    if failures==0: typer.echo("\nAll deletions succeeded.")
    else: typer.echo(f"\nCompleted with {failures} failure(s).")

# ---- entry
def main(): app()

if __name__ == "__main__": main()
