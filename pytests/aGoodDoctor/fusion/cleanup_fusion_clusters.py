#!/usr/bin/env python3
"""Clean up leftover Fusion backup/restore clusters in a Capella project.

The Fusion backup/restore suite runs with preserve_clusters=True, so aborted or
failed runs leave clusters behind (and a project can fill up its CIDR space /
capacity, causing later deploys to hit deploymentFailed). This standalone helper
lists the project's clusters and deletes the ones created by the suite
(name prefix "TAF_Fusion") and/or any in a failed state.

It reuses the same Capella v4 auth flow as pytests/Capella/RestAPIv4/api_base.py
and reads credentials from the [capella] section of the test .ini.

Usage (DRY-RUN by default — prints what would be deleted):
    python pytests/aGoodDoctor/fusion/cleanup_fusion_clusters.py -i fusion_backup_restore.ini

Actually delete:
    python pytests/aGoodDoctor/fusion/cleanup_fusion_clusters.py -i fusion_backup_restore.ini --delete

Other options:
    --prefix TAF_Fusion     name prefix to match (default: TAF_Fusion)
    --ids id1,id2           delete only these specific cluster ids
    --include-healthy       also delete healthy clusters matching the prefix
                            (default: prefix matches in ANY state are deleted;
                             see selection rules below)
    --failed-only           ignore the prefix; delete only failed-state clusters
    --project-id <id>       override the project id from the .ini

Selection rules (a cluster is targeted if):
  * its id is in --ids, OR
  * --failed-only and its currentState is a failed state, OR
  * (default) its name starts with --prefix, OR its currentState is a failed state
"""
import argparse
import configparser
import os
import sys

# Make the TAF SDK importable when run standalone (mirrors testrunner.py).
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", ".."))
for _p in (".", "lib"):
    _full = os.path.join(_REPO_ROOT, _p) if _p != "." else _REPO_ROOT
    if _full not in sys.path:
        sys.path.insert(0, _full)

from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI  # noqa: E402

try:  # sandbox uses an unverified cert; silence the per-request TLS warning
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

FAILED_STATES = {
    "deploymentfailed", "deletionfailed", "restorefailed",
    "rebalancefailed", "scalefailed", "upgradefailed", "turnonfailed",
    "turnofffailed",
}


def _read_capella_ini(ini_path):
    cp = configparser.ConfigParser(
        inline_comment_prefixes=("#",), strict=False)
    # Preserve case of keys.
    cp.optionxform = str
    with open(ini_path) as fh:
        cp.read_file(fh)
    if not cp.has_section("capella"):
        raise SystemExit(
            "[capella] section not found in {}".format(ini_path))
    cap = {k: v.strip() for k, v in cp.items("capella")}
    return cap


def _build_authed_client(cap):
    """Return an authenticated v4 CapellaAPI client plus the two API-key ids to
    clean up afterwards (org_owner_key_id, control_plane_key_id).
    """
    pod = cap.get("pod")
    user = cap.get("capella_user")
    passwd = cap.get("capella_pwd")
    org_id = cap.get("tenant_id")
    token = cap.get("override_token", "")
    for name, val in (("pod", pod), ("capella_user", user),
                      ("capella_pwd", passwd), ("tenant_id", org_id)):
        if not val:
            raise SystemExit(
                "Missing required [capella] key '{}' in the .ini".format(name))

    api = CapellaAPI("https://" + pod, "", "", user, passwd, token)

    # 1) v2 control-plane key authorizes creation of the v4 org key.
    resp = api.create_control_plane_api_key(org_id, "fusion_cleanup")
    if resp.status_code != 201:
        raise SystemExit(
            "Failed to create control-plane API key: {} {}".format(
                resp.status_code, resp.content))
    cp_key = resp.json()
    cp_key_id = cp_key["id"]
    api.org_ops_apis.bearer_token = cp_key["token"]
    api.cluster_ops_apis.bearer_token = cp_key["token"]

    # 2) v4 org-owner key for cluster ops.
    resp = api.org_ops_apis.create_api_key(
        organizationId=org_id,
        name="fusion_cleanup_v4",
        organizationRoles=["organizationOwner"],
        description="Temporary key for fusion cluster cleanup")
    if resp.status_code != 201:
        raise SystemExit(
            "Failed to create v4 org-owner API key: {} {}".format(
                resp.status_code, resp.content))
    owner_key = resp.json()
    api.org_ops_apis.bearer_token = owner_key["token"]
    api.cluster_ops_apis.bearer_token = owner_key["token"]

    return api, org_id, owner_key["id"], cp_key_id


def _cleanup_keys(api, org_id, owner_key_id, cp_key_id):
    try:
        api.org_ops_apis.delete_api_key(
            organizationId=org_id, accessKey=owner_key_id)
    except Exception as exc:
        print("WARN: could not delete v4 org key {}: {}".format(
            owner_key_id, exc))
    try:
        api.delete_control_plane_api_key(org_id, cp_key_id)
    except Exception as exc:
        print("WARN: could not delete control-plane key {}: {}".format(
            cp_key_id, exc))


def _list_clusters(api, org_id, project_id):
    clusters = []
    page = 1
    while True:
        resp = api.cluster_ops_apis.list_clusters(
            org_id, project_id, page=page, perPage=100)
        if resp.status_code != 200:
            raise SystemExit(
                "list_clusters failed: {} {}".format(
                    resp.status_code, resp.content))
        body = resp.json()
        data = body.get("data", [])
        clusters.extend(data)
        cursor = (body.get("cursor", {}) or {}).get("pages", {}) or {}
        last = cursor.get("last")
        if not last or page >= last or not data:
            break
        page += 1
    return clusters


def _select(clusters, prefix, ids, failed_only, include_healthy):
    selected = []
    for c in clusters:
        cid = c.get("id")
        name = c.get("name", "")
        state = (c.get("currentState") or "").lower()
        is_failed = state in FAILED_STATES
        if ids:
            if cid in ids:
                selected.append((c, "explicit id"))
            continue
        if failed_only:
            if is_failed:
                selected.append((c, "failed state '{}'".format(state)))
            continue
        if name.startswith(prefix):
            if is_failed or include_healthy or state != "healthy":
                selected.append((c, "prefix match (state={})".format(state)))
            else:
                # healthy prefix match, but --include-healthy not set
                continue
        elif is_failed:
            selected.append((c, "failed state '{}'".format(state)))
    return selected


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-i", "--ini", required=True,
                    help="Path to the test .ini with a [capella] section")
    ap.add_argument("--prefix", default="TAF_Fusion",
                    help="Cluster-name prefix to match (default: TAF_Fusion)")
    ap.add_argument("--ids", default="",
                    help="Comma-separated cluster ids to delete (overrides "
                         "prefix/state selection)")
    ap.add_argument("--include-healthy", action="store_true",
                    help="Also delete healthy clusters matching the prefix")
    ap.add_argument("--failed-only", action="store_true",
                    help="Ignore prefix; target only failed-state clusters")
    ap.add_argument("--project-id", default=None,
                    help="Override project id (else read from the .ini)")
    ap.add_argument("--delete", action="store_true",
                    help="Actually delete (default is a dry-run)")
    args = ap.parse_args()

    cap = _read_capella_ini(args.ini)
    project_id = (args.project_id or cap.get("project_id")
                  or cap.get("project"))
    if not project_id:
        raise SystemExit(
            "No project id: pass --project-id or set project_id in the .ini")
    ids = {x.strip() for x in args.ids.split(",") if x.strip()}

    api, org_id, owner_key_id, cp_key_id = _build_authed_client(cap)
    try:
        clusters = _list_clusters(api, org_id, project_id)
        print("Project {} has {} cluster(s):".format(
            project_id, len(clusters)))
        for c in clusters:
            print("  - {:<38} {:<22} {}".format(
                c.get("id"), c.get("name", ""),
                c.get("currentState", "")))

        selected = _select(clusters, args.prefix, ids,
                            args.failed_only, args.include_healthy)
        if not selected:
            print("\nNothing matched the selection rules — nothing to do.")
            return

        print("\n{} cluster(s) selected for deletion:".format(len(selected)))
        for c, reason in selected:
            print("  * {:<38} {:<22} [{}]".format(
                c.get("id"), c.get("name", ""), reason))

        if not args.delete:
            print("\nDRY-RUN (no --delete). Re-run with --delete to remove "
                  "the above.")
            return

        print("\nDeleting...")
        for c, _reason in selected:
            cid = c.get("id")
            try:
                resp = api.cluster_ops_apis.delete_cluster(
                    org_id, project_id, cid)
                ok = resp.status_code in (202, 204)
                print("  {} delete {} -> {}".format(
                    "OK " if ok else "ERR", cid, resp.status_code))
                if not ok:
                    print("      {}".format(resp.content))
            except Exception as exc:
                print("  ERR delete {} raised {}".format(cid, exc))
        print("\nDelete requests submitted (deletion runs asynchronously on "
              "Capella).")
    finally:
        _cleanup_keys(api, org_id, owner_key_id, cp_key_id)


if __name__ == "__main__":
    main()
