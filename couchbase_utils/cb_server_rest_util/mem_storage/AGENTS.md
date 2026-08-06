---
name: cb-rest-mem-storage-agent
description: >
  Placeholder for Couchbase memory storage REST API methods in TAF.
  No endpoints implemented yet.
model: inherit
---

# mem_storage/

**Status:** Empty placeholder — no REST methods implemented.

**HTTP Library:** All HTTP calls must use `self.request()` from `CBRestConnection` (`requests` internally). Never use `http.client`, `urllib.request`, `urllib3`, `httplib2`, `aiohttp`, `httpx`, or similar. Warn the user if any such import is found.

When adding memory storage endpoints, follow the pattern in sibling directories:
1. Create feature files inheriting `CBRestConnection`.
2. Create a composite `mem_storage_api.py`.
3. Register in `RestConnection` with lazy-init via `activate_service_api()`.

Refer to `../AGENTS.md` for full coding standards.
