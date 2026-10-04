from typing import Dict, List, Tuple

from .prospeo_client import ProspeoClient
from .redis_cache import RedisCache, TTL_DOMAIN_SCAN, TTL_PERSON_ENRICH, TTL_RECRUITER

MIN_CONFIDENCE = 90
VERIFIED_STATUS = "VERIFIED"


class EnrichmentPipeline:
    """
    Orchestrates: search-person → cache person data → bulk-enrich-person → filter verified.
    Returns (email_list, stats_dict).
    """

    def __init__(self, client: ProspeoClient, cache: RedisCache):
        self.client = client
        self.cache = cache

    async def discover_and_enrich(self, domain: str) -> Tuple[List[str], Dict]:
        stats: Dict = {
            "persons_found": 0,
            "persons_enriched": 0,
            "emails_verified": 0,
            "emails_rejected": 0,
            "cache_hits": 0,
            "credits_used": 0,
        }

        # ── Fast path: domain fully scanned + emails cached ──────────────
        scan_key   = f"company:scanned:{domain}"
        email_key  = f"recruiter_email:{domain}"

        if await self.cache.exists(scan_key):
            cached = await self.cache.get_json(email_key)
            if cached:
                stats["cache_hits"] = len(cached)
                print(f"[Pipeline] 🔁 {domain} cached ({len(cached)} emails) — skipping Prospeo")
                return cached, stats
            # Scanned but no emails found last time
            print(f"[Pipeline] ℹ️ {domain} already scanned, no emails")
            return [], stats

        # ── STEP 1: search-person ─────────────────────────────────────────
        persons = await self.client.search_recruiters(domain)
        if not persons:
            print(f"[Pipeline] ℹ️ No recruiter matches for {domain} — falling through to Hunter.io")
            await self.cache.set(scan_key, "1", ttl=TTL_DOMAIN_SCAN)
            return [], stats

        stats["persons_found"] = len(persons)

        # ── STEP 2: separate cached vs needs-enrich ───────────────────────
        to_enrich: List[str]    = []
        pre_enriched: List[Dict] = []

        for person in persons:
            # search-person returns: {"person": {"person_id": "...", ...}, "company": {...}}
            person_obj = person.get("person", {})
            pid = (
                person_obj.get("person_id")
                or person_obj.get("id")
                or person_obj.get("profile_id")
                or ""
            )
            if not pid:
                continue

            cached_person = await self.cache.get_json(f"person:enrich:{pid}")
            if cached_person:
                stats["cache_hits"] += 1
                pre_enriched.append(cached_person)
                print(f"[Pipeline] 💾 Cache hit: {pid}")
            else:
                to_enrich.append(pid)

        stats["credits_used"] = len(to_enrich)
        print(f"[Pipeline] 💳 Credits to use: {len(to_enrich)} | Cache hits: {stats['cache_hits']}")

        # ── STEP 3: bulk-enrich-person ────────────────────────────────────
        fresh: List[Dict] = []
        if to_enrich:
            enriched_items = await self.client.bulk_enrich(to_enrich)
            for item in enriched_items:
                # bulk-enrich returns: {"identifier": "...", "person": {...}, "company": {...}}
                person_obj = item.get("person", {})
                pid = person_obj.get("person_id") or person_obj.get("id") or ""
                if pid:
                    await self.cache.set_json(f"person:enrich:{pid}", person_obj, ttl=TTL_PERSON_ENRICH)
                fresh.append(person_obj)

        all_enriched = pre_enriched + fresh
        stats["persons_enriched"] = len(all_enriched)

        # ── STEP 4: filter — VERIFIED only (no confidence field in Prospeo) ──
        verified: List[str] = []

        for item in all_enriched:
            # item structure: {"email": {...}, "first_name": "...", ...}
            email_obj  = item.get("email", {}) or {}
            email      = (email_obj.get("email") or "").lower().strip()
            status     = (email_obj.get("status") or "").upper()
            revealed   = email_obj.get("revealed", False)

            if not email or not revealed:
                if email:
                    stats["emails_rejected"] += 1
                    print(f"[Pipeline] ⏭️ Rejected: {email} — not revealed or missing email")
                continue

            if status == VERIFIED_STATUS:
                verified.append(email)
                stats["emails_verified"] += 1
                print(f"[Pipeline] ✅ Verified: {email} (status={status})")
            else:
                stats["emails_rejected"] += 1
                print(f"[Pipeline] ⏭️ Rejected: {email} — status={status} (need VERIFIED)")

        # ── STEP 5: cache results, mark domain scanned ───────────────────
        if verified:
            await self.cache.set_json(email_key, verified[:5], ttl=TTL_RECRUITER)
        await self.cache.set(scan_key, "1", ttl=TTL_DOMAIN_SCAN)

        print(
            f"[Pipeline] 📊 {domain}: "
            f"found={stats['persons_found']} enriched={stats['persons_enriched']} "
            f"verified={stats['emails_verified']} rejected={stats['emails_rejected']} "
            f"credits={stats['credits_used']} cache_hits={stats['cache_hits']}"
        )
        return verified[:5], stats
