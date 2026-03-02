# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import logging

logger = logging.getLogger(__name__)


def invalidate_status_cache() -> None:
    try:
        from ..routes import status_pages

        if hasattr(status_pages, "invalidate_status_cache"):
            status_pages.invalidate_status_cache()
        elif hasattr(status_pages, "_status_cache_fallback"):
            status_pages._status_cache_fallback.clear()
    except ImportError as exc:
        logger.warning("Failed to import status_pages for cache invalidation: %s", exc)
    except Exception as exc:
        logger.error("Error invalidating status cache: %s", exc)
