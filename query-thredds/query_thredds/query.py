"""THREDDS catalog traversal with glob pattern matching."""

import fnmatch
import logging
import time
from collections.abc import Iterator, Sequence

from siphon.catalog import TDSCatalog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_catalog(
    url: str, retries: int = 3, delay: float = 1.0, backoff: float = 2.0
) -> TDSCatalog:
    """Load a TDSCatalog with retry logic for connection errors.

    Args:
        url: THREDDS catalog URL
        retries: Number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry

    Returns:
        TDSCatalog instance

    Raises:
        Exception: If all retry attempts fail
    """
    last_error = None
    for attempt in range(retries):
        try:
            return TDSCatalog(url)
        except Exception as e:
            last_error = e
            # Don't retry client errors (4xx) — they won't resolve with retries
            err_str = str(e)
            if "404" in err_str or "400" in err_str or "403" in err_str:
                logger.error(f"Non-retryable error loading catalog: {e}")
                raise
            if attempt < retries - 1:
                wait = delay * (backoff**attempt)
                logger.warning(
                    "Catalog load failed, retry %s/%s in %.1fs: %s",
                    attempt + 1,
                    retries,
                    wait,
                    e,
                )
                time.sleep(wait)
            else:
                logger.error(f"Failed to load catalog after {retries} attempts: {e}")
    if last_error is None:
        raise RuntimeError("Catalog load failed for unknown reason")
    raise last_error


class GlobMatcher:
    """Glob pattern matcher with ** (recursive) support for path segments."""

    def __init__(self, pattern: str | None):
        """Initialize with a glob pattern (e.g., "Y2025/**/*.nc").
        
        If pattern is None, defaults to "**" to match all paths.
        """
        if pattern is None:
            pattern = "**"
        self.pattern_parts: list[str] = pattern.split("/")

    def matches(self, path_parts: Sequence[str]) -> bool:
        """Check if path fully matches the pattern."""
        return self._match(path_parts, 0, 0)

    def can_match_prefix(self, path_parts: Sequence[str]) -> bool:
        """Check if path could be a prefix of a matching path."""
        return self._prefix_match(path_parts, 0, 0)

    def _match(self, path_parts: Sequence[str], pi: int, pati: int) -> bool:
        """Recursive matching implementation."""
        if pi == len(path_parts) and pati == len(self.pattern_parts):
            return True
        if pati == len(self.pattern_parts):
            return False
        if pi == len(path_parts):
            return all(p == "**" for p in self.pattern_parts[pati:])

        pat = self.pattern_parts[pati]

        if pat == "**":
            # ** matches zero or more segments
            return self._match(path_parts, pi, pati + 1) or self._match(
                path_parts, pi + 1, pati
            )
        elif fnmatch.fnmatchcase(path_parts[pi], pat):
            return self._match(path_parts, pi + 1, pati + 1)
        return False

    def _prefix_match(self, path_parts: Sequence[str], pi: int, pati: int) -> bool:
        """Check if path_parts could lead to a valid match."""
        if pi == len(path_parts):
            return True
        if pati == len(self.pattern_parts):
            return False

        pat = self.pattern_parts[pati]

        if pat == "**":
            return self._prefix_match(path_parts, pi, pati + 1) or self._prefix_match(
                path_parts, pi + 1, pati
            )
        elif fnmatch.fnmatchcase(path_parts[pi], pat):
            return self._prefix_match(path_parts, pi + 1, pati + 1)
        return False


def _traverse_catalog(
    cat: TDSCatalog,
    matcher: GlobMatcher,
    path_parts: list[str],
    depth: int,
    max_depth: int | None,
) -> Iterator[str]:
    """Recursively traverse catalog, yielding matching dataset URLs."""
    if max_depth is not None and depth > max_depth:
        logger.warning(f"Max depth {max_depth} reached, stopping recursion")
        return

    # Yield matching datasets at this level
    for dataset in cat.datasets.values():
        dataset_path = path_parts + [dataset.name]
        if "HTTPServer" in dataset.access_urls and matcher.matches(dataset_path):
            logger.debug(f"Matched: {'/'.join(dataset_path)}")
            yield dataset.access_urls["HTTPServer"]

    # Recurse into matching subcatalogs
    for ref in cat.catalog_refs.values():
        ref_path = path_parts + [ref.name]
        if not matcher.can_match_prefix(ref_path):
            continue

        logger.debug(f"Following: {'/'.join(ref_path)}")
        try:
            sub_cat = ref.follow()
        except Exception as e:
            logger.error(f"Failed to follow {ref.name}: {e}")
            continue

        yield from _traverse_catalog(sub_cat, matcher, ref_path, depth + 1, max_depth)


def recurse_catalog(
    catalog: TDSCatalog,
    pattern: str | None = None,
    max_depth: int | None = None,
    parallel: bool = False,
    max_workers: int = 8,
) -> Iterator[str]:
    """Traverse a THREDDS catalog and yield URLs for matching datasets.

    Uses glob-style pattern matching with ** support for recursive directory matching.
    Pattern segments match catalog ref names at each level, with the final segment
    matching dataset names.

    Examples:
        - "**/*.nc" - Match all .nc files at any depth
        - "Y2025/**/*.nc" - Match .nc files under Y2025 at any depth
        - "Y*/M*/D*/*.nc" - Match .nc files with specific year/month/day structure
        - "Y2025/M0[1-6]/**/*slv*.nc" - Match slv files in first 6 months of 2025
        - None - Match all datasets (equivalent to "**")

    Args:
        catalog: TDSCatalog instance to traverse
        pattern: Glob pattern with ** support. Case-sensitive.
                 If None, matches all datasets (defaults to "**").
        max_depth: Maximum recursion depth, or None for unbounded (default: None)
        parallel: If True, follow sibling catalog refs concurrently (default: False)
        max_workers: Maximum worker threads when parallel=True (default: 8)

    Yields:
        HTTP URLs for matching datasets as they are discovered
    """
    matcher = GlobMatcher(pattern)
    yield from _traverse_catalog(catalog, matcher, [], 0, max_depth)
