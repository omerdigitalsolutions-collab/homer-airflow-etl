"""
Mock scrapers for Yad2 and Facebook Marketplace property listings.

In production these functions call Apify Actors via the Apify API.
Here they generate realistic synthetic data for portfolio demonstration.

Intentional data quality issues injected for downstream testing:
- 5 % outlier prices (×10 or ÷10 the normal range)
- 15 % duplicate listings in the Facebook scraper
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import datetime, timezone

from pipeline.models import DealType, PropertyType, RawListing, ScraperStats, Source

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OUTLIER_RATE: float = 0.05          # 5 % of listings get an outlier price
FB_DUPLICATE_RATE: float = 0.15     # 15 % of FB listings are duplicates

YAD2_MIN_LISTINGS: int = 280
YAD2_MAX_LISTINGS: int = 320
FB_MIN_LISTINGS: int = 180
FB_MAX_LISTINGS: int = 220

# city → list[neighborhood]
CITY_NEIGHBORHOODS: dict[str, list[str]] = {
    "תל אביב": ["פלורנטין", "לב תל אביב", "נווה צדק", "הצפון הישן", "הצפון החדש",
                "רמת אביב", "יפו", "נחלת יצחק"],
    "רמת גן":  ["בורוכוב", "כפר אברהם", "גבעת שמואל", "גן יוסף", "קרית בורוכוב"],
    "פתח תקווה": ["כפר גנים", "אם המושבות", "הסלע", "נווה גן", "קרית אריה"],
    "חולון":   ["קרית שרת", "נווה שרת", "רמת חן", "גבעת קוסמן", "האחים"],
    "גבעתיים": ["הגבעה", "שינקין", "קרית יוסף", "קרית יהודית"],
    "הרצליה":  ["פיתוח", "הרצליה פיתוח", "נווה עמל", "כפר סמריאוגו"],
    "רעננה":   ["מרכז", "שיכון ותיקים", "גבעות עדן", "מנחמיה"],
    "בת ים":   ["נווה בת ים", "גבעת הטחנות", "שיכון ב'", "רמת חנוך"],
    "ירושלים": ["רחביה", "בקעה", "טלביה", "גילה", "פסגת זאב", "מלחה"],
    "חיפה":    ["הכרמל", "נווה שאנן", "הדר", "ראס כרמי", "בת גלים"],
}

CITIES: list[str] = list(CITY_NEIGHBORHOODS.keys())

PROPERTY_TYPES: list[PropertyType] = ["דירה", "פנטהאוז", "קוטג'", "דופלקס", "דירת גן"]
DEAL_TYPES: list[DealType] = ["למכירה", "להשכרה"]

# Price ranges (ILS) per deal type
PRICE_RANGES: dict[DealType, tuple[int, int]] = {
    "למכירה":  (800_000, 6_000_000),
    "להשכרה": (3_000,   12_000),
}

ROOMS: list[float] = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 7.0]

STREET_PREFIXES: list[str] = ["הרצל", "ביאליק", "בן גוריון", "ז'בוטינסקי", "רוטשילד",
                               "דיזנגוף", "שינקין", "שדרות ירושלים", "קיבוץ גלויות",
                               "העצמאות", "יוסף לישנסקי", "הברוש", "הדקל"]

DESCRIPTION_TEMPLATES: list[str] = [
    "דירה מרוהטת בלב השכונה, קרובה לתחבורה ציבורית.",
    "נכס מטופח במצב מעולה, גינה פרטית.",
    "פנורמה מדהימה, שיפוץ מלא ב-2022.",
    "נכס בעל פוטנציאל השבחה, קרוב לבתי ספר.",
    "דירה שקטה, חניה כלולה, מרפסת רחבה.",
    "מיקום מרכזי, נגישות מצוינת לצירים ראשיים.",
    "מושקעת ומעוצבת, ממ\"ד, מחסן.",
    "קרובה למרכז מסחרי, שכנים טובים.",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _random_phone() -> str:
    """Generate a realistic Israeli mobile phone number."""
    prefix = random.choice(["050", "052", "053", "054", "055", "058"])
    number = "".join(str(random.randint(0, 9)) for _ in range(7))
    return f"{prefix}-{number}"


def _random_price(deal_type: DealType, is_outlier: bool) -> float:
    """Return a price within the normal range or an extreme outlier."""
    low, high = PRICE_RANGES[deal_type]
    base = float(random.randint(low, high))
    if is_outlier:
        # Randomly multiply or divide by 10 to create a detectable outlier
        base = base * 10 if random.random() < 0.5 else base / 10
    return round(base, 2)


def _make_listing(source: Source, seed_id: str | None = None) -> RawListing:
    """Create a single synthetic property listing."""
    city = random.choice(CITIES)
    neighborhood = random.choice(CITY_NEIGHBORHOODS[city])
    street = f"רחוב {random.choice(STREET_PREFIXES)} {random.randint(1, 120)}"
    deal_type: DealType = random.choice(DEAL_TYPES)
    is_outlier = random.random() < OUTLIER_RATE

    return RawListing(
        id=seed_id or str(uuid.uuid4()),
        source=source,
        city=city,
        neighborhood=neighborhood,
        street=street,
        deal_type=deal_type,
        property_type=random.choice(PROPERTY_TYPES),
        rooms=random.choice(ROOMS),
        floor=random.randint(0, 30),
        size_sqm=float(random.randint(35, 280)),
        price=_random_price(deal_type, is_outlier),
        description=random.choice(DESCRIPTION_TEMPLATES),
        scraped_at=datetime.now(timezone.utc).isoformat(),
        agent_phone=_random_phone(),
    )


# ---------------------------------------------------------------------------
# Public scraper functions
# ---------------------------------------------------------------------------

def scrape_yad2() -> ScraperStats:
    """
    Scrape property listings from Yad2 (mock implementation).

    In production this function calls the Apify Yad2 Actor via:
        apify_client.actor("apify/yad2-scraper").call(run_input={...})

    Returns:
        ScraperStats containing source name, listing count, and listing data.
    """
    count = random.randint(YAD2_MIN_LISTINGS, YAD2_MAX_LISTINGS)
    logger.info("scrape_yad2 | generating %d mock listings", count)

    listings: list[RawListing] = [_make_listing("yad2") for _ in range(count)]

    logger.info("scrape_yad2 | done — %d listings generated", len(listings))
    return ScraperStats(source="yad2", count=len(listings), listings=listings)


def scrape_facebook() -> ScraperStats:
    """
    Scrape property listings from Facebook Marketplace (mock implementation).

    In production this function calls the Apify Facebook Marketplace Actor via:
        apify_client.actor("apify/facebook-marketplace-scraper").call(run_input={...})

    Intentionally injects ~15 % duplicate listings to exercise the deduplicator.

    Returns:
        ScraperStats containing source name, listing count, and listing data.
    """
    base_count = random.randint(FB_MIN_LISTINGS, FB_MAX_LISTINGS)
    logger.info("scrape_facebook | generating %d base listings (before duplicates)", base_count)

    base_listings: list[RawListing] = [_make_listing("facebook") for _ in range(base_count)]

    # Inject duplicates — same id & content, new scraped_at timestamp
    duplicate_count = int(base_count * FB_DUPLICATE_RATE)
    duplicates: list[RawListing] = []
    for original in random.sample(base_listings, min(duplicate_count, len(base_listings))):
        dup = RawListing(**original)           # shallow copy
        dup["scraped_at"] = datetime.now(timezone.utc).isoformat()
        duplicates.append(dup)

    all_listings = base_listings + duplicates
    random.shuffle(all_listings)

    logger.info(
        "scrape_facebook | done — %d listings (%d unique + %d duplicates)",
        len(all_listings),
        base_count,
        len(duplicates),
    )
    return ScraperStats(source="facebook", count=len(all_listings), listings=all_listings)
