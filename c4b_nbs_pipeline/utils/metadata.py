"""
Metadata Generator
==================

Generate FAIR-compliant metadata records following Dublin Core and ISO 19115.
Produces structured metadata for datasets, enabling discovery and
interoperability through standard catalogues (STAC, CSW, OAI-PMH).
"""

from datetime import datetime
from typing import Any


def generate_dublin_core(
    title: str,
    creator: str | list[str],
    description: str = "",
    subject: list[str] | None = None,
    publisher: str = "Cloud4Business S.r.l.",
    date: str | None = None,
    resource_type: str = "Dataset",
    format: str = "application/x-zarr",
    identifier: str = "",
    rights: str = "CC-BY 4.0",
    coverage_spatial: dict[str, Any] | None = None,
    coverage_temporal: tuple[str, str] | None = None,
    relation: list[str] | None = None,
) -> dict[str, Any]:
    """Generate a Dublin Core metadata record.

    Parameters:
        title: Resource title.
        creator: Creator name(s).
        description: Resource abstract/description.
        subject: Subject keywords.
        publisher: Publishing organisation.
        date: Publication date (ISO 8601). Default: today.
        resource_type: Dublin Core resource type.
        format: MIME type or format description.
        identifier: DOI or persistent identifier.
        rights: Licence identifier.
        coverage_spatial: GeoJSON geometry for spatial coverage.
        coverage_temporal: (start_date, end_date) tuple.
        relation: Related resource identifiers (DOIs, URLs).

    Returns:
        Dublin Core metadata dictionary.
    """
    creators = [creator] if isinstance(creator, str) else creator

    metadata = {
        "dc:title": title,
        "dc:creator": creators,
        "dc:description": description,
        "dc:subject": subject or [],
        "dc:publisher": publisher,
        "dc:date": date or datetime.utcnow().strftime("%Y-%m-%d"),
        "dc:type": resource_type,
        "dc:format": format,
        "dc:identifier": identifier,
        "dc:rights": rights,
        "dc:language": "en",
    }

    if coverage_spatial:
        metadata["dc:coverage.spatial"] = coverage_spatial

    if coverage_temporal:
        metadata["dc:coverage.temporal"] = {
            "start": coverage_temporal[0],
            "end": coverage_temporal[1],
        }

    if relation:
        metadata["dc:relation"] = relation

    return metadata
