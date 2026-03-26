"""Tests for all entity schemas — load and round-trip through DuckDB."""

import gzip
import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from openalex_parse.parse import load_schema, build_select_clause, build_columns_spec, find_gz_glob
from openalex_parse.schemas.works import WORKS_SCHEMA
from openalex_parse.schemas.authors import AUTHORS_SCHEMA
from openalex_parse.schemas.concepts import CONCEPTS_SCHEMA
from openalex_parse.schemas.institutions import INSTITUTIONS_SCHEMA


# ── Sample records ────────────────────────────────────────────────────────────

SAMPLE_AUTHOR = {
    "id": "https://openalex.org/A5012213483",
    "orcid": "https://orcid.org/0000-0002-8025-008X",
    "display_name": "H. Tanaka",
    "works_count": 239,
    "cited_by_count": 3396890,
    "created_date": "2016-06-24",
    "updated_date": "2025-11-05T01:23:59.829358",
    "works_api_url": "https://api.openalex.org/works?filter=author.id:A5012213483",
    "ids": {"openalex": "https://openalex.org/A5012213483", "orcid": "https://orcid.org/0000-0002-8025-008X"},
    "summary_stats": {"2yr_mean_citedness": 1.74, "h_index": 25, "i10_index": 67},
    "display_name_alternatives": ["H. Tanaka", "H Tanaka"],
    "affiliations": [{"institution": {"id": "https://openalex.org/I146399215"}, "years": [2020, 2021]}],
    "last_known_institutions": [{"id": "https://openalex.org/I60134161", "display_name": "Kyoto U"}],
    "topics": [{"id": "https://openalex.org/T10346", "display_name": "Fusion"}],
    "topic_share": [{"id": "https://openalex.org/T10592", "display_name": "Fusion materials"}],
    "sources": [{"id": "https://openalex.org/S1", "display_name": "Journal of Fusion"}],
    "x_concepts": [{"id": 10138342, "display_name": "Physics"}],
    "counts_by_year": [{"year": 2023, "works_count": 5, "cited_by_count": 100}],
}

SAMPLE_CONCEPT = {
    "id": "https://openalex.org/C192562407",
    "wikidata": "https://www.wikidata.org/wiki/Q228736",
    "display_name": "Materials science",
    "level": 0,
    "description": "interdisciplinary field",
    "works_count": 36280611,
    "cited_by_count": 261788127,
    "image_url": "https://example.com/image.jpg",
    "image_thumbnail_url": "https://example.com/thumb.jpg",
    "created_date": "2016-06-24",
    "updated_date": "2022-04-12T19:38:12.138751",
    "works_api_url": "https://api.openalex.org/works?filter=concepts.id:192562407",
    "ids": {"openalex": "https://openalex.org/C192562407", "wikidata": "https://www.wikidata.org/wiki/Q228736"},
    "summary_stats": None,
    "international": None,
    "ancestors": None,
    "related_concepts": None,
    "counts_by_year": None,
}

SAMPLE_INSTITUTION = {
    "id": "https://openalex.org/I1294671590",
    "ror": "https://ror.org/02feahw73",
    "display_name": "CNRS",
    "country_code": "FR",
    "type": "other",
    "type_id": "https://openalex.org/institution-types/funder",
    "homepage_url": "https://www.cnrs.fr",
    "image_url": "https://example.com/cnrs.jpg",
    "image_thumbnail_url": "https://example.com/cnrs_thumb.jpg",
    "works_count": 1179653,
    "cited_by_count": 135984688,
    "is_super_system": False,
    "wiki_page": "http://en.wikipedia.org/wiki/CNRS",
    "wikidata_id": "https://www.wikidata.org/wiki/Q280413",
    "created_date": "2016-06-24T00:00:00",
    "updated_date": "2025-11-11T23:23:10.385787",
    "works_api_url": "https://api.openalex.org/works?filter=institutions.id:I1294671590",
    "ids": {"openalex": "https://openalex.org/I1294671590", "ror": "https://ror.org/02feahw73"},
    "geo": {"city": "Paris", "country_code": "FR"},
    "summary_stats": {"h_index": 1924},
    "display_name_acronyms": ["CNRS"],
    "display_name_alternatives": ["French National Centre for Scientific Research"],
    "associated_institutions": [{"id": "https://openalex.org/I1298838906"}],
    "lineage": ["https://openalex.org/I1294671590"],
    "repositories": [{"id": "https://openalex.org/S4306402512", "display_name": "HAL"}],
    "roles": [{"role": "funder", "id": "https://openalex.org/F4320322892"}],
    "topics": [{"id": "https://openalex.org/T10017", "display_name": "Geology"}],
    "topic_share": [{"id": "https://openalex.org/T10962", "display_name": "Thermo"}],
    "counts_by_year": [{"year": 2023, "works_count": 50000}],
}


def _make_gz(tmp_path, records, name="part_000.gz"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    gz_path = tmp_path / name
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return tmp_path


def _parse_roundtrip(gz_dir, schema, tmp_path, output_name="test.parquet"):
    """Parse gz → parquet → read back using production code path."""
    output_path = tmp_path / output_name
    select_clause = build_select_clause(schema)
    columns_spec = build_columns_spec(schema)
    gz_glob = find_gz_glob(gz_dir)

    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT {select_clause}
            FROM read_json('{gz_glob}',
                format='newline_delimited',
                columns={columns_spec},
                maximum_object_size=10485760)
        ) TO '{output_path}' (FORMAT PARQUET)
    """)
    con.close()
    return pl.read_parquet(output_path)


# ── Schema loading ────────────────────────────────────────────────────────────

class TestSchemaLoading:
    """Verify all schema files load correctly."""

    @pytest.mark.parametrize("schema_name,schema_dict", [
        ("works", WORKS_SCHEMA),
        ("authors", AUTHORS_SCHEMA),
        ("concepts", CONCEPTS_SCHEMA),
        ("institutions", INSTITUTIONS_SCHEMA),
    ])
    def test_schema_has_id(self, schema_name, schema_dict):
        assert "id" in schema_dict
        assert schema_dict["id"]["type"] == "str"

    @pytest.mark.parametrize("schema_name,schema_dict", [
        ("works", WORKS_SCHEMA),
        ("authors", AUTHORS_SCHEMA),
        ("concepts", CONCEPTS_SCHEMA),
        ("institutions", INSTITUTIONS_SCHEMA),
    ])
    def test_schema_types_valid(self, schema_name, schema_dict):
        valid_types = {"str", "int", "float", "bool", "json"}
        for field, config in schema_dict.items():
            assert config["type"] in valid_types, f"{schema_name}.{field} has invalid type: {config['type']}"

    @pytest.mark.parametrize("schema_path", [
        "openalex_parse/schemas/works.py",
        "openalex_parse/schemas/authors.py",
        "openalex_parse/schemas/concepts.py",
        "openalex_parse/schemas/institutions.py",
    ])
    def test_load_from_file(self, schema_path):
        path = Path(__file__).resolve().parents[1] / schema_path
        schema = load_schema(path)
        assert "id" in schema


# ── Authors round-trip ────────────────────────────────────────────────────────

class TestAuthorsRoundTrip:
    def test_parse_and_read_back(self, tmp_path):
        gz_dir = _make_gz(tmp_path / "gz", [SAMPLE_AUTHOR])
        result = _parse_roundtrip(gz_dir, AUTHORS_SCHEMA, tmp_path)
        assert result.shape[0] == 1
        assert result.shape[1] == len(AUTHORS_SCHEMA)
        assert result["id"][0] == "https://openalex.org/A5012213483"
        assert result["works_count"][0] == 239
        assert result["cited_by_count"][0] == 3396890

        # JSON fields parseable
        stats = json.loads(result["summary_stats"][0])
        assert stats["h_index"] == 25

        affiliations = json.loads(result["affiliations"][0])
        assert isinstance(affiliations, list)
        assert len(affiliations) == 1


# ── Concepts round-trip ───────────────────────────────────────────────────────

class TestConceptsRoundTrip:
    def test_parse_and_read_back(self, tmp_path):
        gz_dir = _make_gz(tmp_path / "gz", [SAMPLE_CONCEPT])
        result = _parse_roundtrip(gz_dir, CONCEPTS_SCHEMA, tmp_path)
        assert result.shape[0] == 1
        assert result.shape[1] == len(CONCEPTS_SCHEMA)
        assert result["id"][0] == "https://openalex.org/C192562407"
        assert result["level"][0] == 0
        assert result["works_count"][0] == 36280611

        # Null JSON fields survive
        assert result["ancestors"][0] is None
        assert result["summary_stats"][0] is None


# ── Institutions round-trip ───────────────────────────────────────────────────

class TestInstitutionsRoundTrip:
    def test_parse_and_read_back(self, tmp_path):
        gz_dir = _make_gz(tmp_path / "gz", [SAMPLE_INSTITUTION])
        result = _parse_roundtrip(gz_dir, INSTITUTIONS_SCHEMA, tmp_path)
        assert result.shape[0] == 1
        assert result.shape[1] == len(INSTITUTIONS_SCHEMA)
        assert result["id"][0] == "https://openalex.org/I1294671590"
        assert result["country_code"][0] == "FR"
        assert result["is_super_system"][0] is False

        # JSON fields
        geo = json.loads(result["geo"][0])
        assert geo["city"] == "Paris"

        roles = json.loads(result["roles"][0])
        assert roles[0]["role"] == "funder"


# ── Integration (requires real data) ─────────────────────────────────────────

REAL_AUTHORS_DIR = Path("/share/yin/openalex-2025_11_17/data/authors/updated_date=2025-11-05")
REAL_CONCEPTS_DIR = Path("/share/yin/openalex-2025_11_17/data/concepts/updated_date=2022-04-12")
REAL_INSTITUTIONS_DIR = Path("/share/yin/openalex-2025_11_17/data/institutions/updated_date=2025-11-11")


@pytest.mark.skipif(not REAL_AUTHORS_DIR.exists(), reason="Real data not available")
class TestIntegrationAuthors:
    def test_parse_real_sample(self, tmp_path):
        output_path = tmp_path / "authors.parquet"
        gz_glob = str(REAL_AUTHORS_DIR / "*.gz")
        select_clause = build_select_clause(AUTHORS_SCHEMA)
        columns_spec = build_columns_spec(AUTHORS_SCHEMA)

        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT {select_clause}
                FROM read_json('{gz_glob}',
                    format='newline_delimited',
                    columns={columns_spec},
                    maximum_object_size=10485760)
                LIMIT 100
            ) TO '{output_path}' (FORMAT PARQUET)
        """)
        con.close()

        result = pl.read_parquet(output_path)
        assert result.shape[0] == 100
        assert result.shape[1] == len(AUTHORS_SCHEMA)
        assert result["id"].null_count() == 0
        assert result["id"].str.starts_with("https://openalex.org/A").all()


@pytest.mark.skipif(not REAL_INSTITUTIONS_DIR.exists(), reason="Real data not available")
class TestIntegrationInstitutions:
    def test_parse_real_sample(self, tmp_path):
        output_path = tmp_path / "institutions.parquet"
        select_clause = build_select_clause(INSTITUTIONS_SCHEMA)
        columns_spec = build_columns_spec(INSTITUTIONS_SCHEMA)
        gz_glob = str(REAL_INSTITUTIONS_DIR / "*.gz")

        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT {select_clause}
                FROM read_json('{gz_glob}',
                    format='newline_delimited',
                    columns={columns_spec},
                    maximum_object_size=10485760)
                LIMIT 100
            ) TO '{output_path}' (FORMAT PARQUET)
        """)
        con.close()

        result = pl.read_parquet(output_path)
        assert result.shape[0] == 100
        assert result.shape[1] == len(INSTITUTIONS_SCHEMA)
        assert result["id"].null_count() == 0
        assert result["id"].str.starts_with("https://openalex.org/I").all()
