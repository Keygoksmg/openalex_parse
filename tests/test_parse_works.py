"""Tests for the openalex_parse pipeline."""

import gzip
import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from openalex_parse.parse import load_schema, build_select_clause, build_columns_spec, find_gz_glob
from openalex_parse.schema_detect import classify_type, detect_schema, load_user_schema
from openalex_parse.schemas.works import WORKS_SCHEMA


# ── Fixtures ─────────────────────────────────────────────────────────────────

SAMPLE_RECORD = {
    "id": "https://openalex.org/W123456789",
    "doi": "https://doi.org/10.1234/test",
    "title": "A Test Paper",
    "display_name": "A Test Paper",
    "publication_year": 2023,
    "publication_date": "2023-06-15",
    "language": "en",
    "language_id": "https://openalex.org/languages/en",
    "type": "article",
    "type_crossref": "journal-article",
    "type_id": "https://openalex.org/work-types/article",
    "cited_by_count": 10,
    "fwci": 1.5,
    "referenced_works_count": 5,
    "authors_count": 3,
    "concepts_count": 0,
    "topics_count": 1,
    "locations_count": 1,
    "institutions_distinct_count": 2,
    "countries_distinct_count": 2,
    "has_abstract": True,
    "has_fulltext": True,
    "is_retracted": False,
    "is_paratext": False,
    "is_xpac": False,
    "publisher": "Test Publisher",
    "doi_registration_agency": "Crossref",
    "fulltext_origin": "pdf",
    "cited_by_api_url": "https://api.openalex.org/works?filter=cites:W123456789",
    "created_date": "2023-06-01",
    "updated": "2023-06-15T00:00:00.000000",
    "updated_date": "2023-06-15T00:00:00.000000",
    "ids": {"openalex": "https://openalex.org/W123456789"},
    "primary_location": {"source": {"id": "https://openalex.org/S1", "display_name": "Test Journal"}},
    "open_access": {"is_oa": True, "oa_status": "gold"},
    "best_oa_location": None,
    "citation_normalized_percentile": {"value": 0.85},
    "cited_by_percentile_year": {"min": 80, "max": 90},
    "apc_list": None,
    "apc_paid": None,
    "biblio": {"volume": "1", "issue": "2", "first_page": "10", "last_page": "20"},
    "primary_topic": {"id": "https://openalex.org/T100", "display_name": "Machine Learning"},
    "summary_stats": {"cited_by_count": 10, "2yr_cited_by_count": 5},
    "has_content": {"pdf": False, "grobid_xml": False},
    "indexed_in": ["crossref", "doaj"],
    "authorships": [
        {
            "author": {"id": "https://openalex.org/A1", "display_name": "Alice", "orcid": None},
            "author_position": "first",
            "is_corresponding": True,
            "institutions": [{"id": "https://openalex.org/I1", "display_name": "MIT", "country_code": "US"}],
        },
        {
            "author": {"id": "https://openalex.org/A2", "display_name": "Bob", "orcid": None},
            "author_position": "last",
            "is_corresponding": False,
            "institutions": [],
        },
    ],
    "keywords": [{"keyword": "AI", "score": 0.9}],
    "concepts": [],
    "topics": [{"id": "https://openalex.org/T100", "display_name": "Machine Learning"}],
    "related_works": [],
    "referenced_works": ["https://openalex.org/W111", "https://openalex.org/W222"],
    "locations": [{"source": {"display_name": "Test Journal"}}],
    "counts_by_year": [{"year": 2023, "cited_by_count": 5}],
    "corresponding_author_ids": ["https://openalex.org/A1"],
    "corresponding_institution_ids": ["https://openalex.org/I1"],
    "sustainable_development_goals": [],
    "grants": [],
    "mesh": [],
    "datasets": [],
    "versions": [],
    "institution_assertions": [],
    "awards": [],
    "funders": [],
    "institutions": [],
    "abstract_inverted_index": {"This": [0], "is": [1], "a": [2], "test": [3]},
}

SAMPLE_RECORD_NULLS = {
    "id": "https://openalex.org/W999999999",
    "doi": None,
    "title": None,
    "display_name": None,
    "publication_year": 2020,
    "publication_date": "2020-01-01",
    "language": None,
    "language_id": None,
    "type": "article",
    "type_crossref": None,
    "type_id": None,
    "cited_by_count": 0,
    "fwci": 0.0,
    "referenced_works_count": 0,
    "authors_count": 0,
    "concepts_count": 0,
    "topics_count": 0,
    "locations_count": 0,
    "institutions_distinct_count": 0,
    "countries_distinct_count": 0,
    "has_abstract": False,
    "has_fulltext": False,
    "is_retracted": None,
    "is_paratext": False,
    "is_xpac": None,
    "publisher": None,
    "doi_registration_agency": None,
    "fulltext_origin": None,
    "cited_by_api_url": None,
    "created_date": "2020-01-01",
    "updated": None,
    "updated_date": None,
    "ids": {},
    "primary_location": None,
    "open_access": {"is_oa": False},
    "best_oa_location": None,
    "citation_normalized_percentile": None,
    "cited_by_percentile_year": None,
    "apc_list": None,
    "apc_paid": None,
    "biblio": {},
    "primary_topic": None,
    "summary_stats": None,
    "has_content": None,
    "indexed_in": [],
    "authorships": [],
    "keywords": [],
    "concepts": [],
    "topics": [],
    "related_works": [],
    "referenced_works": [],
    "locations": [],
    "counts_by_year": [],
    "corresponding_author_ids": [],
    "corresponding_institution_ids": [],
    "sustainable_development_goals": [],
    "grants": [],
    "mesh": [],
    "datasets": [],
    "versions": [],
    "institution_assertions": [],
    "awards": [],
    "funders": [],
    "institutions": [],
    "abstract_inverted_index": None,
}


@pytest.fixture
def sample_gz_dir(tmp_path):
    """Create a temp directory with a .gz file containing sample records."""
    gz_path = tmp_path / "part_000.gz"
    records = [SAMPLE_RECORD, SAMPLE_RECORD_NULLS]
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return tmp_path


@pytest.fixture
def sample_schema_file(tmp_path):
    """Create a temporary schema config file."""
    schema_file = tmp_path / "test_schema.py"
    schema_file.write_text(
        'TEST_SCHEMA = {\n'
        '    "id": {"type": "str"},\n'
        '    "title": {"type": "str"},\n'
        '    "publication_year": {"type": "int"},\n'
        '}\n'
    )
    return schema_file


# ── classify_type ────────────────────────────────────────────────────────────

class TestClassifyType:
    def test_null(self):
        assert classify_type(None) == "null"

    def test_bool(self):
        assert classify_type(True) == "bool"
        assert classify_type(False) == "bool"

    def test_int(self):
        assert classify_type(42) == "int"
        assert classify_type(0) == "int"

    def test_float(self):
        assert classify_type(3.14) == "float"

    def test_str(self):
        assert classify_type("hello") == "str"
        assert classify_type("") == "str"

    def test_empty_list(self):
        assert classify_type([]) == "list[empty]"

    def test_list_of_dicts(self):
        assert classify_type([{"a": 1}]) == "list[dict]"

    def test_list_of_strings(self):
        assert classify_type(["a", "b"]) == "list[str]"

    def test_dict(self):
        assert classify_type({"key": "val"}) == "dict"

    def test_bool_before_int(self):
        assert classify_type(True) == "bool"


# ── load_schema ──────────────────────────────────────────────────────────────

class TestLoadSchema:
    def test_loads_schema_from_file(self, sample_schema_file):
        schema = load_schema(sample_schema_file)
        assert "id" in schema
        assert schema["id"]["type"] == "str"

    def test_loads_real_works_schema(self):
        schema_path = Path(__file__).resolve().parents[1] / "openalex_parse" / "schemas" / "works.py"
        schema = load_schema(schema_path)
        assert "id" in schema
        assert schema["fwci"]["type"] == "float"


class TestLoadUserSchema:
    def test_loads_schema_from_file(self, sample_schema_file):
        schema = load_user_schema(sample_schema_file)
        assert schema == {"id": "str", "title": "str", "publication_year": "int"}


# ── build_select_clause ─────────────────────────────────────────────────────

class TestBuildSelectClause:
    def test_scalar_fields(self):
        schema = {"id": {"type": "str"}, "year": {"type": "int"}}
        clause = build_select_clause(schema)
        assert 'CAST("id" AS VARCHAR)' in clause
        assert 'CAST("year" AS BIGINT)' in clause

    def test_json_fields(self):
        schema = {"authorships": {"type": "json"}}
        clause = build_select_clause(schema)
        assert 'to_json("authorships")' in clause


# ── find_gz_glob ─────────────────────────────────────────────────────────────

class TestFindGzGlob:
    def test_direct_gz_files(self, sample_gz_dir):
        glob = find_gz_glob(sample_gz_dir)
        assert glob.endswith("*.gz")

    def test_nested_gz_files(self, tmp_path):
        sub = tmp_path / "updated_date=2025-01-01"
        sub.mkdir()
        gz_path = sub / "part_000.gz"
        with gzip.open(gz_path, "wt") as f:
            f.write("{}\n")
        glob = find_gz_glob(tmp_path)
        assert "**" in glob

    def test_no_gz_files(self, tmp_path):
        assert find_gz_glob(tmp_path) is None


# ── detect_schema ────────────────────────────────────────────────────────────

class TestDetectSchema:
    def test_detects_all_fields(self):
        detected = detect_schema([SAMPLE_RECORD, SAMPLE_RECORD_NULLS])
        assert "id" in detected
        assert detected["id"]["frequency"] == 2

    def test_counts_types(self):
        detected = detect_schema([SAMPLE_RECORD, SAMPLE_RECORD_NULLS])
        assert "float" in detected["fwci"]["types"]
        assert detected["fwci"]["types"]["float"] == 2

    def test_counts_nulls(self):
        detected = detect_schema([SAMPLE_RECORD, SAMPLE_RECORD_NULLS])
        assert detected["doi"]["frequency"] == 2

    def test_captures_example(self):
        detected = detect_schema([SAMPLE_RECORD])
        assert detected["id"]["example"] is not None
        assert "W123456789" in detected["id"]["example"]


# ── DuckDB round-trip ────────────────────────────────────────────────────────

class TestDuckDBRoundTrip:
    def _run_parse(self, gz_dir, output_path, limit=None):
        """Helper: parse gz → parquet using production code path."""
        select_clause = build_select_clause(WORKS_SCHEMA)
        columns_spec = build_columns_spec(WORKS_SCHEMA)
        gz_glob = find_gz_glob(gz_dir)
        limit_clause = f"LIMIT {limit}" if limit else ""

        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT {select_clause}
                FROM read_json('{gz_glob}',
                    format='newline_delimited',
                    columns={columns_spec},
                    maximum_object_size=10485760)
                {limit_clause}
            ) TO '{output_path}' (FORMAT PARQUET)
        """)
        con.close()

    def test_parse_write_read_back(self, sample_gz_dir, tmp_path):
        """Full pipeline: gz → DuckDB parse → parquet → read back → verify."""
        output_path = tmp_path / "test.parquet"
        self._run_parse(sample_gz_dir, output_path)

        result = pl.read_parquet(output_path)
        assert result.shape[0] == 2

        # Scalar values
        row_2023 = result.filter(pl.col("publication_year") == 2023)
        assert row_2023["id"][0] == "https://openalex.org/W123456789"
        assert row_2023["cited_by_count"][0] == 10
        assert row_2023["fwci"][0] == 1.5
        assert row_2023["has_abstract"][0] is True

        # JSON fields are valid JSON strings
        auths = json.loads(row_2023["authorships"][0])
        assert len(auths) == 2
        assert auths[0]["author"]["display_name"] == "Alice"

        ids = json.loads(row_2023["ids"][0])
        assert ids["openalex"] == "https://openalex.org/W123456789"

    def test_null_record_round_trip(self, sample_gz_dir, tmp_path):
        """Null-heavy record survives round-trip."""
        output_path = tmp_path / "test.parquet"
        self._run_parse(sample_gz_dir, output_path)

        result = pl.read_parquet(output_path)
        row_2020 = result.filter(pl.col("publication_year") == 2020)
        assert row_2020["doi"][0] is None
        assert row_2020["cited_by_count"][0] == 0

    def test_limit(self, sample_gz_dir, tmp_path):
        """LIMIT clause works."""
        output_path = tmp_path / "test.parquet"
        self._run_parse(sample_gz_dir, output_path, limit=1)

        result = pl.read_parquet(output_path)
        assert result.shape[0] == 1


# ── Integration (requires real data) ────────────────────────────────────────

REAL_DATA_DIR = Path("/share/yin/openalex-2025_11_17/data/works/updated_date=2025-11-11")


@pytest.mark.skipif(not REAL_DATA_DIR.exists(), reason="Real data not available")
class TestIntegration:
    def test_parse_real_sample(self, tmp_path):
        """Parse 100 real records via DuckDB, write, read back."""
        output_path = tmp_path / "works.parquet"
        gz_glob = str(REAL_DATA_DIR / "*.gz")
        select_clause = build_select_clause(WORKS_SCHEMA)
        columns_spec = build_columns_spec(WORKS_SCHEMA)

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
        assert result.shape[1] == len(WORKS_SCHEMA)

        # All IDs should be non-null OpenAlex URLs
        assert result["id"].null_count() == 0
        assert result["id"].str.starts_with("https://openalex.org/W").all()

        # JSON fields should be parseable
        auths = json.loads(result["authorships"][0])
        assert isinstance(auths, list)
