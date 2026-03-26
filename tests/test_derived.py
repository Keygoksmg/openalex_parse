"""Tests for Layer 2 derived tables."""

import gzip
import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from openalex_parse.parse import build_select_clause, build_columns_spec, find_gz_glob
from openalex_parse.schemas.works import WORKS_SCHEMA
from openalex_parse.derived.work_title_abstracts import reconstruct_abstract


# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_WORKS = [
    {
        "id": "https://openalex.org/W1",
        "doi": "https://doi.org/10.1234/test1",
        "title": "Paper One",
        "display_name": "Paper One",
        "publication_year": 2023,
        "publication_date": "2023-01-01",
        "language": "en",
        "language_id": "https://openalex.org/languages/en",
        "type": "article",
        "type_crossref": "journal-article",
        "type_id": "https://openalex.org/work-types/article",
        "cited_by_count": 10,
        "fwci": 1.5,
        "referenced_works_count": 5,
        "authors_count": 2,
        "concepts_count": 0,
        "topics_count": 0,
        "locations_count": 0,
        "institutions_distinct_count": 2,
        "countries_distinct_count": 2,
        "has_abstract": True,
        "has_fulltext": False,
        "is_retracted": False,
        "is_paratext": False,
        "is_xpac": False,
        "publisher": "Publisher A",
        "doi_registration_agency": "Crossref",
        "fulltext_origin": None,
        "cited_by_api_url": "https://api.openalex.org/works?filter=cites:W1",
        "created_date": "2023-01-01",
        "updated": "2023-01-01T00:00:00.000000",
        "updated_date": "2023-01-01T00:00:00.000000",
        "ids": {"openalex": "https://openalex.org/W1"},
        "primary_location": None,
        "open_access": {"is_oa": True},
        "best_oa_location": None,
        "citation_normalized_percentile": None,
        "cited_by_percentile_year": None,
        "apc_list": None,
        "apc_paid": None,
        "biblio": {},
        "primary_topic": None,
        "summary_stats": {"cited_by_count": 10},
        "has_content": {"pdf": False, "grobid_xml": False},
        "indexed_in": [],
        "authorships": [
            {
                "author_position": "first",
                "raw_author_name": "Alice Smith",
                "author": {"id": "https://openalex.org/A1", "display_name": "Alice Smith", "orcid": "https://orcid.org/0000-0001"},
                "is_corresponding": True,
                "countries": ["US"],
                "institutions": [
                    {"id": "https://openalex.org/I1", "display_name": "MIT", "ror": "https://ror.org/042nb2s44", "country_code": "US", "type": "education"},
                ],
                "affiliations": [],
            },
            {
                "author_position": "last",
                "raw_author_name": "Bob Jones",
                "author": {"id": "https://openalex.org/A2", "display_name": "Bob Jones", "orcid": None},
                "is_corresponding": False,
                "countries": ["UK"],
                "institutions": [
                    {"id": "https://openalex.org/I2", "display_name": "Oxford", "ror": "https://ror.org/052gg0110", "country_code": "GB", "type": "education"},
                ],
                "affiliations": [],
            },
        ],
        "keywords": [],
        "concepts": [],
        "topics": [],
        "related_works": [],
        "referenced_works": [],
        "locations": [],
        "counts_by_year": [],
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
        "abstract_inverted_index": {
            "Machine": [0],
            "learning": [1, 5],
            "is": [2],
            "a": [3],
            "subfield": [4],
        },
    },
    {
        "id": "https://openalex.org/W2",
        "doi": None,
        "title": "Paper Two",
        "display_name": "Paper Two",
        "publication_year": 2024,
        "publication_date": "2024-03-15",
        "language": "en",
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
        "is_retracted": False,
        "is_paratext": False,
        "is_xpac": None,
        "publisher": None,
        "doi_registration_agency": None,
        "fulltext_origin": None,
        "cited_by_api_url": None,
        "created_date": "2024-03-01",
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
        "abstract_inverted_index": None,
    },
]


@pytest.fixture
def works_parquet(tmp_path):
    """Create a test works parquet from sample records."""
    gz_dir = tmp_path / "gz"
    gz_dir.mkdir()
    gz_path = gz_dir / "part_000.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for rec in SAMPLE_WORKS:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    output_path = tmp_path / "works.parquet"
    select_clause = build_select_clause(WORKS_SCHEMA)
    columns_spec = build_columns_spec(WORKS_SCHEMA)
    gz_glob = str(gz_dir / "*.gz")

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
    return output_path


# ── reconstruct_abstract unit tests ──────────────────────────────────────────

class TestReconstructAbstract:
    def test_basic(self):
        inv = json.dumps({"This": [0], "is": [1], "a": [2], "test": [3]})
        assert reconstruct_abstract(inv) == "This is a test"

    def test_word_at_multiple_positions(self):
        inv = json.dumps({"Machine": [0], "learning": [1, 5], "is": [2], "a": [3], "subfield": [4]})
        assert reconstruct_abstract(inv) == "Machine learning is a subfield learning"

    def test_null_input(self):
        assert reconstruct_abstract(None) is None
        assert reconstruct_abstract("null") is None
        assert reconstruct_abstract("{}") is None

    def test_empty_string(self):
        assert reconstruct_abstract("") is None

    def test_invalid_json(self):
        assert reconstruct_abstract("not json") is None

    def test_complex_abstract(self):
        inv = json.dumps({
            "We": [0], "present": [1], "a": [2, 7], "novel": [3],
            "approach": [4], "to": [5], "solving": [6], "challenging": [8], "problem.": [9],
        })
        result = reconstruct_abstract(inv)
        assert result == "We present a novel approach to solving a challenging problem."


# ── work_title_abstracts derived table ────────────────────────────────────────

class TestWorkTitleAbstracts:
    def test_output_schema(self, works_parquet, tmp_path):
        """Title-abstract table has correct columns."""
        from openalex_parse.derived.work_title_abstracts import reconstruct_abstract

        df = pl.read_parquet(works_parquet, columns=["id", "doi", "title", "abstract_inverted_index"])
        result = df.select(
            pl.col("id").alias("work_id"),
            pl.col("doi"),
            pl.col("title"),
            pl.col("abstract_inverted_index")
              .map_elements(reconstruct_abstract, return_dtype=pl.Utf8)
              .alias("abstract"),
        )
        assert set(result.columns) == {"work_id", "doi", "title", "abstract"}
        assert result.shape[0] == 2

    def test_abstract_reconstruction(self, works_parquet, tmp_path):
        """Abstracts are correctly reconstructed."""
        from openalex_parse.derived.work_title_abstracts import reconstruct_abstract

        df = pl.read_parquet(works_parquet, columns=["id", "abstract_inverted_index"])
        result = df.select(
            pl.col("id").alias("work_id"),
            pl.col("abstract_inverted_index")
              .map_elements(reconstruct_abstract, return_dtype=pl.Utf8)
              .alias("abstract"),
        )

        # W1 has abstract
        w1 = result.filter(pl.col("work_id") == "https://openalex.org/W1")
        assert w1["abstract"][0] == "Machine learning is a subfield learning"

        # W2 has no abstract
        w2 = result.filter(pl.col("work_id") == "https://openalex.org/W2")
        assert w2["abstract"][0] is None


# ── work_author_institutions derived table ────────────────────────────────────

class TestWorkAuthorInstitutions:
    def test_explode_authorships(self, works_parquet, tmp_path):
        """Authorships are correctly exploded into flat rows."""
        output_path = tmp_path / "work_author_institutions.parquet"
        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT
                    w.id AS work_id,
                    w.doi,
                    w.publication_year,
                    a.author_position,
                    a.raw_author_name,
                    a.author.id AS author_id,
                    a.author.display_name AS author_display_name,
                    a.author.orcid AS orcid,
                    a.is_corresponding,
                    a.countries,
                    a.institutions[1].id AS first_institution_id,
                    a.institutions[1].display_name AS first_institution_display_name,
                    a.institutions[1].ror AS first_institution_ror,
                    a.institutions[1].country_code AS first_institution_country_code,
                    a.institutions[1].type AS first_institution_type
                FROM read_parquet('{works_parquet}') w,
                LATERAL (
                    SELECT UNNEST(from_json(w.authorships, '[{{
                        "author_position": "VARCHAR",
                        "raw_author_name": "VARCHAR",
                        "author": {{
                            "id": "VARCHAR",
                            "display_name": "VARCHAR",
                            "orcid": "VARCHAR"
                        }},
                        "is_corresponding": "BOOLEAN",
                        "countries": "JSON",
                        "institutions": [{{
                            "id": "VARCHAR",
                            "display_name": "VARCHAR",
                            "ror": "VARCHAR",
                            "country_code": "VARCHAR",
                            "type": "VARCHAR"
                        }}]
                    }}]')) AS a
                )
                WHERE w.authorships IS NOT NULL AND w.authorships != '[]'
            ) TO '{output_path}' (FORMAT PARQUET)
        """)
        con.close()

        result = pl.read_parquet(output_path)

        # W1 has 2 authors, W2 has 0 → 2 rows
        assert result.shape[0] == 2

        # Check first author
        alice = result.filter(pl.col("author_display_name") == "Alice Smith")
        assert alice.shape[0] == 1
        assert alice["work_id"][0] == "https://openalex.org/W1"
        assert alice["author_position"][0] == "first"
        assert alice["is_corresponding"][0] is True
        assert alice["first_institution_display_name"][0] == "MIT"
        assert alice["first_institution_country_code"][0] == "US"

        # Check second author
        bob = result.filter(pl.col("author_display_name") == "Bob Jones")
        assert bob.shape[0] == 1
        assert bob["author_position"][0] == "last"
        assert bob["first_institution_display_name"][0] == "Oxford"
        assert bob["first_institution_country_code"][0] == "GB"

    def test_empty_authorships_excluded(self, works_parquet, tmp_path):
        """Papers with empty authorships produce no rows."""
        output_path = tmp_path / "work_author_institutions.parquet"
        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT
                    w.id AS work_id,
                    a.author.id AS author_id
                FROM read_parquet('{works_parquet}') w,
                LATERAL (
                    SELECT UNNEST(from_json(w.authorships, '[{{
                        "author": {{"id": "VARCHAR"}}
                    }}]')) AS a
                )
                WHERE w.authorships IS NOT NULL AND w.authorships != '[]'
            ) TO '{output_path}' (FORMAT PARQUET)
        """)
        con.close()

        result = pl.read_parquet(output_path)
        # Only W1 has authors
        work_ids = result["work_id"].unique().to_list()
        assert work_ids == ["https://openalex.org/W1"]


# ── Integration (requires parsed test data) ──────────────────────────────────

WORKS_TEST_PARQUET = Path("/share/yin/kk929_codes/openalex_parse/data/intermediates/works_test.parquet")


@pytest.mark.skipif(not WORKS_TEST_PARQUET.exists(), reason="works_test.parquet not available")
class TestDerivedIntegration:
    def test_title_abstracts_on_real_data(self, tmp_path):
        """Reconstruct abstracts from real parsed data."""
        df = pl.read_parquet(
            WORKS_TEST_PARQUET,
            columns=["id", "doi", "title", "abstract_inverted_index"],
            n_rows=100,
        )
        result = df.select(
            pl.col("id").alias("work_id"),
            pl.col("doi"),
            pl.col("title"),
            pl.col("abstract_inverted_index")
              .map_elements(reconstruct_abstract, return_dtype=pl.Utf8)
              .alias("abstract"),
        )
        assert result.shape[0] == 100
        assert set(result.columns) == {"work_id", "doi", "title", "abstract"}
        # At least some should have abstracts
        n_with_abstract = result.filter(pl.col("abstract").is_not_null()).shape[0]
        assert n_with_abstract > 0

    def test_author_institutions_on_real_data(self, tmp_path):
        """Explode authorships from real parsed data."""
        output_path = tmp_path / "wai.parquet"
        con = duckdb.connect()
        # Use only first 100 works
        con.execute(f"""
            COPY (
                SELECT
                    w.id AS work_id,
                    w.publication_year,
                    a.author_position,
                    a.author.id AS author_id,
                    a.author.display_name AS author_display_name,
                    a.institutions[1].id AS first_institution_id,
                    a.institutions[1].country_code AS first_institution_country_code
                FROM (SELECT * FROM read_parquet('{WORKS_TEST_PARQUET}') LIMIT 100) w,
                LATERAL (
                    SELECT UNNEST(from_json(w.authorships, '[{{
                        "author_position": "VARCHAR",
                        "author": {{"id": "VARCHAR", "display_name": "VARCHAR"}},
                        "institutions": [{{"id": "VARCHAR", "country_code": "VARCHAR"}}]
                    }}]')) AS a
                )
                WHERE w.authorships IS NOT NULL AND w.authorships != '[]'
            ) TO '{output_path}' (FORMAT PARQUET)
        """)
        con.close()

        result = pl.read_parquet(output_path)
        # Should have more rows than input (one author per row)
        assert result.shape[0] > 0
        assert "work_id" in result.columns
        assert "author_id" in result.columns
        assert "first_institution_country_code" in result.columns
