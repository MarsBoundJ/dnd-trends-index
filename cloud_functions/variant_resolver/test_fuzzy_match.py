"""
cloud_functions/variant_resolver/test_fuzzy_match.py

Unit tests for the fuzzy match pre-filter.
Run locally with: python -m pytest test_fuzzy_match.py -v

No GCP credentials needed — tests only cover pure Python logic.
"""

import pytest
from fuzzy_match import _tokenize, _token_overlap_score, find_best_match, ConceptRecord


# ---------------------------------------------------------------------------
# Tokenization tests
# ---------------------------------------------------------------------------

def test_tokenize_removes_stopwords():
    # "build" is kept — it's a meaningful token distinguishing build guides
    assert _tokenize("dnd ranger build 2024") == {"ranger", "build"}

def test_tokenize_handles_punctuation():
    assert _tokenize("Baldur's Gate 3") == {"baldur", "gate", "3"}

def test_tokenize_lowercases():
    assert _tokenize("Blood Hunter") == {"blood", "hunter"}

def test_tokenize_empty():
    assert _tokenize("dnd 5e 2024") == set()   # all stopwords

def test_tokenize_multiword_concept():
    assert _tokenize("Sword Saint") == {"sword", "saint"}


# ---------------------------------------------------------------------------
# Scoring tests
# ---------------------------------------------------------------------------

def test_exact_concept_match():
    # "ranger dnd" should score very high against concept "Ranger"
    candidate = _tokenize("ranger dnd")         # {"ranger"}
    concept   = _tokenize("Ranger")             # {"ranger"}
    score = _token_overlap_score(candidate, concept)
    assert score >= 0.72, f"Expected >= 0.72, got {score}"

def test_variant_with_extra_tokens():
    # "dnd ranger build guide" → {"ranger", "build", "guide"}  (guide now stopword? No.)
    # vs concept "Ranger" → {"ranger"}
    candidate = _tokenize("dnd ranger build")
    concept   = _tokenize("Ranger")
    score = _token_overlap_score(candidate, concept)
    assert score >= 0.50, f"Expected >= 0.50, got {score}"

def test_multiword_concept_match():
    # "blood hunter 5e" should match "Blood Hunter"
    candidate = _tokenize("blood hunter 5e")    # {"blood", "hunter"}
    concept   = _tokenize("Blood Hunter")       # {"blood", "hunter"}
    score = _token_overlap_score(candidate, concept)
    assert score >= 0.90, f"Expected >= 0.90, got {score}"

def test_unrelated_term_scores_low():
    # "baldurs gate 3 druid" should score LOW against "Ranger"
    candidate = _tokenize("baldurs gate 3 druid")
    concept   = _tokenize("Ranger")
    score = _token_overlap_score(candidate, concept)
    assert score < 0.30, f"Expected < 0.30, got {score}"

def test_empty_tokens_returns_zero():
    assert _token_overlap_score(set(), {"ranger"}) == 0.0
    assert _token_overlap_score({"ranger"}, set()) == 0.0


# ---------------------------------------------------------------------------
# find_best_match tests
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_concepts():
    return [
        ConceptRecord("Ranger",       "Class",   "2014",    _tokenize("Ranger")),
        ConceptRecord("Blood Hunter", "Class",   None,      _tokenize("Blood Hunter")),
        ConceptRecord("Fireball",     "Spell",   "2014",    _tokenize("Fireball")),
        ConceptRecord("Druid",        "Class",   "2014",    _tokenize("Druid")),
        ConceptRecord("Sword Saint",  "Class",   "Homebrew",_tokenize("Sword Saint")),
    ]

def test_finds_correct_concept(sample_concepts):
    concept, score = find_best_match("dnd ranger build", sample_concepts)
    assert concept is not None
    assert concept.concept_name == "Ranger"
    assert score >= 0.65

def test_finds_multiword_concept(sample_concepts):
    concept, score = find_best_match("blood hunter 5e guide", sample_concepts)
    assert concept is not None
    assert concept.concept_name == "Blood Hunter"
    assert score >= 0.65

def test_bg3_term_scores_low_against_ranger(sample_concepts):
    # "baldurs gate 3 ranger" contains "ranger" but is primarily a BG3 term.
    # With 4 tokens (baldurs, gate, 3, ranger) vs concept 1 token (ranger),
    # Jaccard pulls the score down. Score should be below HIGH_CONFIDENCE_THRESHOLD (0.72)
    # so it routes to Gemini rather than being auto-tagged as a Ranger variant.
    concept, score = find_best_match("baldurs gate 3 ranger", sample_concepts)
    assert score < 0.72, f"BG3 term should not be high-confidence match, got {score}"

def test_no_match_returns_none(sample_concepts):
    concept, score = find_best_match("lazy dungeon master tips", sample_concepts)
    assert score < 0.30

def test_all_stopwords_returns_none(sample_concepts):
    concept, score = find_best_match("best dnd 5e 2024", sample_concepts)
    assert concept is None
    assert score == 0.0
