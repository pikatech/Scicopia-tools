#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 16 17:30:18 2021

@author: tech
"""
import pytest
from collections import Counter

from scicopia_tools.compile.ngrams import (
    clean_ngrams,
    export_ngrams,
    lower_ngrams,
    ngrams,
    trim_ngrams,
    weight_ngrams,
)


@pytest.fixture
def pipeline():
    import spacy

    nlp = spacy.load("en_core_web_lg", exclude=["ner", "lemmatizer", "textcat"])
    return nlp


def test_clean_ngrams():
    counts = Counter()
    counts.update(["be, like that", "Comment：\n Accepted for", "No punctuation here"])

    expected = Counter()
    expected.update(["No punctuation here"])

    result = clean_ngrams(counts)
    assert result == expected


def test_lower_ngrams():
    counts = Counter()
    counts.update(["be like that", "Accepted for publication", "No punctuation here"])

    expected = Counter()
    expected.update(["be like that", "accepted for publication", "no punctuation here"])

    result = lower_ngrams(counts)
    assert result == expected


def test_simple_bigrams():
    text = "This is a test. Just a test."
    counts = Counter(
        {"This is": 1, "is a": 1, "a test.": 2, "test. Just": 1, "Just a": 1}
    )

    bigrams = Counter(" ".join(x) for x in ngrams(text.split(), 2))
    assert bigrams == counts


def test_trim_bigrams():
    counts = Counter(
        {"This is": 1, "is a": 1, "a test.": 2, "test. Just": 1, "Just a": 1}
    )
    expected = Counter({"a test.": 2})

    thresholded_counts = trim_ngrams(counts, 2)
    assert thresholded_counts == expected


def test_bigram_export(pipeline):
    text = ["This is a test.", "Just a test."]
    counts = Counter({"This is": 1, "is a": 1, "a test.": 2, "Just a": 1})

    bigrams = export_ngrams(text, pipeline, n="2")
    assert bigrams == counts


def test_bi_trigram_export(pipeline):
    text = ["This is a test.", "Just a test."]
    counts = Counter(
        {
            "This is": 1,
            "is a": 1,
            "a test.": 2,
            "Just a": 1,
            "This is a": 1,
            "is a test.": 1,
            "Just a test.": 1,
        }
    )

    bigrams = export_ngrams(text, pipeline, n="2-3")
    assert bigrams == counts


def test_bigram_export_patterns(pipeline):
    text = [
        "He had Alzheimer's disease on a space station.",
        "Will there be human-like AI?",
    ]
    counts = Counter(
        {
            "Alzheimer's disease": 1,
            "space station": 1,
            "human-like AI": 1,
        }
    )

    bigrams = export_ngrams(text, pipeline, n="2", patterns=True)
    assert bigrams == counts


def test_bigram_hyphen_filter(pipeline):
    text = [
        "We expect state-of-the-art performance of our new query auto-completion.",
    ]
    counts = Counter(
        {
            "state-of-the-art performance": 1,
            "query auto-completion": 1,
            "new query": 1,
        }
    )

    bigrams = export_ngrams(text, pipeline, n="2", patterns=True)
    assert bigrams == counts


def test_bi_trigram_hyphen_filter(pipeline):
    text = [
        "We expect state-of-the-art performance of our new query auto-completion.",
    ]
    counts = Counter(
        {
            "state-of-the-art performance": 1,
            "query auto-completion": 1,
            "new query": 1,
            "new query auto-completion": 1,
        }
    )

    bigrams = export_ngrams(text, pipeline, n="2-3", patterns=True)
    assert bigrams == counts


def test_bi_trigram_filter(pipeline):
    text = [
        "Flexible A* algorithms, particularly iterative deepening A*, are valuable here.",
    ]
    counts = Counter(
        {
            "A*": 1,
            "deepening A*": 1,
            "iterative deepening A*": 1,
        }
    )

    bi_trigrams = export_ngrams(text, pipeline, n="2-4", patterns=True)
    assert bi_trigrams == counts


def test_bi_trigram_filter():
    counts = Counter(
        {
            "word": 23,
            "another bi-gram": 12,
            "my favourite tri-gram": 100,
            "Simultaneous Localization and Mapping": 7,
        }
    )
    expected = Counter(
        {
            "word": 23,
            "another bi-gram": 48,
            "my favourite tri-gram": 2700,
            "Simultaneous Localization and Mapping": 1792,
        }
    )
    assert weight_ngrams(counts) == expected
