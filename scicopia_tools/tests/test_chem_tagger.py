#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 15:23:40 2020

@author: kampe
"""

import pytest

from scicopia_tools.components.ChemTagger import ChemTagger


@pytest.fixture
def pipeline():
    import spacy

    nlp = spacy.load("en_core_web_sm", exclude=["ner", "lemmatizer", "textcat"])
    dict_path = "scicopia_tools/tests/resources/chemicals.txt"
    nlp.add_pipe("chemtagger", config={"wordlist": dict_path})
    return nlp


def test_overlap(pipeline):
    # Taken from: https://en.wikipedia.org/wiki/Hydrogen_peroxide
    doc = pipeline(
        "Enzymes that use or decompose hydrogen peroxide are classified as peroxidases."
    )
    assert len(doc.ents) == 1
    assert doc.ents[0].text == "hydrogen peroxide"


def test_compounds(pipeline):
    # Taken from: https://en.wikipedia.org/wiki/Ethyl_acetate
    doc = pipeline("Ethyl acetate is the ester of ethanol and acetic acid.")
    assert len(doc.ents) == 3


def test_pos_tags(pipeline):
    doc = pipeline("Our steps lead us deeper into the lead mine.")
    assert len(doc.ents) == 1
