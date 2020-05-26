#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 26 17:50:25 2020

@author: tech
"""

from analyzers.LangDetect import LangDetect


def test_german():
    text = "Falsches Üben von Xylophonmusik quält jeden größeren Zwerg."
    detector = LangDetect()
    result = detector.process(text)
    assert result == {"language": "de"}


def test_english():
    text = "Pack my box with five dozen liquor jugs."
    detector = LangDetect()
    result = detector.process(text)
    assert result == {"language": "en"}
