#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 15:23:40 2020

@author: kampe
"""

from analyzers.hearst import hearst


def test_such_as():
    text = "Patients of all ages can develop nail disorders, such as onychocryptosis, which are recurrent and painful conditions."
    relations = hearst(text)
    assert len(relations) == 1
    assert " ".join(relations[0]) == "nail disorder such as onychocryptosis"
