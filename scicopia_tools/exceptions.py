#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 20:45:13 2021

@author: tech
"""

class ScicopiaException(Exception):
    """Base-class for all exceptions with Scicopia."""

class DBError(ScicopiaException):
    """Exceptions related to ArangoDB."""

class ConfigError(ScicopiaException):
    """Exceptions related to ArangoDB."""