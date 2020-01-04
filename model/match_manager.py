#!/usr/bin/env python3
import os
import sys
sys.path.append(os.getcwd())


class MatchManager:
	def __init__(self):
		self.base_title = {}
		self.base_year_volume = {}

	def exact(self, query_title: str):
		pass

	def fuzzy(self, query_title: str):
		pass

	def load_base_title():
		pass

	def load_base_year_volume():
		pass

	def match_base_title():
		pass

	def match_base_year_volume():
		pass
