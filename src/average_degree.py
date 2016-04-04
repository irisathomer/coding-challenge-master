#!/usr/bin/env python

from __future__ import print_function
from datetime import datetime
from dateutil import parser
from decimal import Decimal, ROUND_DOWN
import json
import sys

'''example of program that calculates the average degree of hashtags'''

TWOPLACES = Decimal(10) ** -2

class TweetGraph(object):
	"""
	Graph representation of the hashtags
	"""
	def __init__(self):
		self._graph = {}
		self._vertices = {}

	def add_vertex(self, vertex, ts):
		if vertex not in self._vertices:
			self._vertices[vertex] = ts
			self._graph[vertex] = {}
		elif ts > self._vertices[vertex]:
			self._vertices[vertex] = ts			

	def _update_edge_for_add(self, from_vertex, to_vertex, ts):
		# If there is already an edge, check its TS is older; If not, we used its TS
		if to_vertex in self._graph[from_vertex]:
			oldTs = self._graph[from_vertex][to_vertex]
			if oldTs > ts:
				#do nothing
				pass

		self._graph[from_vertex][to_vertex] = ts

	def add_edge(self, vertex1, vertex2, ts):
		self.add_vertex(vertex1, ts)
		self.add_vertex(vertex2, ts)

		self._update_edge_for_add(vertex1, vertex2, ts)
		self._update_edge_for_add(vertex2, vertex1, ts)

	def remove_vertex(self, vertex, ts):
		assert(vertex in self._vertices)
		if self._vertices[vertex] <= ts:
			self._vertices.delete(vertex)
			assert(len(self._graph[vertex]) == 0)
			self._graph.delete(vertex)

	def _update_edge_for_remove(self, from_vertex, to_vertex, ts):
		assert(to_vertex in self._graph[from_vertex])
		current_ts = self._graph[from_vertex][to_vertex]

		# If there is no newer edge, we remove it
		if current_ts <= ts:
			del(self._graph[from_vertex][to_vertex])

	def remove_edge(self, vertex1, vertex2, ts):
		self._update_edge_for_remove(vertex1, vertex2, ts)
		self._update_edge_for_remove(vertex2, vertex1, ts)

		self.remove_vertex(vertex1, ts)
		self.remove_vertex(vertex2, ts)

	def print_graph(self):
		print(self.compute_avg_degree())
		for k, v in self._graph.iteritems():
			print(k, [str(k1) + ', ' + str(v1) for k1, v1 in v.iteritems()])
		for k, v in self._vertices.iteritems():
			print(k, repr(v))

	def compute_avg_degree(self):
		degree_sum = 0
		vertex_sum = 0
		for v in self._graph.values():
			vertex_sum += 1
			degree_sum += len(v)

		return Decimal(degree_sum*1.0/vertex_sum).quantize(TWOPLACES, rounding=ROUND_DOWN)

class TweetLoader(object):
	""" 
	A helper class for coverting a json file.
	"""

	def load_tweets_from_file(self, filename):
		""" 
		"""

		try:
			with open(filename, 'r') as json_file:
				linecount = 0
				for line in json_file:
					tweet_json = json.loads(line)
					if 'limit' in tweet_json.keys():
						print('skipping line: ', linecount)
						linecount+=1
						continue

					created_at = parser.parse(tweet_json['created_at'])
					hashtags = tweet_json['entities']['hashtags']
					print(linecount, repr(created_at), repr(hashtags))
					linecount+=1
		except:
			print("Unexpected error:", sys.exc_info()[0])
			raise


if __name__ == '__main__':
	if len(sys.argv) < 3:
		raise InputError('Missing file arguments')
	print('input' + sys.argv[1])
	print('output' + sys.argv[2])
	TweetLoader().load_tweets_from_file(sys.argv[1])
