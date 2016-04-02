#!/usr/bin/env python

from __future__ import print_function
from datetime import datetime
from dateutil import parser
import json
import sys

'''example of program that calculates the average degree of hashtags'''

class VertexWithTS(object):
	def __init__(self, name, ts):
		self.name = name
		self.ts = ts

	def __eq__(self, other):
		if not isinstance(other, Vertex):
			return False

		return self.name == other.name

	def __hash__(self):
		return hash(self.name)

	def __str__(self):
		return self.name #+ ',' + repr(self.ts)

class TweetGraph(object):
	def __init__(self):
		self._graph = {}
		self._vertices = {}

	def add_vertex_only(self, vertex, ts):
		if vertex not in self._vertices:
			self._vertices[vertex] = ts
			self._graph[vertex] = set()
		elif ts > self._vertices[vertex]:
			self._vertices[vertex] = ts			

	def _update_edge_set(self, vertex1, vertex2, ts):
		toVertex = VertexWithTS(vertex2, ts)
		if toVertex in self._graph[vertex1]:
			oldTs = self._graph[vertex1].remove(toVertex).ts
			if oldTs > toVertex.ts:
				toVertex.ts = oldTs

		self._graph[vertex1].add(toVertex)

	def add_edge(self, vertex1, vertex2, ts):
		self.add_vertex_only(vertex1, ts)
		self.add_vertex_only(vertex2, ts)

		self._update_edge_set(vertex1, vertex2, ts)
		self._update_edge_set(vertex2, vertex1, ts)

	def print_graph(self):
		for k, v in self._graph.iteritems():
			print(k, [str(e) for e in v])
		for k, v in self._vertices.iteritems():
			print(k, repr(v))

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
