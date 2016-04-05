#!/usr/bin/env python

from __future__ import print_function
from collections import deque
from datetime import datetime, timedelta
from dateutil import parser
from decimal import Decimal, ROUND_DOWN
import json
import sys
import logging

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
			if self._graph[from_vertex][to_vertex] > ts:
				#do nothing
				return

		self._graph[from_vertex][to_vertex] = ts

	def add_edge(self, vertex1, vertex2, ts):
		self.add_vertex(vertex1, ts)
		self.add_vertex(vertex2, ts)

		self._update_edge_for_add(vertex1, vertex2, ts)
		self._update_edge_for_add(vertex2, vertex1, ts)

	# Proper clean up expects edges to be removed first
	def remove_vertex(self, vertex, ts):
		if len(self._graph[vertex]) == 0:
			del(self._graph[vertex])
			if vertex in self._vertices and self._vertices[vertex] <= ts:
				del(self._vertices[vertex])

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

		if vertex_sum == 0:
			return Decimal('0.00')

		return Decimal(degree_sum*1.0/vertex_sum).quantize(TWOPLACES, rounding=ROUND_DOWN)

class TweetQueue(object):
	"""
	Queue that manages the 60-second window
	"""
	def __init__(self, tweet_graph):
		self._queue = deque([])
		self._tweet_graph = tweet_graph

	def _get_max_ts(self):
		if len(self._queue) == 0:
			return None
		else:
			return self._queue[-1][0]

	def add_to_queue(self, hashtags, created_at):
		if len(hashtags) == 0:
			return
		# This tweet is discarded because it has not effect to the graph
		if self._get_max_ts() is not None and created_at < self._get_max_ts() - timedelta(seconds=60):
			return

		# Ensure there is no duplicate in hashtags
		hashtags = list(set(hashtags))
		# Queue is either empty or in order
		if len(self._queue) == 0 or self._get_max_ts() <= created_at:
			self._update_queue(hashtags, created_at)
		# Queue is out of order
		else:
			inorder_list = deque([])
			while self._get_max_ts() > created_at:
				inorder_list.appendleft(self._queue.pop())
			self._update_queue(hashtags, created_at)
			self._queue.extend(inorder_list)

	def _update_queue(self, hashtags, created_at):
		# Remove the older tweets from the front of the queue
		if len(self._queue) > 0:
			while self._queue[0][0] + timedelta(seconds=60) < created_at:
				old_tuple = self._queue.popleft()
				#print('popping: ' + str(old_tuple[0]))
				self._remove_hashtags(old_tuple[1], old_tuple[0])

		self._queue.append((created_at, hashtags))
		self._add_hashtags(hashtags, created_at)

	def _add_hashtags(self, hashtags, created_at):
		if len(hashtags) == 0:
			return
		if len(hashtags) == 1:
			self._tweet_graph.add_vertex(hashtags[0], created_at)
			return

		for i in range(len(hashtags)-1):
			for j in range(i+1, len(hashtags)):
				self._tweet_graph.add_edge(hashtags[i], hashtags[j], created_at)

	def _remove_hashtags(self, hashtags, created_at):
		if len(hashtags) == 0:
			return
		if len(hashtags) == 1:
			self._tweet_graph.remove_vertex(hashtags[0], created_at)
			return

		for i in range(len(hashtags)-1):
			for j in range(i+1, len(hashtags)):
				self._tweet_graph.remove_edge(hashtags[i], hashtags[j], created_at)

	def print_queue(self):
		print([repr(q) for q in self._queue])

class TweetLoader(object):
	""" 
	A helper class for coverting a json file.
	"""

	def process_tweets(self, input_filename, output_filename):
		""" 
		"""

		graph = TweetGraph()
		queue = TweetQueue(graph)
		try:
			with open(input_filename, 'r') as json_file, open(output_filename, 'w') as output_file:
				linecount = 0
				for line in json_file:

					try:
						tweet_json = json.loads(line)
						if 'limit' in tweet_json.keys():
							print('skipping line: ', linecount)
							continue

						created_at = parser.parse(tweet_json['created_at'])
						hashtags = [h['text'] for h in tweet_json['entities']['hashtags']]
						#print(linecount, repr(created_at), repr(hashtags))
						queue.add_to_queue(hashtags, created_at)
						output_file.write(str(graph.compute_avg_degree())+'\n')
					except:
						logging.exception(str(linecount) + ': Unexpected error')
						output_file.write('ERROR\n')
						continue
					finally:
						linecount+=1
		except:
			print("Unexpected error:", sys.exc_info()[0])
			raise


if __name__ == '__main__':
	if len(sys.argv) < 3:
		raise InputError('Missing file arguments')
	print('input' + sys.argv[1])
	print('output' + sys.argv[2])
	TweetLoader().process_tweets(sys.argv[1], sys.argv[2])
