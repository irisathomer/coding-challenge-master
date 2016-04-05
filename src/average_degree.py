#!/usr/bin/env python

from __future__ import print_function
from collections import deque
from datetime import datetime, timedelta
from dateutil import parser
from decimal import Decimal, ROUND_DOWN
import json
import sys
import logging

'''Program calculates the average degree of hashtags. 

	TweetLoader is the main class that processes an input file of tweets and outputs the average degree to an output file.
	TweetQueue is a helper class that represents the 60-second window of tweets.
	TweetGraph is a helper class that represents the hashtag graph.'''

TWOPLACES = Decimal(10) ** -2

class TweetGraph(object):
	"""
	An undirected graph representation of the hashtags.

	The state maintains 2 dictionaries: 
		1. self._graph is an adjacency list (using a dict of dict) that maintains all the edges. Because it's an undirected graph,
			an edge between V1 and V2 will appear twice, in self._graph[V1][V2] and self._graph[V2][V1]. The 'weight' of the edge is
			the maximum timestamp of the tweet that the edge appears in.
		2. self._vertices is a dictionary of vertex and the maximum timestamp of the tweet that the vertex appears in.

		add/remove operation to the graph is O(1) runtime. compute_avg_degree is O(V) runtime.
	"""
	def __init__(self):
		self._graph = {}
		self._vertices = {}

	def add_vertex(self, vertex, ts):
		""" Adds a vertex only to the graph. Use add_edge() if you want to add an edge.
		"""
		if vertex not in self._vertices:
			self._vertices[vertex] = ts
			self._graph[vertex] = {}
		elif ts > self._vertices[vertex]:
			self._vertices[vertex] = ts			

	def _update_edge_for_add(self, from_vertex, to_vertex, ts):
		""" If there is already an edge, update the timestamp if ts is newer. Adds a new edge if needed.
		"""
		if to_vertex in self._graph[from_vertex]:
			if self._graph[from_vertex][to_vertex] > ts:
				#do nothing because the edge timestamp is newer.
				return

		self._graph[from_vertex][to_vertex] = ts

	def add_edge(self, vertex1, vertex2, ts):
		""" Adds an edge between vertex1 and vertex2. This adds the vertices if needed and also adds bidirectional edges.
		"""
		self.add_vertex(vertex1, ts)
		self.add_vertex(vertex2, ts)

		self._update_edge_for_add(vertex1, vertex2, ts)
		self._update_edge_for_add(vertex2, vertex1, ts)

	# Proper clean up expects edges to be removed first
	def remove_vertex(self, vertex, ts):
		""" Removes a vertex only from the graph. Use remove_edge() if you want to remove an edge.
			This guarantees a vertex is removed only if ts >= the timestamp of the vertex.
		"""
		if vertex in self._vertices and self._vertices[vertex] <= ts:
			del(self._vertices[vertex])
		if vertex not in self._vertices and vertex in self._graph and len(self._graph[vertex]) == 0:
			del(self._graph[vertex])

	def _update_edge_for_remove(self, from_vertex, to_vertex, ts):
		if to_vertex not in self._graph[from_vertex]:
			return

		# If there is no newer edge, we remove it
		if self._graph[from_vertex][to_vertex] <= ts:
			del(self._graph[from_vertex][to_vertex])

	def remove_edge(self, vertex1, vertex2, ts):
		""" Adds an edge between vertex1 and vertex2. It also cleans up the vertices if needed and handles the bidirectionality.
			This guarantees an edge or a vertex is removed only if ts >= the timestamp of the edge or vertex.
		"""
		self._update_edge_for_remove(vertex1, vertex2, ts)
		self._update_edge_for_remove(vertex2, vertex1, ts)

		self.remove_vertex(vertex1, ts)
		self.remove_vertex(vertex2, ts)

	def print_graph(self):
		""" Helper function to see the state of the graph """
		print(self.compute_avg_degree())
		for k, v in self._graph.iteritems():
			print(k, [k1.encode('utf-8') + ', ' + str(v1) for k1, v1 in v.iteritems()])
		for k, v in self._vertices.iteritems():
			print(k, str(v))

	def compute_avg_degree(self):
		""" Computes the average degree of the graph."""
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
	A queue that manages the 60-second window of tweets and enforces the temporal order within the queue. 
	It also updates _tweet_graph as the window slids.

	The state maintains:
	1. A queue that only contains 60 seconds of tweets, in temporal order. Each element is a tuple of (timestamp, [hashtags])
	2. A TweetGraph

	This is implemented using the python deque structure, which has a pointer to the left and right end of the queue. 
		Assuming that for each new in-order tweet, we need to remove k tweets with h hashtags, then the average runtime is O(kh^2). 
	The scanning of the 60-second window is O(1).
		However, the worst case is when a tweet is out of order, we need to pop the queue to the right insertion point, which can be O(n)
	if n is the size of the queue. The assumption is that this is infrequent. If that is not true, then one can do a binary search on the
	queue to find the insertion point as an improvement.
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
		# This tweet is discarded because it is earlier than the 60-second window and has no effect to the graph
		if self._get_max_ts() is not None and created_at < self._get_max_ts() - timedelta(seconds=60):
			return

		# Ensure there is no duplicate in hashtags
		hashtags = list(set(hashtags))

		# Queue is either empty or in order
		if self._get_max_ts() is None or self._get_max_ts() <= created_at:
			self._update_queue(hashtags, created_at)
		else:
		# Queue is out of order. We pop the queue from the right until we find an insertion point, then we add the elements back.
		# This is to maintain temporal order.
			inorder_list = deque([])
			while self._get_max_ts() is not None and self._get_max_ts() > created_at:
				inorder_list.appendleft(self._queue.pop())
			self._update_queue(hashtags, created_at)
			self._queue.extend(inorder_list)

	def _update_queue(self, hashtags, created_at):
		# Remove the older tweets from the front of the queue and from the graph. Exclusive window.
		while len(self._queue) > 0 and self._queue[0][0] + timedelta(seconds=60) <= created_at:
			old_tuple = self._queue.popleft()
			#print('popping: ' + str(old_tuple[0]))
			self._remove_hashtags(old_tuple[1], old_tuple[0])

		# Add the tweet to the queue and also to the graph
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
	Main class that processes an input file of tweets and outputs the average degree to an output file.
	"""

	def process_tweets(self, input_filename, output_filename):
		""" 
		Reads from input_filename, computes the average degree of the 60-second window and writes to output_filename.
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
