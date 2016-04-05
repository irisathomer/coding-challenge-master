import unittest
from average_degree import TweetGraph, TweetQueue
from dateutil import parser
from decimal import Decimal

class TestTweetGraph(unittest.TestCase):
	''' Unit test for average_degree. I ran out of time here'''

	def test_graph_simple(self):
		g = TweetGraph()
		t0 = parser.parse('Thu Mar 24 17:51:10 +0000 2016')
		g.add_edge('Spark', 'Apache', t0)
		self.assertEquals(1.00, g.compute_avg_degree())
		#g.print_graph()
		t1 = parser.parse('Thu Mar 24 17:51:15 +0000 2016')
		g.add_edge('Apache', 'Hadoop', t1)
		g.add_edge('Apache', 'Storm', t1)
		g.add_edge('Hadoop', 'Storm', t1)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t2 = parser.parse('Thu Mar 24 17:51:30 +0000 2016')
		g.add_vertex('Apache', t2)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t3 = parser.parse('Thu Mar 24 17:51:55 +0000 2016')
		g.add_edge('Flink', 'Spark', t3)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t4 = parser.parse('Thu Mar 24 17:51:58 +0000 2016')
		g.add_edge('Spark', 'HBase', t4)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()

		t5 = parser.parse('Thu Mar 24 17:52:12 +0000 2016')
		g.add_edge('Hadoop', 'Apache', t5)
		g.remove_edge('Spark', 'Apache', t0)
		#g.print_graph()
		self.assertEquals(Decimal('1.66'), g.compute_avg_degree())

	def test_graph_out_of_order(self):
		g = TweetGraph()
		t0 = parser.parse('Thu Mar 24 17:51:10 +0000 2016')
		g.add_edge('Spark', 'Apache', t0)
		self.assertEquals(1.00, g.compute_avg_degree())
		#g.print_graph()
		t1 = parser.parse('Thu Mar 24 17:51:15 +0000 2016')
		g.add_edge('Apache', 'Hadoop', t1)
		g.add_edge('Apache', 'Storm', t1)
		g.add_edge('Hadoop', 'Storm', t1)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t2 = parser.parse('Thu Mar 24 17:51:30 +0000 2016')
		g.add_vertex('Apache', t2)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t3 = parser.parse('Thu Mar 24 17:51:55 +0000 2016')
		g.add_edge('Flink', 'Spark', t3)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t4 = parser.parse('Thu Mar 24 17:51:58 +0000 2016')
		g.add_edge('Spark', 'HBase', t4)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()

		t5 = parser.parse('Thu Mar 24 17:52:12 +0000 2016')
		g.add_edge('Hadoop', 'Apache', t5)
		g.remove_edge('Spark', 'Apache', t0)
		self.assertEquals(Decimal('1.66'), g.compute_avg_degree())
		#g.print_graph()
		
		t6 = parser.parse('Thu Mar 24 17:51:57 +0000 2016')
		g.add_edge('Flink', 'HBase', t6)
		self.assertEquals(Decimal('2.00'), g.compute_avg_degree())
		#g.print_graph()

	def test_queue_simple(self):
		g = TweetGraph()
		q = TweetQueue(g)
		t0 = parser.parse('Thu Mar 24 17:51:10 +0000 2016')
		q.add_to_queue(['Spark','Apache'], t0)
		self.assertEquals(1.00, g.compute_avg_degree())
		#g.print_graph()
		t1 = parser.parse('Thu Mar 24 17:51:15 +0000 2016')
		q.add_to_queue(['Apache','Hadoop','Storm'], t1)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t2 = parser.parse('Thu Mar 24 17:51:30 +0000 2016')
		q.add_to_queue(['Apache'], t2)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t3 = parser.parse('Thu Mar 24 17:51:55 +0000 2016')
		q.add_to_queue(['Flink','Spark'], t3)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t4 = parser.parse('Thu Mar 24 17:51:58 +0000 2016')
		q.add_to_queue(['Spark','HBase'], t4)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()

		t5 = parser.parse('Thu Mar 24 17:52:12 +0000 2016')
		q.add_to_queue(['Hadoop','Apache'], t5)
		#g.print_graph()
		self.assertEquals(Decimal('1.66'), g.compute_avg_degree())

	def test_queue_emtpy(self):
		''' Test empty hashtags'''
		g = TweetGraph()
		q = TweetQueue(g)
		t0 = parser.parse('Thu Mar 24 17:51:10 +0000 2016')
		q.add_to_queue(['Spark','Apache'], t0)
		self.assertEquals(1.00, g.compute_avg_degree())
		#g.print_graph()
		t1 = parser.parse('Thu Mar 24 17:51:15 +0000 2016')
		q.add_to_queue(['Apache','Hadoop','Storm'], t1)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()

		# This should pop the t0 tweet
		t2 = parser.parse('Thu Mar 24 17:52:10 +0000 2016')
		q.add_to_queue([], t2)
		#g.print_graph()
		self.assertEquals(2.00, g.compute_avg_degree())
		self.assertEquals(3, len(g._vertices))
		self.assertEquals(2, len(q._queue))

		# This should pop the t1 tweet
		t3 = parser.parse('Thu Mar 24 17:52:15 +0000 2016')
		q.add_to_queue([], t3)
		#g.print_graph()
		self.assertEquals(0.00, g.compute_avg_degree())
		self.assertEquals(0, len(g._vertices))
		self.assertEquals(2, len(q._queue))

	def test_queue_duplicate(self):
		''' Test duplicate edges'''
		g = TweetGraph()
		q = TweetQueue(g)
		t0 = parser.parse('Thu Mar 24 17:51:10 +0000 2016')
		q.add_to_queue(['Spark','Apache'], t0)
		self.assertEquals(1.00, g.compute_avg_degree())
		#g.print_graph()
		t1 = parser.parse('Thu Mar 24 17:51:15 +0000 2016')
		q.add_to_queue(['Apache','Hadoop','Storm'], t1)
		self.assertEquals(2.00, g.compute_avg_degree())
		self.assertEquals(4, len(g._vertices))
		self.assertEquals(2, len(q._queue))
		#g.print_graph()

		q.add_to_queue(['Apache','Hadoop'], t1)
		self.assertEquals(2.00, g.compute_avg_degree())
		self.assertEquals(4, len(g._vertices))
		self.assertEquals(3, len(q._queue))

		t2 = parser.parse('Thu Mar 24 17:52:10 +0000 2016')
		q.add_to_queue([], t2)
		#g.print_graph()
		self.assertEquals(2.00, g.compute_avg_degree())
		self.assertEquals(3, len(g._vertices))
		self.assertEquals(3, len(q._queue))

		t3 = parser.parse('Thu Mar 24 17:52:15 +0000 2016')
		q.add_to_queue([], t3)
		#g.print_graph()
		self.assertEquals(0.00, g.compute_avg_degree())
		self.assertEquals(0, len(g._vertices))
		self.assertEquals(0, len(g._graph))
		self.assertEquals(2, len(q._queue))

	def test_queue_out_of_order(self):
		g = TweetGraph()
		self.assertEquals(Decimal('0.00'), g.compute_avg_degree())

		q = TweetQueue(g)
		t0 = parser.parse('Thu Mar 24 17:51:10 +0000 2016')
		q.add_to_queue(['Spark','Apache'], t0)
		self.assertEquals(1.00, g.compute_avg_degree())
		#g.print_graph()
		t1 = parser.parse('Thu Mar 24 17:51:15 +0000 2016')
		q.add_to_queue(['Apache','Hadoop','Storm'], t1)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t2 = parser.parse('Thu Mar 24 17:51:30 +0000 2016')
		q.add_to_queue(['Apache'], t2)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t3 = parser.parse('Thu Mar 24 17:51:55 +0000 2016')
		q.add_to_queue(['Flink','Spark'], t3)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()
		t4 = parser.parse('Thu Mar 24 17:51:58 +0000 2016')
		q.add_to_queue(['Spark','HBase'], t4)
		self.assertEquals(2.00, g.compute_avg_degree())
		#g.print_graph()

		t5 = parser.parse('Thu Mar 24 17:52:12 +0000 2016')
		q.add_to_queue(['Hadoop','Apache'], t5)
		#g.print_graph()
		#q.print_queue()
		self.assertEquals(Decimal('1.66'), g.compute_avg_degree())
		self.assertEquals(5, len(q._queue))

		# This is out-of-order
		t6 = parser.parse('Thu Mar 24 17:52:10 +0000 2016')
		q.add_to_queue(['Flink', 'HBase'], t6)
		#g.print_graph()
		#q.print_queue()
		self.assertEquals(Decimal('2.00'), g.compute_avg_degree())
		self.assertEquals(6, len(q._queue))
		# Check that t5 is after t6
		self.assertEquals(t1, q._queue[0][0])
		self.assertEquals(t5, q._queue[-1][0])
		self.assertEquals(t6, q._queue[-2][0])

		# This is discarded
		t7 = parser.parse('Thu Mar 24 17:51:10 +0000 2016')
		q.add_to_queue(['Cassandra', 'NoSQL'], t7)
		#g.print_graph()
		#q.print_queue()
		self.assertEquals(Decimal('2.00'), g.compute_avg_degree())
		self.assertEquals(6, len(q._queue))

		t8 = parser.parse('Thu Mar 24 17:52:20 +0000 2016')
		q.add_to_queue(['Kafka', 'Apache'], t8)
		#g.print_graph()
		#q.print_queue()
		self.assertEquals(Decimal('1.66'), g.compute_avg_degree())
		self.assertEquals(6, len(q._queue))
		self.assertEquals(t2, q._queue[0][0])
		self.assertEquals(t8, q._queue[-1][0])

if __name__ == '__main__':
    unittest.main()