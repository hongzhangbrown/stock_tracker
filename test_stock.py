#!/usr/bin/env python
from vatic_code_test import *
from StringIO import StringIO
import unittest


class TestVatic(unittest.TestCase):


	def setUp(self):
		self.stock_tracker = StockTracker()
		
	def test_trade(self):
		# one closing trade cancels multiple open trade with carryover
		quote = QuoteMessage(2,'ABC',10.06,10.07)
		self.stock_tracker.process_quote_message(quote)

		trade = TradeMessage(2,'ABC','B',10.06,0)
		self.send_trade_message(trade)

		quote = QuoteMessage(2,'ABC',10.07,10.08)
		self.stock_tracker.process_quote_message(quote)

		trade = TradeMessage(3,'ABC','B',10.06,200)
		self.send_trade_message(trade)

		quote = QuoteMessage(2,'ABC',10.05,10.06)
		self.stock_tracker.process_quote_message(quote)

		trade = TradeMessage(4,'ABC','B',10.06,300)
		self.send_trade_message(trade)

		trade = TradeMessage(5,'ABC','S',10.06,300)
		self.send_trade_message(trade)

		# one closing trade cancels multiple open trade without carryover
		trade = TradeMessage(6,'ABC','S',10.06,350)
		self.send_trade_message(trade)

		trade = TradeMessage(7,'ABC','B',10.06,400)
		self.send_trade_message(trade)

	def send_trade_message(self, trade):
		self.stock_tracker.process_trade_message(trade)
		self.print_stock_tracker()


	def print_stock_tracker(self):
		for symbol in self.stock_tracker.stocks:
			print 'Stock: ' + symbol
			stock = self.stock_tracker.stocks[symbol]
			for trade in stock.trades:
				print '\t Trade: time ' + str(trade.time) + '; price ' + str(trade.price) + '; quantity ' + str(trade.quantity) + '; side ' + trade.side + \
				'; bid ' + str(trade.bid) + '; ask ' + str(trade.ask) + '; liquidity ' + trade.liquidity
	 
if __name__ == "__main__":
	unittest.main()
