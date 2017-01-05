#!/usr/bin/env python

from collections import namedtuple
import sys

# The Trade namedtuple is used by the Stock class to keep track of information about 
# each open / closing trade
Trade = namedtuple('Trade',['time','price','quantity','side','bid','ask','liquidity'])

# Each TradeMessage / QuoteMessage stores information from one line in the input data file
TradeMessage = namedtuple('TradeMessage',['time','symbol','side','price','quantity'])
QuoteMessage = namedtuple('QuoteMessage', ['time','symbol','bid','ask'])


class Stock:
	""" Keeps track of up-to-date bid / ask prices and open trades

		Attributes:
		symbol: symbol of the stock
		trades: a FIFO structure that holds all open trades
		curr_bid / curr_ask: current bid and ask prices
	"""

 	def __init__(self, symbol):
	 	self.symbol = symbol
		self.trades = []
		self.curr_bid = 0.0
		self.curr_ask = 0.0

	# updates the tracked bid / ask prices given the incoming information
	def handle_quote_update(self, new_bid, new_ask):
		self.curr_bid = new_bid
		self.curr_ask = new_ask

	# processes a trade and updates the open trades accordingly
	def process_trade(self, time, side, price, quantity):
		if quantity == 0:
			return
		new_trade = Trade(time, price, quantity, side, self.curr_bid, self.curr_ask, self.compute_liquidity(side, price))
		# check if open / close trade
		# create a new open trade if the incoming trade is on the same side as the trades in the queue or the queue is empty
		if len(self.trades) == 0 or side == self.trades[0].side:
			self.trades.append(new_trade)
		# create paired trades otherwise
		else:
			remaining_quantity = quantity
			while len(self.trades) > 0 and remaining_quantity > 0:
				# handles the case when the first open trade is not completely consumed by the closing trade
				if self.trades[0].quantity > remaining_quantity:
					new_open_trade = self.trades[0]._replace(quantity=self.trades[0].quantity-remaining_quantity)
					self.create_paired_trades(self.trades[0], new_trade, remaining_quantity)
					self.trades[0] = new_open_trade
					remaining_quantity = 0
				# consumes the first open trade completely
				else:
					open_trade = self.trades.pop(0)
					self.create_paired_trades(open_trade, new_trade, open_trade.quantity)
					remaining_quantity -= open_trade.quantity
			# create a new open trade if all open trades are consumed and the incoming trade still has leftover
			if remaining_quantity > 0:
				new_open_trade = new_trade._replace(quantity=remaining_quantity)
				self.trades.append(new_open_trade)

	def compute_liquidity(self, side, price):
		if (side == 'B' and price <= self.curr_bid) or (side == 'S' and price >= self.curr_ask):
			return 'P'
		elif (side == 'S' and price <= self.curr_bid) or (side == 'B' and price >= self.curr_ask):
			return 'A'
		else:
			return 'n/a'

	# This function uses most information from open_trade and close_trade (both are Trade namedtuple) to generate information
	# for the paired trades, except that it discards the quantity info from open_trade / close_trade and uses 
	# the quantity parameter as the quantity for the paired trades
	def create_paired_trades(self, open_trade, close_trade, quantity):
		pnl = quantity * round(close_trade.price - open_trade.price, 2)
		if open_trade.side == 'S':
			pnl *= -1
		pnl += 0 # make sure we don't have "-0.0"
		paired_trade = [str(open_trade.time), str(close_trade.time), self.symbol, str(quantity), '%.2f' % pnl, open_trade.side, close_trade.side, \
		 '%.2f' % open_trade.price, '%.2f' % close_trade.price, '%.2f' % open_trade.bid, '%.2f' % close_trade.bid, \
		 '%.2f' % open_trade.ask, '%.2f' % close_trade.ask, \
		 open_trade.liquidity, close_trade.liquidity]
		print ','.join(paired_trade)


class StockTracker:
	""" Keeps track of a set of Stock objects, each managing its own trade information;
		this class takes in messages containing quote / trade updates, instructs 
		the corresponding Stock object to perform the appropriate opperation, 
		and creates new Stock objects when needed

		Attributes:
		stocks: a dictionary that stores a set of Stock objects that appear in the messages, 
		indexed by the stock symbols
	"""

 	def __init__(self):
		self.stocks = {}
	
	def process_quote_message(self, quote_message):
		self.init_stock_if_new(quote_message.symbol)
		self.stocks[quote_message.symbol].handle_quote_update(quote_message.bid, quote_message.ask)

	def process_trade_message(self, trade_message):
		self.init_stock_if_new(trade_message.symbol)
		self.stocks[trade_message.symbol].process_trade(trade_message.time, trade_message.side, trade_message.price, trade_message.quantity)

	def init_stock_if_new(self, symbol):
		if symbol not in self.stocks:
			self.stocks[symbol] = Stock(symbol)


class MessageDispatcher:
	""" Extract data from data files and parse data into QuoteMessage / TradeMessage.
		Dispatch the messages in temporal order to the StockTracker.

		Attributes:
		trades_file: string, the path of the file with trade transactions
		quotes_file: string, the path of the file with quote updates
	"""

	def __init__(self, trades_file, quotes_file):
		""" Initialize an instance containing file pathes
		"""
		self.trades_file = trades_file
		self.quotes_file = quotes_file		

	def dispatch_messages(self):
		""" Extract data from the two files. Parse the data and 
			generate information in temporal order
		""" 

		with open(self.trades_file,'r') as trades, open(self.quotes_file,'r') as quotes:
			trades.readline()
			quotes.readline()
			curr_trade = self.string_to_message(trades.readline())
			curr_quote = self.string_to_message(quotes.readline())
			end_of_quotes = False
			while True:
				# Keeps processing records from one file until the timestamp catches 
				# up with the timestamp of the top entry from the other file
				# If a quote message and a trade message comes in at the same time,
				# process the quote message first
				if curr_trade.time < curr_quote.time or end_of_quotes:
					stockTracker.process_trade_message(curr_trade)
					raw_trade_line = trades.readline()
					if not raw_trade_line:
						break
					curr_trade = self.string_to_message(raw_trade_line)
				else:
					stockTracker.process_quote_message(curr_quote)
					raw_quote_line = quotes.readline()
					if not raw_quote_line:
						end_of_quotes = True
						continue
					curr_quote = self.string_to_message(raw_quote_line)


	
	@staticmethod
	def string_to_message(string):
		""" Given a string, parse the string into namedtuples 
			Return:	
				a namedtuple QuoteMessage / TradeMessage
		"""
		message_ = string.strip('\n').split(',')
		if len(message_) != 5 and len(message_) != 4:
			raise ValueError('Unknown type of message')
		if len(message_) == 5:
			time, symbol, side, price, quantity = message_
			try:
				time = int(time)
				price = float(price)
				quantity = int(quantity)
				message = TradeMessage(time, symbol, side, price, quantity)
			except ValueError:
				print 'Unknown message'
		elif len(message_) == 4:
			time, symbol, bid, ask = message_
			try:	
				time = int(time)
				bid = float(bid)
				ask = float(ask)
				message = QuoteMessage(time, symbol, bid, ask)
			except ValueError:
				print 'Unknown message'
		return message


if __name__ == "__main__":

	if len(sys.argv) < 3:
		print 'Program requires two arguments: [quote file name] [trade file name]'
		sys.exit(-1)

	quotes_file = sys.argv[1]
	trades_file = sys.argv[2]

	print 'OPEN_TIME,CLOSE_TIME,SYMBOL,QUANTITY,PNL,OPEN_SIDE,CLOSE_SIDE,OPEN_PRICE,CLOSE_PRICE,OPEN_BID,CLOSE_BID,OPEN_ASK,CLOSE_ASK,OPEN_LIQUIDITY,CLOSE_LIQUIDITY'

	stockTracker = StockTracker()
	md = MessageDispatcher(trades_file,quotes_file)
	md.dispatch_messages()




