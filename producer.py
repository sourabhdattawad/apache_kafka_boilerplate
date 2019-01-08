from kafka import KafkaProducer


class producer(object):
	"""docstring for producer"""
	def __init__(self):
		super(producer, self).__init__()
		self.producer =  KafkaProducer(bootstrap_servers='localhost:9092')

	def publish_message(self, topic, message):

		self.producer.send(topic, message)
		self.producer.flush()



producer = producer()

producer.publish_message('raw', b'Hello, World!')
producer.publish_message('raw2', b'Hello, World!')
producer.publish_message('raw', b'Hello, World!')
producer.publish_message('raw', b'Hello, World!')
producer.publish_message('raw2', b'Hello, World!')


		
