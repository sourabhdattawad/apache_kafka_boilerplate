from multiprocessing import Process
from kafka import KafkaConsumer, KafkaProducer



class consumer(object):
	"""docstring for consumer"""
	def __init__(self, access):
		super(consumer, self).__init__()
		self.producer =  KafkaProducer(bootstrap_servers='localhost:9092')

		self.publish_access = access

	def create_consumer(self,topic,  ):
		return KafkaConsumer(topic)


	def read_message(self, consumer, consumer_obj):

		for message in consumer:
			print(message)
			if consumer_obj.publish_access:
				self.publish_to_topic('parsed', b'parsed_string')


	def publish_to_topic(self, topic, message):

		self.producer =  KafkaProducer(bootstrap_servers='localhost:9092')
		self.producer.send(topic, message)
		self.producer.flush()




consumer_obj_1 = consumer(True)
consumer_obj_2 = consumer(True)
consumer_obj_3 = consumer(False)

c1 = consumer_obj_1.create_consumer('raw')
c2 = consumer_obj_2.create_consumer('raw2')
c3 = consumer_obj_3.create_consumer('parsed')

t1 = Process(target=consumer_obj_1.read_message, args=(c1,consumer_obj_1,))
t2 = Process(target=consumer_obj_2.read_message, args=(c2,consumer_obj_2,))
t3 = Process(target=consumer_obj_3.read_message, args=(c3,consumer_obj_3,))

t2.start()
t1.start()
t3.start()


		
