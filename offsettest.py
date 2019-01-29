from confluent_kafka import Consumer, KafkaError
def on_assign(c,part):
  print ("assign", part)

def on_commit(err, part):
  print ("commit", part)
c = Consumer({
  'bootstrap.servers':'localhost:9092',
  'group.id':'test-consumer-group',
  'default.topic.config': {'auto.offset.reset':'smallest', 'auto.commit.enable':False },
  'topic.metadata.refresh.interval.ms': 20000,
  'on_commit':on_commit
})

c.subscribe(['NBC_APPS.TBL_MS_DIVISION'],on_assign=on_assign)

while True:
  msg = c.poll(timeout=1.0)
  if msg is None:
    continue


print ("Received message with key: %s"%msg.key())
c.close()
exit()
