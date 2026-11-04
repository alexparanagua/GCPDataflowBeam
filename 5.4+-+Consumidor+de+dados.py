# consumer

import csv
import time
from google.cloud import pubsub_v1
import os


serviceAccount = r'/home/alex-lima/Downloads/curso-dataflow-beam-512320-7819b1637de9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

subscription = 'projects/curso-dataflow-beam-512320/subscriptions/MeusVoos-sub'
subscriber = pubsub_v1.SubscriberClient()

def monstrar_msg(mensagem):
  print(('Mensagem: {}'.format(mensagem)))
  mensagem.ack()

subscriber.subscribe(subscription,callback=monstrar_msg)

while True:
  time.sleep(3)