#pip install google-cloud-pubsub
#producer

import csv
import time
from google.cloud import pubsub_v1
import os

serviceAccount = r'/home/alex-lima/Downloads/curso-dataflow-beam-512320-7819b1637de9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

topico = 'projects/curso-dataflow-beam-512320/topics/MeusVoos'
publisher = pubsub_v1.PublisherClient()

entrada = r"/home/alex-lima/Documentos/GCPDataflowBeam/voos_sample.csv"     

with open(entrada, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(topico,row)
        time.sleep(2)