import os

import apache_beam as beam
import logging

logging.getLogger("apache_beam").setLevel(logging.ERROR)


serviceAccount = r'/home/alex-lima/Downloads/curso-dataflow-beam-512320-7819b1637de9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

p1 = beam.Pipeline()


class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            try:
                if int(record[8]) > 0:
                    yield record
            except ValueError:
                pass


dados_filtrados = (
    p1
    | "Importar Dados" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines=1)
    | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com atraso" >> beam.ParDo(filtro())
)

Tempo_Atrasos = (
    dados_filtrados
    | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Somar por key" >> beam.CombinePerKey(sum)
    #  | "Mostrar Resultados" >> beam.Map(print)
)

Qtd_Atrasos = (
    dados_filtrados
    | "Criar par Qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Contar por key" >> beam.combiners.Count.PerKey()
    #  | "Mostrar Resultados QTD" >> beam.Map(print)
)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | "Group By" >> beam.CoGroupByKey()
    | "Saida para GCP" >> beam.io.WriteToText(r"gs://gcpdataflowbeam/Arq_saida_atrasos.csv")
)

p1.run().wait_until_finish()
