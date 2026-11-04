from datetime import date
import os

import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from google.oauth2 import service_account

logging.getLogger("apache_beam").setLevel(logging.ERROR)
logging.getLogger("apache_beam").setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

serviceAccount = r'/home/alex-lima/Downloads/curso-dataflow-beam-512320-7819b1637de9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

pipeline_options = PipelineOptions.from_dictionary({
    'project': 'curso-dataflow-beam-512320',
    'region': 'us-central1',
    'runner': 'DataflowRunner',
    'temp_location': 'gs://gcpdataflowbeam/temp',
    'staging_location': 'gs://gcpdataflowbeam/temp',
    # 'job_name': 'dataflow-beam-512320',
    'template_location': 'gs://gcpdataflowbeam/template/template_dataflow_beam_512320'
})

p1 = beam.Pipeline(options=pipeline_options)


class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            try:
                if int(record[8]) > 0:
                    yield record
            except ValueError:
                pass


print("Iniciando o pipeline com o gerenciador de contexto...")
with beam.Pipeline(options=pipeline_options) as p1:
    dados_filtrados = (
        p1
        | "Importar Dados" >> beam.io.ReadFromText(r"gs://gcpdataflowbeam/entrada/voos_sample.csv", skip_header_lines=1)
        | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
        | "Pegar voos com atraso" >> beam.ParDo(filtro())
    )

    Tempo_Atrasos = (
        dados_filtrados
        | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
        | "Somar por key" >> beam.CombinePerKey(sum)
    )

    Qtd_Atrasos = (
        dados_filtrados
        | "Criar par Qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
        | "Contar por key" >> beam.combiners.Count.PerKey()
    )

    tabela_atrasos = (
        {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
        | "Group By" >> beam.CoGroupByKey()
        | "Saida para GCP" >> beam.io.WriteToText(r"gs://gcpdataflowbeam/saida/Arq2_saida_atrasos"+date.today().strftime("%Y-%m-%d %H-%M-%S")+".csv")
        # | "Mostrar Resultados QTD" >> beam.Map(print)
    )

print("Processamento concluído com sucesso!")
