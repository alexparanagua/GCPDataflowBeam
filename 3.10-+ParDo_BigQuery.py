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
    'template_location': 'gs://gcpdataflowbeam/template/batch_job_bigquery_beam_512320',
    'save_main_session': True
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


def criar_dict_nivel1(record):
    dict_ = {}
    dict_['airport'] = record[0]
    dict_['lista'] = record[1]
    return (dict_)


def desaninhar_dict(record):
    def expand(key, value):
        if isinstance(value, dict):
            return [(key + '_' + k, v) for k, v in desaninhar_dict(value).items()]
        else:
            return [(key, value)]
    items = [item for k, v in record.items() for item in expand(k, v)]
    return dict(items)


def criar_dict_nivel0(record):
    dict_ = {}
    dict_['airport'] = record['airport']
    dict_['lista_Qtd_Atrasos'] = record['lista_Qtd_Atrasos'][0]
    dict_['lista_Tempo_Atrasos'] = record['lista_Tempo_Atrasos'][0]
    return (dict_)


table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
tabela = 'curso-dataflow-beam-512320.curso_dataflow_voos.curso_dataflow_vooos_atraso'

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
        | beam.CoGroupByKey()
        | beam.Map(lambda record: criar_dict_nivel1(record))
        | beam.Map(lambda record: desaninhar_dict(record))
        | beam.Map(lambda record: criar_dict_nivel0(record))
        | beam.io.WriteToBigQuery(
            table=tabela,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location='gs://gcpdataflowbeam/temp'
        )
    )

p1.run().wait_until_finish()
print("Processamento concluído com sucesso!")
