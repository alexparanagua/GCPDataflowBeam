import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

serviceAccount = r'/home/alex-lima/Downloads/curso-dataflow-beam-512320-7819b1637de9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

pipeline_options = {
    'project': 'curso-dataflow-beam-512320',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://gcpdataflowbeam/temp',
    'temp_location': 'gs://gcpdataflowbeam/temp',
    'template_location': 'gs://gcpdataflowbeam/template/streaming_tumbling_fixed',
    'save_main_session': True,
    'streaming': True}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

subscription = 'projects/curso-dataflow-beam-512320/subscriptions/MeusVoos-sub'
saida = 'projects/curso-dataflow-beam-512320/topics/saidaVoos'


class separar_linhas(beam.DoFn):
    def process(self, record):
        return [record.decode("utf-8").split(',')]


class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]


pcollection_entrada = (
    p1 | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(
        subscription=subscription)
)

Tempo_Atrasos = (
    pcollection_entrada
    | "Separar por Vírgulas Atraso" >> beam.ParDo(separar_linhas())
    | "Pegar voos com atraso" >> beam.ParDo(filtro())
    | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Window" >> beam.WindowInto(window.FixedWindows(5))
    | "Somar por key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    pcollection_entrada
    | "Separar por Vírgulas Qtd" >> beam.ParDo(separar_linhas())
    | "Pegar voos com Qtd" >> beam.ParDo(filtro())
    | "Criar par Qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Window Atr" >> beam.WindowInto(window.FixedWindows(5))
    | "Contar por key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | "join Atrasos e Qtd" >> beam.CoGroupByKey()
    | 'Converting to byte String' >> beam.Map(lambda row: (''.join(str(row)).encode('utf-8')))
    | "Escrever no Tópico" >> beam.io.WriteToPubSub(saida)
)

result = p1.run()
result.wait_until_finish()
