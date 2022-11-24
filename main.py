import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from beam_methods import *

pipeline_options = PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipeline_options)##define a quantidade de recursos e capacidade de processamento

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> ReadFromText("./data/casos_dengue.txt", skip_header_lines=1)
    | "De texto para lista" >> beam.Map(lambda record: record.split("|"))
    | "De lista para dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano mes" >> beam.Map(trata_data)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue) #FlatMap retorna diversos elementos numa mesma execução
    | "Soma de casos por chave" >> beam.CombinePerKey(sum)
    # | "Resultado:" >> beam.Map(print)
)

chuvas = (
    pipeline
    |"Leitura do dataset de chuvas" >> ReadFromText("./data/chuvas.csv", skip_header_lines=1)
    |"De texto para lista chuvas" >> beam.Map(lambda record: record.split(","))
    |"Criando a chave UF-ANO-MES" >> beam.Map(chave_uf_ano_mes_de_list)
    |"soma de casos por chave" >> beam.CombinePerKey(sum)
    | "Arrendondar resultados de chuvas" >> beam.Map(arredonda)
    # |"Resultado chuvas:" >> beam.Map(print)
)

resultado = (
    # (chuvas, dengue)
    # |"Unir PCollections" >> beam.Flatten()
    # |"Realizar Agrupamento" >> beam.GroupByKey()
    ({"chuvas": chuvas, "dengue": dengue})
    |"Mesclar pcols" >> beam.CoGroupByKey()
    |"Filtrar dados" >> beam.Filter(filtra_campos_vazios)
    |"Desagrupar elementos" >> beam.Map(descompactar_elementos)
    |"Preparar CSV" >> beam.Map(preparar_csv)
    # |"Mostrar resultados da uniao" >> beam.Map(print)
)

header = "UF;ANO;MES;CHUVA;DENGUE"

resultado | "Criar arquivo CSV" >> WriteToText("./consolidated/resultado", file_name_suffix="csv", num_shards=1, header=header)

pipeline.run()
