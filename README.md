# Apache Beam

O Apache Beam trata toda a lógica de processos internamente e deixa transparente como que o processo vai ser paralelizado para quem desenvolve.

Ele também permite que ajustes sejam feitos para que, em processos específicos, a pessoa que desenvolve consiga configurar como isso vai acontecer e possa ajustar melhor como a sua pipeline vai ser executada.

Por isso, o processo feito utilizando o Apache Beam pode ser executado utilizando diversas ferramentas que são definidas como RUNNERS.

Em cada runner, conseguimos configurar melhor esses parâmetros e tirar melhor proveito de toda essa ferramenta.

Para processos em geral, e principalmente, para quem está começando, isso não vai fazer tanta diferença, já que no nosso projeto utilizaremos apenas o DirectRunner para execução local. No entanto, para quem tem curiosidade e quer complementar seus estudos, é possível conferir como tudo isso acontece.

ruinners: https://beam.apache.org/documentation/runners/


direct_runner: https://beam.apache.org/documentation/runners/direct/


A prática é muito importante para fixação dos conteúdos estudados. Chegou a hora de você seguir todos os passos realizados por mim durante esta aula:

Importar o Apache Beam e os módulos necessários para criar a pipeline e realizar a leitura do arquivo;
Criar a pcollection, com nome dengue.
Caso já tenha feito, excelente. Se ainda não, é importante que você implemente o que foi visto em vídeo para poder continuar com a próxima aula, que tem como pré-requisito todo o código escrito nesta.

Se, por acaso, você já domina essa parte, no início de cada aula (a partir da segunda), você poderá baixar o projeto feito até aquele ponto.

Lembre-se que você pode personalizar seu projeto como quiser e testar alternativas à vontade. O importante é acompanhar a construção do projeto na prática, a partir do que é estudado em aula.

VER OPINIÃO DO INSTRUTOR
Opinião do instrutor

Para definir trazer o Apache Beam para o nosso ambiente, nós realizamos a instalação do módulo via pip e agora precisamos trazê-lo para o nosso código.

Vamos então importá-lo junto com os módulos necessários para criar a pipeline e realizar a leitura do arquivo.

from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)COPIAR CÓDIGO
Agora, podemos criar a pcollection, com nome dengue para receber o resultado das etapas que vamos aplicar partindo da leitura do arquivo.

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >>
        ReadFromText('sample_casos_dengue.txt', skip_header_lines=1)
    | "Mostrar resultados" >> beam.Map(print))COPIAR CÓDIGO
Como iremos iniciar o processo, a pcollection de dengue passa a pipeline que criamos como parâmetro e após isso utilizamos | (pipe) para definir uma etapa da execução.

Lembrando que uma pcollection pode ter várias etapas, cada etapa iniciando com o | seguido do módulo do apache beam que realizará um processo que afetará o resultado da pcollection.

Quando o processo é executado ele mostrará a linha lida, pois na etapa "Mostrar resultados" aplicamos o método print do Python dentro do Map, que percorrerá cada linha do arquivo lido e aplicará a função passada, mostrando o que foi lido no terminal.

O gabarito deste exercício é o passo a passo demonstrado no vídeo. Você também pode conferir o código no projeto encontrado no início da aula seguinte.

Tenha certeza de que tudo está certo antes de continuar. Ficou com dúvida? Podemos te ajudar pelo nosso fórum. Lembre-se que nele você também pode ajudar e ter ajuda de colegas de curso, incentivando o espírito de comunidade :)


BEAM MAP

https://beam.apache.org/documentation/transforms/python/elementwise/map/

Utilizar o FlatMap para criar geradores é algo bem comum na engenharia de dados. Para entender melhor como utilizar essa ferramenta nos seus próximos projetos, a seguir, você encontra o exemplo de uma pipeline que recebe 2 listas de diferentes tamanhos, e que ao final apresenta uma pcollection com apenas 1 item da lista por elemento.

Para criar isso, podemos utilizar o FlatMap que realiza essa função muito bem, da seguinte forma:

import apache_beam as beam

def gera_elementos(elementos):
    for elemento in elementos:
        yield elemento

with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Lista de números' >> beam.Create([
            [2, 1, 6],
            [5, 3],
        ])
        | 'Tirar da lista com gerador' >> beam.FlatMap(gera_elementos)
        | beam.Map(print))
>>
2
1
6
5
3COPIAR CÓDIGO
No método gera_elementos(), o retorno do valor utiliza o yield, que tem resultado bem parecido com o return. Porém, ele não encerra a execução ao ser chamado. Se substituíssemos o yiled pelo return o resultado do código seria:

import apache_beam as beam

def gera_elementos(elementos):
    for elemento in elementos:
        return elemento

with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Lista de números' >> beam.Create([
            [2, 1, 6],
            [5, 3],
        ])
        | 'Tirar da lista com gerador' >> beam.FlatMap(gera_elementos)
        | beam.Map(print))
>>
2COPIAR CÓDIGO
Pois ao ser invocado, ele termina a execução do método onde é chamado, assim não resultando no que necessitamos dentro do contexto de retirarmos os elementos das listas.

Já o yield vai ser invocado até o final do laço onde está sendo utilizado. Ao final do laço, ele retornará todos os elementos que passaram por ele dentro do laço em que foi utilizado.

No link a seguir você pode saber mais sobre geradores e como utilizá-los nos seus projetos, que podem ajudar a resolver alguns tipos de problemas de maneira bem mais fácil e rápida.