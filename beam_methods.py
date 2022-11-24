import re

colunas_dengue = [
    "id",
    "data_iniSE",
    "casos",
    "ibge_code",
    "cidade",
    "uf",
    "cep",
    "latitude",
    "longitude"
]

def lista_para_dicionario(record, colunas_dengue):
    return dict(zip(colunas_dengue, record))

def trata_data(record):
    """
    recebe um dicionatio e cria um novo campo ANO-MES
    retorna o mesmo dicionario com um novo campo
    """
    record["ano_mes"] = "-".join(record["data_iniSE"].split("-")[:2])
    return record
    
def chave_uf(record):
    """
    Receber um dicionario
    Retornar uma tupla com os estado(UF) e o elemento (UF, dicionario)
    """

    chave = record["uf"]
    return (chave, record)

def casos_dengue(record):
    """
    Recebe uma tupla ("RS", [{},{}])
    Retornar uma tupla ("RS-2O14-12", 8.0)
    """
    uf, registros = record
    for registro in registros:
        if bool(re.search(r"\d", registro["casos"])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro["casos"]))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)
            
def arredonda(record):
    """
    Recebe uma tupla ('PA-2019-06', 2364.000000000003)
    Retorna uma tupla com o valor arredondado ('PA-2019-06', 2364.0)
    """
    chave, mm = record
    return (chave, round(mm, 1))

def chave_uf_ano_mes_de_list(record):
    """
    Receber uma lista de elementos
    Retorna uma tupla contento uma chave e o valor de chuva em mm
    ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = record
    ano_mes = "-".join(data.split("-")[:2])
    chave = f"{uf}-{ano_mes}"
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm
    
def filtra_campos_vazios(record):
    """
    Remove elementos que tenha chaves vazias
    """
    chave, dados = record
    if all([
        dados["chuvas"],
        dados["dengue"]
    ]):
        return True
    return False
    
def descompactar_elementos(record):
    chave, dados = record
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split("-")
    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(record, delimitador=";"):
    """
       Recebe tupla ('CE', '2015', '05', 16.4, 15872.0)
       Retorna uma string delimitada "CE'; '2015'; '05'; 16.4; 15872.0"
    """
    
    return f"{delimitador}".join(record)