import requests
import pandas as pd
import json
import base64
from datetime import datetime

def buscar_dados_ibovespa():
    print("Iniciando a busca de dados da carteira do IBOV...")

    try:
        url_api = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"

        # O payload precisa ser um JSON convertido para Base64, assim como o navegador faz.
        payload = {"index": "IBOV", "language": "pt-br"}
        payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()

        # Monta a URL final da requisição
        url_completa = f"{url_api}{payload_b64}"
        print(f"URL da API construída: {url_completa}")

        # Simula um cabeçalho de navegador para evitar bloqueios.
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # Faz a requisição GET para a API
        response = requests.get(url_completa, headers=headers)

        # Levanta um erro para respostas com status de erro (4xx ou 5xx)
        response.raise_for_status()
        print("Dados recebidos com sucesso da API.")

        # Converte a resposta JSON em um dicionário Python
        dados_json = response.json()

        # Extrai a lista de ações da chave 'results'
        lista_acoes = dados_json.get('results')

        if not lista_acoes:
            print("A chave 'results' não foi encontrada ou está vazia no JSON retornado.")
            return None

        # Cria um DataFrame do Pandas a partir da lista de ações
        df = pd.DataFrame(lista_acoes)
        print(f"DataFrame criado com {df.shape[0]} linhas e {df.shape[1]} colunas.")

        # Pré-visualização das 5 primeiras linhas do DataFrame
        print("\nPré-visualização dos dados:")
        print(df.head())

        return df

    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer a requisição HTTP: {e}")
        return None
    except json.JSONDecodeError:
        print("Erro ao decodificar a resposta JSON. A resposta pode não ser um JSON válido.")
        return None
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        return None

def salvar_dados(df: pd.DataFrame, nome_base: str):
    """
    Salva o DataFrame em arquivos CSV e Parquet.

    Args:
        df (pd.DataFrame): O DataFrame a ser salvo.
        nome_base (str): O nome base para os arquivos de saída (sem a extensão).
    """
    if df is None or df.empty:
        print("O DataFrame está vazio. Nenhum arquivo será salvo.")
        return

    try:
        # Salvar em formato CSV
        caminho_csv = f"{nome_base}.csv"
        df.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
        print(f"\nArquivo salvo com sucesso em: {caminho_csv}")

        # Salvar em formato Parquet
        caminho_parquet = f"{nome_base}.parquet"
        df.to_parquet(caminho_parquet, index=False, engine='pyarrow')
        print(f"Arquivo salvo com sucesso em: {caminho_parquet}")

    except Exception as e:
        print(f"Erro ao salvar os arquivos: {e}")


if __name__ == "__main__":
    # Busca os dados da B3
    dataframe_ibov = buscar_dados_ibovespa()

    # Se os dados foram buscados com sucesso, prossiga para salvá-los
    if dataframe_ibov is not None:
        # Cria um nome de arquivo base com a data atual
        data_atual = datetime.now().strftime('%Y-%m-%d')
        nome_arquivo_base = f"composicao_ibov_{data_atual}"

        # Salva o DataFrame nos formatos desejados
        salvar_dados(dataframe_ibov, nome_arquivo_base) 