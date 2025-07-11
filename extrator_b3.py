import requests
import pandas as pd
import json
import base64
from datetime import datetime

def buscar_dados_ibovespa():
    print("Iniciando a busca de dados da carteira do IBOV...")

    try:
        url_api = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"
        payload = {"index": "IBOV", "language": "pt-br"}
        payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
        url_completa = f"{url_api}{payload_b64}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url_completa, headers=headers)
        response.raise_for_status()
        print("Dados recebidos com sucesso da API.")

        dados_json = response.json()
        lista_acoes = dados_json.get('results')

        if not lista_acoes:
            print("A chave 'results' não foi encontrada ou está vazia no JSON retornado.")
            return None

        df = pd.DataFrame(lista_acoes)
        print(f"DataFrame criado com {df.shape[0]} linhas e {df.shape[1]} colunas.")

        # --- ESTA É A LINHA CORRIGIDA ---
        data_de_hoje_str = datetime.now().strftime('%Y-%m-%d')
        df['data_pregao'] = data_de_hoje_str
        print(f"Coluna 'data_pregao' adicionada como TEXTO com a data: {data_de_hoje_str}")
        # ----------------------------------

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
    """
    if df is None or df.empty:
        print("O DataFrame está vazio. Nenhum arquivo será salvo.")
        return

    try:
        caminho_csv = f"{nome_base}.csv"
        df.to_csv(caminho_csv, index=False, encoding='utf-8-sig')
        print(f"\nArquivo salvo com sucesso em: {caminho_csv}")

        caminho_parquet = f"{nome_base}.parquet"
        df.to_parquet(caminho_parquet, index=False, engine='pyarrow')
        print(f"Arquivo salvo com sucesso em: {caminho_parquet}")

    except Exception as e:
        print(f"Erro ao salvar os arquivos: {e}")


if __name__ == "__main__":
    dataframe_ibov = buscar_dados_ibovespa()

    if dataframe_ibov is not None:
        data_atual = datetime.now().strftime('%Y-%m-%d')
        nome_arquivo_base = f"composicao_ibov_{data_atual}"
        salvar_dados(dataframe_ibov, nome_arquivo_base)