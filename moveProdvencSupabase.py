"""
Script: moveProdvencSupabase.py
Objetivo: Mover dados de produtos com vencimento do MongoDB (estoque_centralizado) para PostgreSQL (Supabase)
Ambientes:
  - Origem: MongoDB (estoque_centralizado.prodvenc)
  - Destino: PostgreSQL/Supabase (estoquecentralizado.produtovencer)
Versao: 1.0
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import datetime
import logging

import psycopg2
from psycopg2 import sql


# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ========== CONFIGURACOES MONGODB ==========
MONGO_DESTINO = "mongodb+srv://mateus_lemos:pXqgrqsWz0nqOqQtV588@estoque-centralizado.gwuokd.mongodb.net/?"
BASE_DESTINO = "estoque_centralizado"
COLLECTION_ORIGEM = "prodvenc"

# ========== CONFIGURACOES SUPABASE/POSTGRES ==========
# Informe a string de conexao do Supabase diretamente aqui.
SUPABASE_DATABASE_URL = "postgresql://postgres.ulpppmzsxjbsqiyfskwi:estoquecentralizado@aws-1-sa-east-1.pooler.supabase.com:6543/postgres"
POSTGRES_SCHEMA = "estoquecentralizado"
POSTGRES_TABLE = "produtovencer"

TAMANHO_LOTE = 2000


def conectar_mongodb():
    """Conecta ao MongoDB de origem dos dados"""
    try:
        logger.info("[MONGODB] Conectando ao MongoDB de origem dos dados...")
        client = MongoClient(MONGO_DESTINO, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        logger.info("[MONGODB] + Conectado com sucesso!")
        return client
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"[MONGODB] - Erro ao conectar: {e}")
        return None


def conectar_postgres():
    """Conecta ao PostgreSQL/Supabase"""
    if not SUPABASE_DATABASE_URL:
        logger.error("[POSTGRES] - SUPABASE_DATABASE_URL nao configurada no codigo")
        return None

    try:
        logger.info("[POSTGRES] Conectando ao PostgreSQL/Supabase...")
        conn = psycopg2.connect(SUPABASE_DATABASE_URL)
        conn.autocommit = False
        logger.info("[POSTGRES] + Conectado com sucesso!")
        return conn
    except psycopg2.Error as e:
        logger.error(f"[POSTGRES] - Erro ao conectar: {e}")
        return None


def garantir_schema_e_tabela(conn):
    """Cria schema/tabela no formato equivalente ao Oracle, se nao existirem"""
    create_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(
        sql.Identifier(POSTGRES_SCHEMA)
    )

    create_table = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {}.{} (
            id TEXT PRIMARY KEY,
            lojaid INTEGER NOT NULL,
            produtoid INTEGER NOT NULL,
            datavencimento DATE NULL,
            saldo NUMERIC NULL,
            stativo INTEGER NOT NULL,
            dt_criacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            dt_atualizacao TIMESTAMPTZ NULL
        );
        """
    ).format(sql.Identifier(POSTGRES_SCHEMA), sql.Identifier(POSTGRES_TABLE))

    with conn.cursor() as cursor:
        cursor.execute(create_schema)
        cursor.execute(create_table)

    conn.commit()
    logger.info(f"[POSTGRES] + Garantido {POSTGRES_SCHEMA}.{POSTGRES_TABLE}")


def contar_documentos(collection):
    """Conta quantos documentos estao na collection"""
    try:
        logger.info(f"\n[CONTAGEM] Contando documentos em {BASE_DESTINO}.{COLLECTION_ORIGEM}...")
        total = collection.count_documents({})
        logger.info(f"[CONTAGEM] + Total de documentos: {total}")
        return total
    except Exception as e:
        logger.error(f"[CONTAGEM] - Erro ao contar: {e}")
        return 0


def extrair_documentos(collection, skip, limit):
    """Extrai documentos do MongoDB com paginacao"""
    try:
        return list(collection.find({}).skip(skip).limit(limit))
    except Exception as e:
        logger.error(f"[EXTRACAO] - Erro ao extrair documentos: {e}")
        return []


def _to_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_date(value):
    if not value:
        return None

    if isinstance(value, datetime.datetime):
        return value.date()

    if isinstance(value, datetime.date):
        return value

    return None


def converter_para_postgres(doc_mongo):
    """Converte documento MongoDB para formato PostgreSQL"""
    try:
        return {
            'id': str(doc_mongo.get('_id', '')),
            'lojaid': _to_int(doc_mongo.get('lojaid', 0)),
            'produtoid': _to_int(doc_mongo.get('produtoid', 0)),
            'datavencimento': _to_date(doc_mongo.get('dtvencimento')),
            'saldo': _to_float(doc_mongo.get('saldo', 0)),
            'stativo': _to_int(doc_mongo.get('stativo', 0)),
        }
    except Exception as e:
        logger.error(f"[CONVERSAO] - Erro ao converter documento: {e}")
        return None


def inserir_lote_postgres(conn, documentos_lote):
    """Insere/atualiza lote de documentos no PostgreSQL com ON CONFLICT"""
    if not documentos_lote:
        return 0, 0

    total_upsert = 0
    total_erro = 0

    upsert_query = sql.SQL(
        """
        INSERT INTO {}.{} (
            id, lojaid, produtoid, datavencimento, saldo, stativo, dt_criacao, dt_atualizacao
        )
        VALUES (
            %(id)s, %(lojaid)s, %(produtoid)s, %(datavencimento)s, %(saldo)s, %(stativo)s, NOW(), NULL
        )
        ON CONFLICT (id) DO UPDATE SET
            saldo = EXCLUDED.saldo,
            datavencimento = EXCLUDED.datavencimento,
            stativo = EXCLUDED.stativo,
            dt_atualizacao = NOW();
        """
    ).format(sql.Identifier(POSTGRES_SCHEMA), sql.Identifier(POSTGRES_TABLE))

    cursor = conn.cursor()
    try:
        for doc in documentos_lote:
            try:
                cursor.execute(upsert_query, doc)
                total_upsert += 1
            except psycopg2.Error as e:
                logger.warning(f"[INSERCAO] - Erro ao inserir documento {doc.get('id')}: {e}")
                total_erro += 1

        conn.commit()
        return total_upsert, total_erro
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"[INSERCAO] - Erro ao inserir lote: {e}")
        return 0, len(documentos_lote)
    finally:
        cursor.close()


def gerar_relatorio(total_mongodb, total_upsert, total_erro):
    """Gera relatorio final da migracao"""
    logger.info(f"\n{'='*80}")
    logger.info("RELATORIO FINAL DA MIGRACAO")
    logger.info(f"{'='*80}")
    logger.info(f"Total de documentos no MongoDB:      {total_mongodb}")
    logger.info(f"Total inseridos/atualizados no PG:   {total_upsert}")
    logger.info(f"Total de erros:                      {total_erro}")
    logger.info(f"{'='*80}\n")


def main():
    logger.info("="*80)
    logger.info("MIGRACAO DE DADOS - Produtos com Vencimento (MongoDB -> Supabase/PostgreSQL)")
    logger.info("Versao: 1.0")
    logger.info("="*80)

    client_mongo = conectar_mongodb()
    if not client_mongo:
        logger.error("Abortando: nao foi possivel conectar ao MongoDB")
        return

    postgres_conn = conectar_postgres()
    if not postgres_conn:
        logger.error("Abortando: nao foi possivel conectar ao PostgreSQL/Supabase")
        client_mongo.close()
        return

    try:
        garantir_schema_e_tabela(postgres_conn)

        db = client_mongo[BASE_DESTINO]
        collection = db[COLLECTION_ORIGEM]

        total_documentos = contar_documentos(collection)
        if total_documentos == 0:
            logger.info("Nenhum documento para migrar!")
            return

        logger.info(f"\n[INICIO] Iniciando migracao de {total_documentos} documento(s)...")
        logger.info(f"[LOTES] Tamanho de cada lote: {TAMANHO_LOTE}\n")

        total_upsert_geral = 0
        total_erro_geral = 0

        num_lote = 0
        total_lotes = (total_documentos + TAMANHO_LOTE - 1) // TAMANHO_LOTE

        for skip in range(0, total_documentos, TAMANHO_LOTE):
            num_lote += 1

            documentos_mongo = extrair_documentos(collection, skip, TAMANHO_LOTE)
            if not documentos_mongo:
                logger.warning(f"[LOTE {num_lote}/{total_lotes}] Nenhum documento extraido")
                continue

            logger.info(f"[LOTE {num_lote}/{total_lotes}] Extraido(s): {len(documentos_mongo)} documento(s)")

            documentos_postgres = []
            for doc_mongo in documentos_mongo:
                doc_pg = converter_para_postgres(doc_mongo)
                if doc_pg:
                    documentos_postgres.append(doc_pg)

            if not documentos_postgres:
                logger.warning(f"[LOTE {num_lote}/{total_lotes}] Nenhum documento convertido")
                continue

            logger.info(f"[LOTE {num_lote}/{total_lotes}] Convertido(s): {len(documentos_postgres)} documento(s)")

            total_upsert_lote, total_erro_lote = inserir_lote_postgres(postgres_conn, documentos_postgres)
            total_upsert_geral += total_upsert_lote
            total_erro_geral += total_erro_lote

            logger.info(
                f"[LOTE {num_lote}/{total_lotes}] + Inserido/Atualizado: {total_upsert_lote} / Erro(s): {total_erro_lote}\n"
            )

        gerar_relatorio(total_documentos, total_upsert_geral, total_erro_geral)

    except Exception as e:
        logger.error(f"[ERRO] Erro durante migracao: {e}")
    finally:
        postgres_conn.close()
        client_mongo.close()
        logger.info("[FIM] Conexoes fechadas. Migracao concluida!")


if __name__ == "__main__":
    main()
