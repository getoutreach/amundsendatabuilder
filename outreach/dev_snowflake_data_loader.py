
# TODO second pass, remove sqlachemy connector use snowflake packages

import logging
import os
from pyhocon import ConfigFactory
import uuid
import yaml

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.extractor.snowflake_metadata_extractor import SnowflakeMetadataExtractor
from databuilder.extractor.snowflake_table_last_updated_extractor import SnowflakeTableLastUpdatedExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from elasticsearch.client import Elasticsearch
from databuilder.transformer.base_transformer import NoopTransformer


def get_connection_string(*, database, warehouse):
    user = os.environ["SNOWFLAKEUSER"]
    password = os.environ["SNOWFLAKEPASSWORD"]
    account = os.environ["SNOWFLAKEACCOUNT"]
    conn_string = f'snowflake://{user}:{password}@{account}/{database}?warehouse={warehouse}'
    return conn_string


def create_snowflake_metadata_job(*, database, ignore_schemas, conn_string, host, neo4j, **kwargs):
    node_files_folder = host["node_files_folder"]
    relationship_files_folder = host["relationship_files_folder"]
    where_clause = f"WHERE c.TABLE_SCHEMA not in (\'{', '.join(ignore_schemas)}\')"
    task = DefaultTask(extractor=SnowflakeMetadataExtractor(), loader=FsNeo4jCSVLoader())
    job_config = ConfigFactory.from_dict({
        f'extractor.snowflake.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': conn_string,
        f'extractor.snowflake.{SnowflakeMetadataExtractor.SNOWFLAKE_DATABASE_KEY}': database,
        f'extractor.snowflake.{SnowflakeMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.FORCE_CREATE_DIR}': True,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j["endpoint"],
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j["user"],
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j["password"],
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag'
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    return job


def create_snowflake_last_update_job(*, database, ignore_schemas, conn_string, host, neo4j, **kwargs):
    node_files_folder = host["node_files_folder"]
    relationship_files_folder = host["relationship_files_folder"]
    where_clause = f"WHERE t.TABLE_SCHEMA not in (\'{', '.join(ignore_schemas)}\')"
    task = DefaultTask(extractor=SnowflakeTableLastUpdatedExtractor(), loader=FsNeo4jCSVLoader())
    job_config = ConfigFactory.from_dict({
        f'extractor.snowflake_table_last_updated.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': conn_string,
        f'extractor.snowflake_table_last_updated.{SnowflakeTableLastUpdatedExtractor.SNOWFLAKE_DATABASE_KEY}': database,
        f'extractor.snowflake_table_last_updated.{SnowflakeTableLastUpdatedExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.FORCE_CREATE_DIR}': True,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j["endpoint"],
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j["user"],
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j["password"],
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag'
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    return job


def create_es_publisher_job(*, elasticsearch, host, neo4j, **kwargs):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_search_index`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param cypher_query:               Query handed to the `Neo4jSearchDataExtractor` class, if None is given (default)
                                       it uses the `Table` query baked into the Extractor
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    elasticsearch_client = Elasticsearch([{'host': elasticsearch["host"]}])
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())
    data_path = host["es_data_path"]
    job_config = ConfigFactory.from_dict({
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.GRAPH_URL_CONFIG_KEY}': neo4j["endpoint"],
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.MODEL_CLASS_CONFIG_KEY}': 'databuilder.models.table_elasticsearch_document.TableESDocument',
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_USER}': neo4j["user"],
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_PW}': neo4j["password"],
        f'loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY}': data_path,
        f'loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY}': 'w',
        f'publisher.elasticsearch.{ElasticsearchPublisher.FILE_PATH_CONFIG_KEY}': data_path,
        f'publisher.elasticsearch.{ElasticsearchPublisher.FILE_MODE_CONFIG_KEY}': 'r',
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY}': elasticsearch_client,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY}': elasticsearch_new_index_key,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY}': 'table',
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY}': 'table_search_index',
    })
    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    return job


if __name__ == "__main__":
    logging.basicConfig(
        level='INFO'
        , format='%(asctime)s - %(name)s - %(levelname)s : %(message)s'
        , datefmt='%m/%d/%Y %I:%M:%S %p'
        , handlers=[logging.StreamHandler()]
        )
    cfg = """
        job:
            name: Snowflake Amundsen Load Job
        host:
            data_folder: /var/tmp/amundsen/tables
            node_files_folder: /var/tmp/amundsen/tables/nodes
            relationship_files_folder: /var/tmp/amundsen/tables/relationships
            es_data_path: /var/tmp/amundsen/search_data.json
        snowflake:
            warehouse: amundsen_wh
            databases:
                - internal_data
                - vendor_data
            ignore_schemas:
                - INFORMATION_SCHEMA
        neo4j:
            endpoint: bolt://localhost:7687
            user: neo4j
            password: test
        elasticsearch:
            host: localhost
        """
    cfg_dict = yaml.safe_load(cfg)
    for database in cfg_dict["snowflake"]["databases"]:
        conn_string = get_connection_string(database=database, warehouse=cfg_dict["snowflake"]["warehouse"])
        metadata_job = create_snowflake_metadata_job(database=database, ignore_schemas=cfg_dict["snowflake"]["ignore_schemas"],
                                                     conn_string=conn_string, **cfg_dict)
        metadata_job.launch()
        update_job = create_snowflake_last_update_job(database=database, ignore_schemas=cfg_dict["snowflake"]["ignore_schemas"],
                                                      conn_string=conn_string, **cfg_dict)
        update_job.launch()
    es_job = create_es_publisher_job(**cfg_dict)
    es_job.launch()
