
# TODO second pass, remove sqlachemy connector use snowflake packages

import logging
import os
from pyhocon import ConfigFactory
import uuid
import yaml

from databuilder.extractor.dashboard.tableau.tableau_dashboard_extractor import TableauDashboardExtractor

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


def create_tableau_metadata_job(*, host, neo4j, tableau, **kwargs):
    node_files_folder = host["node_files_folder"]
    relationship_files_folder = host["relationship_files_folder"]
    job_config = ConfigFactory.from_dict({
        'extractor.tableau_dashboard_metadata.tableau_host': tableau["host"],
        'extractor.tableau_dashboard_metadata.api_version': tableau["api_version"],
        'extractor.tableau_dashboard_metadata.site_name': tableau["site_name"],
        'extractor.tableau_dashboard_metadata.tableau_personal_access_token_name': tableau["token_name"],
        'extractor.tableau_dashboard_metadata.tableau_personal_access_token_secret': tableau["token_secret"],
        'extractor.tableau_dashboard_metadata.api_base_url': tableau["host"],
        # 'extractor.tableau_dashboard_metadata.excluded_projects': tableau_excluded_projects,
        # 'extractor.tableau_dashboard_metadata.cluster': tableau_dashboard_cluster,
        # 'extractor.tableau_dashboard_metadata.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_metadata.transformer.timestamp_str_to_epoch.timestamp_format': "%Y-%m-%dT%H:%M:%SZ",
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
    task = DefaultTask(extractor=TableauDashboardExtractor(), loader=FsNeo4jCSVLoader())
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    job.launch()


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
        tableau:
            host: https://tableau.outreach-internal.com
            site_name: Outreach General
            api_version: 3.8
            token_name: api
            token_secret: KfL1PkCCSeuw2hqhu5qF8Q==:ZHAIKuPEQ2AE7v5548nrRLAUwcp5YJJD
        neo4j:
            endpoint: bolt://localhost:7687
            user: neo4j
            password: test
        elasticsearch:
            host: localhost
        """
    cfg_dict = yaml.safe_load(cfg)
    metadata_job = create_tableau_metadata_job(**cfg_dict)
    metadata_job.launch()
    es_job = create_es_publisher_job(**cfg_dict)
    es_job.launch()
