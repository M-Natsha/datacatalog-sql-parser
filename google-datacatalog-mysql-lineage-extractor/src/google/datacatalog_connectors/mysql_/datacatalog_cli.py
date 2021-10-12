#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import sys
import logging
from google.datacatalog_connectors.mysql_.parse.parse_sql import MySqlParser
from .metadata_scraper import MetadataScraper
from .lineage_extractor.asset_level_lineage_scraper import AssetLevelLinneagScraper
from google.datacatalog_connectors.mysql_.parse.parse_sql import MySqlParser

class MySQL2DatacatalogCli():
    def run(self, argv):
        """Runs the command line."""

        args = self._parse_args(argv)
        # Enable logging
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        logging.info('\n\n==============Starting CLI===============')
        
        logging.info('\n\n========== Creating connection ===========')
        # connect to database
        scraper = MetadataScraper()
        con = scraper._create_rdbms_connection(self._get_connection_args(args))
        
        logging.info('\n\n========== Connection created ===========')
        
        # get logs with limit
        scraper = AssetLevelLinneagScraper(con)
        scraper.scrape()
        # parse and print
        
        logging.info('\n\n============== Ending CLI ===============')
        
        
        

    def _get_connection_args(self, args):
        return {
            'database': args.mysql_database,
            'host': args.mysql_host,
            'user': args.mysql_user,
            'pass': args.mysql_pass
        }

    def _parse_args(self, argv):
        parser = argparse.ArgumentParser(
            description='Command line to sync mysql '
            'metadata to Datacatalog')
        parser.add_argument(
            '--mysql-host',
            help='Your mysql server host, this is required even'
            ' for the raw_metadata_csv,'
            ' so we are able to map the created entries'
            ' resource with the mysql host',
            required=True)
        parser.add_argument('--mysql-user', help='Your mysql credentials user')
        parser.add_argument('--mysql-pass',
                            help='Your mysql credentials password')
        parser.add_argument('--mysql-database',
                            help='Your mysql database name')
        return parser.parse_args(argv)


def main():
    argv = sys.argv
    MySQL2DatacatalogCli().run(argv[1:] if len(argv) > 0 else argv)


def exctract_lineage_argv():
    argv = sys.argv[1:]
    if(len(argv) < 1):
        print("Provid Sql query that you want to parse")
        return
    
    sql_query = argv[0]
    extracted = MySqlParser().parse_query(sql_query)
    print(extracted)