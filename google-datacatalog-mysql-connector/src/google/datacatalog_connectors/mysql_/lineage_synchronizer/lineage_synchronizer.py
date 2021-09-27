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

import logging
from google.datacatalog_connectors.mysql_.lineage_synchronizer.ingest \
    import IngestLineage

from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape \
    import table_lineage_scraper


class lineageSynchronizer:

    def __init__(self, connection_args, project_id, location_id):
        self.connection_args = connection_args
        self.project_id = project_id
        self.location_id = location_id

    def run(self):
        logging.info('\n\n==============Scrape metadata===============')
        scraper = self._get_scraper()(connection_args=self.connection_args)
        lineage_data = scraper.scrape()

        logging.info('\n============End-lineage-datacatalog============')
        self._get_ingest()(self.project_id,
                           self.location_id).ingest(lineage_data)

    def _get_scraper(self):
        return table_lineage_scraper.tableLineageScraper

    def _get_prepare(self):
        raise NotImplementedError(
            'Implementing this method is required to connect to a RDBMS!')

    def _get_ingest(self):
        return IngestLineage
