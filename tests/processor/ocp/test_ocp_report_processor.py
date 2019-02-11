#
# Copyright 2018 Red Hat, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

"""Test the OCPReportProcessor."""
import csv
import copy
import gzip
import json
import shutil
import tempfile

from sqlalchemy.sql.expression import delete

from masu.database import OCP_REPORT_TABLE_MAP
from masu.database.ocp_report_db_accessor import OCPReportDBAccessor
from masu.database.reporting_common_db_accessor import ReportingCommonDBAccessor
from masu.exceptions import MasuProcessingError
from masu.external import GZIP_COMPRESSED, UNCOMPRESSED
from masu.processor.ocp.ocp_report_processor import OCPReportProcessor, OCPReportProcessorError, OCPReportTypes, ProcessedOCPReport
from tests import MasuTestCase


class ProcessedOCPReportTest(MasuTestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the test class with required objects."""
        cls.report = ProcessedOCPReport()

    def test_remove_processed_rows(self):
        test_entry = {'test': 'entry'}
        self.report.report_periods.update(test_entry)
        self.report.line_items.append(test_entry)
        self.report.reports.update(test_entry)

        self.report.remove_processed_rows()

        self.assertEqual(self.report.report_periods, {})
        self.assertEqual(self.report.line_items, [])
        self.assertEqual(self.report.reports, {})


class OCPReportProcessorTest(MasuTestCase):
    """Test Cases for the OCPReportProcessor object."""

    @classmethod
    def setUpClass(cls):
        """Set up the test class with required objects."""
        # These test reports should be replaced with OCP reports once processor is impelmented.
        cls.test_report = './tests/data/ocp/e6b3701e-1e91-433b-b238-a31e49937558_February-2019-my-ocp-cluster-1.csv'
        cls.storage_report = './tests/data/ocp/e6b3701e-1e91-433b-b238-a31e49937558_storage.csv'
        cls.unknown_report = './tests/data/test_cur.csv'
        cls.test_report_gzip = './tests/data/test_cur.csv.gz'

        cls.ocp_processor = OCPReportProcessor(
            schema_name='acct10001',
            report_path=cls.test_report,
            compression=UNCOMPRESSED,
            provider_id=1
        )

        with ReportingCommonDBAccessor() as report_common_db:
            cls.column_map = report_common_db.column_map

        cls.accessor = OCPReportDBAccessor('acct10001', cls.column_map)
        cls.report_schema = cls.accessor.report_schema
        cls.session = cls.accessor._session

        _report_tables = copy.deepcopy(OCP_REPORT_TABLE_MAP)
        cls.report_tables = list(_report_tables.values())

        # Grab a single row of test data to work with
        with open(cls.test_report, 'r') as f:
            reader = csv.DictReader(f)
            cls.row = next(reader)

    @classmethod
    def tearDownClass(cls):
        cls.accessor.close_connections()
        cls.accessor.close_session()

    def tearDown(self):
        """Return the database to a pre-test state."""
        self.session.rollback()

        for table_name in self.report_tables:
            self.accessor._cursor.execute(f'DELETE FROM {table_name}')
        self.accessor._pg2_conn.commit()

        self.ocp_processor._processor.processed_report.remove_processed_rows()

        self.ocp_processor._processor.line_item_columns = None

    def test_initializer(self):
        """Test initializer."""
        self.assertIsNotNone(self.ocp_processor._processor._schema_name)
        self.assertIsNotNone(self.ocp_processor._processor._report_path)
        self.assertIsNotNone(self.ocp_processor._processor._compression)

    def test_initializer_unsupported_compression(self):
        """Assert that an error is raised for an invalid compression."""
        with self.assertRaises(MasuProcessingError):
            OCPReportProcessor(schema_name='acct10001',
                               report_path=self.test_report,
                               compression='unsupported',
                               provider_id=1)

    def test_detect_report_type(self):
        usage_processor = OCPReportProcessor(
            schema_name='acct10001',
            report_path=self.test_report,
            compression=UNCOMPRESSED,
            provider_id=1
        )
        self.assertEqual(usage_processor.report_type, OCPReportTypes.CPU_MEM_USAGE)

        storage_processor = OCPReportProcessor(
            schema_name='acct10001',
            report_path=self.storage_report,
            compression=UNCOMPRESSED,
            provider_id=1
        )
        self.assertEqual(storage_processor.report_type, OCPReportTypes.STORAGE)

        with self.assertRaises(OCPReportProcessorError):
            OCPReportProcessor(
                schema_name='acct10001',
                report_path=self.unknown_report,
                compression=UNCOMPRESSED,
                provider_id=1
            )

    def test_process_default(self):
        """Test the processing of an uncompressed file."""
        counts = {}
        processor = OCPReportProcessor(
            schema_name='acct10001',
            report_path=self.test_report,
            compression=UNCOMPRESSED,
            provider_id=1
        )
        report_db = self.accessor
        report_schema = report_db.report_schema
        for table_name in self.report_tables:
            table = getattr(report_schema, table_name)
            count = report_db._session.query(table).count()
            counts[table_name] = count

        processor.process()
        for table_name in self.report_tables:
            table = getattr(report_schema, table_name)
            count = report_db._session.query(table).count()
            if table_name not in ('reporting_ocpusagelineitem_daily', 'reporting_ocpusagelineitem_daily_summary'):
                self.assertTrue(count >= counts[table_name])

    def test_process_duplicates(self):
        """Test that row duplicates are not inserted into the DB."""
        counts = {}
        processor = OCPReportProcessor(
            schema_name='acct10001',
            report_path=self.test_report,
            compression=UNCOMPRESSED,
            provider_id=1
        )

        # Process for the first time
        processor.process()

        report_db = self.accessor
        report_schema = report_db.report_schema

        for table_name in self.report_tables:
            table = getattr(report_schema, table_name)
            count = report_db._session.query(table).count()
            counts[table_name] = count

        processor = OCPReportProcessor(
            schema_name='acct10001',
            report_path=self.test_report,
            compression=UNCOMPRESSED,
            provider_id=1
        )
        # Process for the second time
        processor.process()
        for table_name in self.report_tables:
            table = getattr(report_schema, table_name)
            count = report_db._session.query(table).count()
            self.assertTrue(count == counts[table_name])

    def test_process_duplicate_rows_same_file(self):
        """Test that row duplicates are not inserted into the DB."""
        data = []
        with open(self.test_report, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)

        expected_count = len(data)
        data.extend(data)
        tmp_file = '/tmp/test_process_duplicate_rows_same_file.csv'
        field_names = data[0].keys()

        with open(tmp_file, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(data)

        processor = OCPReportProcessor(
            schema_name='acct10001',
            report_path=tmp_file,
            compression=UNCOMPRESSED,
            provider_id=1
        )

        # Process for the first time
        processor.process()

        report_db = self.accessor
        report_schema = report_db.report_schema
        table_name = OCP_REPORT_TABLE_MAP['line_item']
        table = getattr(report_schema, table_name)
        count = report_db._session.query(table).count()
        self.assertEqual(count, expected_count)

    def test_get_file_opener_default(self):
        """Test that the default file opener is returned."""
        opener, mode = self.ocp_processor._processor._get_file_opener(UNCOMPRESSED)

        self.assertEqual(opener, open)
        self.assertEqual(mode, 'r')

    def test_get_file_opener_gzip(self):
        """Test that the gzip file opener is returned."""
        opener, mode = self.ocp_processor._processor._get_file_opener(GZIP_COMPRESSED)

        self.assertEqual(opener, gzip.open)
        self.assertEqual(mode, 'rt')

    def test_update_mappings(self):
        """Test that mappings are updated."""
        test_entry = {'key': 'value'}
        counts = {}
        ce_maps = {
            'report_periods': self.ocp_processor._processor.existing_report_periods_map,
            'reports': self.ocp_processor._processor.existing_report_map,
        }

        for name, ce_map in ce_maps.items():
            counts[name] =  len(ce_map.values())
            ce_map.update(test_entry)

        self.ocp_processor._processor._update_mappings()

        for name, ce_map in ce_maps.items():
            self.assertTrue(len(ce_map.values()) > counts[name])
            for key in test_entry:
                self.assertIn(key, ce_map)

    def test_write_processed_rows_to_csv(self):
        """Test that the CSV bulk upload file contains proper data."""
        cluster_id = '12345'
        report_period_id = self.ocp_processor._processor._create_report_period(self.row, cluster_id, self.accessor)
        report_id = self.ocp_processor._processor._create_report(self.row, report_period_id, self.accessor)
        self.ocp_processor._processor._create_usage_report_line_item(
            self.row,
            report_period_id,
            report_id,
            self.accessor
        )

        file_obj = self.ocp_processor._processor._write_processed_rows_to_csv()
        line_item_data = self.ocp_processor._processor.processed_report.line_items.pop()
        # Convert data to CSV format
        expected_values = [str(value) if value else None
                           for value in line_item_data.values()]

        reader = csv.reader(file_obj, delimiter='\t')
        new_row = next(reader)

        actual = {}
        for i, key in enumerate(line_item_data.keys()):
            actual[key] = new_row[i] if new_row[i] else None

        self.assertEqual(actual.keys(), line_item_data.keys())
        self.assertEqual(list(actual.values()), expected_values)

    def test_create_report_period(self):
        """Test that a report period id is returned."""
        table_name = OCP_REPORT_TABLE_MAP['report_period']
        table = getattr(self.report_schema, table_name)
        id_column = getattr(table, 'id')
        cluster_id = '12345'
        report_period_id = self.ocp_processor._processor._create_report_period(self.row, cluster_id, self.accessor)

        self.assertIsNotNone(report_period_id)

        query = self.accessor._get_db_obj_query(table_name)
        id_in_db = query.order_by(id_column.desc()).first().id

        self.assertEqual(report_period_id, id_in_db)

    def test_create_report(self):
        """Test that a report id is returned."""
        table_name = OCP_REPORT_TABLE_MAP['report']
        table = getattr(self.report_schema, table_name)
        id_column = getattr(table, 'id')
        cluster_id = '12345'
        report_period_id = self.ocp_processor._processor._create_report_period(self.row, cluster_id, self.accessor)

        report_id = self.ocp_processor._processor._create_report(self.row, report_period_id, self.accessor)
        self.accessor.commit()

        self.assertIsNotNone(report_id)

        query = self.accessor._get_db_obj_query(table_name)
        id_in_db = query.order_by(id_column.desc()).first().id

        self.assertEqual(report_id, id_in_db)

    def test_create_usage_report_line_item(self):
        """Test that line item data is returned properly."""
        cluster_id = '12345'
        report_period_id = self.ocp_processor._processor._create_report_period(self.row, cluster_id, self.accessor)
        report_id = self.ocp_processor._processor._create_report(self.row, report_period_id, self.accessor)
        row = copy.deepcopy(self.row)
        row['pod_labels'] = 'label_one:mic_check|label_two:one_two'
        self.ocp_processor._processor._create_usage_report_line_item(
            row,
            report_period_id,
            report_id,
            self.accessor
        )

        line_item = None
        if self.ocp_processor._processor.processed_report.line_items:
            line_item = self.ocp_processor._processor.processed_report.line_items[-1]

        self.assertIsNotNone(line_item)
        self.assertEqual(line_item.get('report_period_id'), report_period_id)
        self.assertEqual(line_item.get('report_id'), report_id)
        self.assertIsNotNone(line_item.get('pod_labels'))

        self.assertIsNotNone(self.ocp_processor._processor.line_item_columns)

    def test_remove_temp_cur_files(self):
        """Test to remove temporary usage report files."""
        # Update once temporary file logic is implemented.
        cur_dir = tempfile.mkdtemp()
        manifest_id = 1
        expected_delete_list = []

        removed_files = self.ocp_processor.remove_temp_cur_files(cur_dir, manifest_id)
        self.assertEqual(sorted(removed_files), sorted(expected_delete_list))

        shutil.rmtree(cur_dir)

    def test_process_pod_labels(self):
        """Test that our report label string format is parsed."""
        test_label_str = 'label_one:first|label_two:next|label_three:final'

        expected = json.dumps({
            'one': 'first',
            'two': 'next',
            'three': 'final'
        })

        result = self.ocp_processor._processor._process_pod_labels(test_label_str)

        self.assertEqual(result, expected)

    def test_process_pod_labels_bad_label_str(self):
        """Test that a bad string is handled."""
        test_label_str = 'label_onefirst|label_twonext|label_threefinal'

        expected = json.dumps({})

        result = self.ocp_processor._processor._process_pod_labels(test_label_str)

        self.assertEqual(result, expected)

    def test_process_storage_default(self):
        """Test the processing of an uncompressed storagefile."""
        processor = OCPReportProcessor(
            schema_name='acct10001',
            report_path=self.storage_report,
            compression=UNCOMPRESSED,
            provider_id=1
        )

        processor.process()
        self.assertIsNotNone(processor._processor._report_path)
