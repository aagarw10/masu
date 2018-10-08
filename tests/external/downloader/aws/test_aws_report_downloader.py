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

"""Test the AWS S3 utility functions."""

import json
import os.path
import random
import shutil
import tempfile
import uuid
from datetime import datetime
from unittest.mock import ANY, Mock, patch, PropertyMock

import boto3
from botocore.exceptions import ClientError
from dateutil.relativedelta import relativedelta
from faker import Faker
from moto import mock_s3

from masu.config import Config
from masu.database.report_manifest_db_accessor import ReportManifestDBAccessor
from masu.database.report_stats_db_accessor import ReportStatsDBAccessor
from masu.exceptions import MasuProviderError
from masu.external.downloader.aws.aws_report_downloader import (AWSReportDownloader,
                                                                AWSReportDownloaderError,
                                                                AWSReportDownloaderNoFileError)
from masu.external.report_downloader import ReportDownloader
from masu.util.aws import common as utils
from masu.external import AWS_REGIONS
from tests import MasuTestCase
from tests.external.downloader.aws import fake_arn

DATA_DIR = Config.TMP_DIR
FAKE = Faker()
CUSTOMER_NAME = FAKE.word()
REPORT = FAKE.word()
BUCKET = FAKE.word()
PREFIX = FAKE.word()

# the cn endpoints aren't supported by moto, so filter them out
AWS_REGIONS = list(filter(lambda reg: not reg.startswith('cn-'), AWS_REGIONS))
REGION = random.choice(AWS_REGIONS)

class FakeSession():
    """
    Fake Boto Session object.

    This is here because Moto doesn't mock out the 'cur' endpoint yet. As soon
    as Moto supports 'cur', this can be removed.
    """

    @staticmethod
    def client(service):
        fake_report = {'ReportDefinitions': [{
            'ReportName': REPORT,
            'TimeUnit': random.choice(['HOURLY','DAILY']),
            'Format': random.choice(['text', 'csv']),
            'Compression': random.choice(['ZIP','GZIP']),
            'S3Bucket': BUCKET,
            'S3Prefix': PREFIX,
            'S3Region': REGION,
        }]}

        # only mock the 'cur' boto client.
        if 'cur' in service:
            return Mock(**{'describe_report_definitions.return_value': fake_report})

        # pass-through requests for the 's3' boto client.
        with mock_s3():
            return boto3.client(service)

class FakeSessionNoReport():
    """
    Fake Boto Session object with no reports in the S3 bucket.

    This is here because Moto doesn't mock out the 'cur' endpoint yet. As soon
    as Moto supports 'cur', this can be removed.
    """

    @staticmethod
    def client(service):
        fake_report = {'ReportDefinitions': []}

        # only mock the 'cur' boto client.
        if 'cur' in service:
            return Mock(**{'describe_report_definitions.return_value': fake_report})

        # pass-through requests for the 's3' boto client.
        with mock_s3():
            return boto3.client(service)

class AWSReportDownloaderTest(MasuTestCase):
    """Test Cases for the AWS S3 functions."""

    fake = Faker()

    @classmethod
    def setUpClass(cls):
        cls.fake_customer_name = CUSTOMER_NAME
        cls.fake_report_name = REPORT
        cls.fake_bucket_prefix = PREFIX
        cls.fake_bucket_name = BUCKET
        cls.selected_region = REGION
        cls.auth_credential = fake_arn(service='iam', generate_account_id=True)

        cls.manifest_accessor = ReportManifestDBAccessor()

    @classmethod
    def tearDownClass(cls):
        cls.manifest_accessor.close_session()

    @patch('masu.util.aws.common.get_assume_role_session',
           return_value=FakeSession)
    def setUp(self, fake_session):
        os.makedirs(DATA_DIR, exist_ok=True)

        self.report_downloader = ReportDownloader(self.fake_customer_name,
                                                  self.auth_credential,
                                                  self.fake_bucket_name,
                                                  'AWS',
                                                  1)

        self.aws_report_downloader = AWSReportDownloader(**{'customer_name': self.fake_customer_name,
                                                        'auth_credential': self.auth_credential,
                                                        'bucket': self.fake_bucket_name,
                                                        'report_name': self.fake_report_name,
                                                        'provider_id': 1})

    def tearDown(self):
        shutil.rmtree(DATA_DIR, ignore_errors=True)

        manifests = self.manifest_accessor._get_db_obj_query().all()
        for manifest in manifests:
            self.manifest_accessor.delete(manifest)
        self.manifest_accessor.commit()

    @mock_s3
    def test_download_bucket(self):
        fake_report_date = datetime.today().replace(day=1)
        fake_report_end_date = fake_report_date + relativedelta(months=+1)
        report_range = '{}-{}'.format(fake_report_date.strftime('%Y%m%d'),
                                      fake_report_end_date.strftime('%Y%m%d'))

        # Moto setup
        conn = boto3.resource('s3', region_name=self.selected_region)
        conn.create_bucket(Bucket=self.fake_bucket_name)

        # push mocked csvs into Moto env
        fake_csv_files = []
        fake_csv_files_with_key = {}
        for x in range(0, random.randint(2, 10)):
            csv_filename = '{}.csv'.format('-'.join(self.fake.words(random.randint(2, 5))))
            fake_csv_files.append(csv_filename)

            # mocked report file definition
            fake_report_file = '{}/{}/{}/{}/{}'.format(
                self.fake_bucket_prefix,
                self.fake_report_name,
                report_range,
                uuid.uuid4(),
                csv_filename)
            fake_csv_files_with_key[csv_filename] = fake_report_file
            fake_csv_body = ','.join(self.fake.words(random.randint(5, 10)))
            conn.Object(self.fake_bucket_name,
                        fake_report_file).put(Body=fake_csv_body)
            key = conn.Object(self.fake_bucket_name, fake_report_file).get()
            self.assertEqual(fake_csv_body, str(key['Body'].read(), 'utf-8'))

        # mocked Manifest definition
        fake_object = '{}/{}/{}/{}-Manifest.json'.format(
            self.fake_bucket_prefix,
            self.fake_report_name,
            report_range,
            self.fake_report_name)
        fake_object_body = {'reportKeys': fake_csv_files}

        # push mocked manifest into Moto env
        conn.Object(self.fake_bucket_name,
                    fake_object).put(Body=json.dumps(fake_object_body))
        key = conn.Object(self.fake_bucket_name, fake_object).get()
        self.assertEqual(fake_object_body, json.load(key['Body']))

        # actual test
        out = self.aws_report_downloader.download_bucket()
        expected_files = []
        for csv_filename in fake_csv_files:
            report_key = fake_csv_files_with_key.get(csv_filename)
            expected_assembly_id = utils.get_assembly_id_from_cur_key(report_key)
            expected_csv = '{}/{}/aws/{}/{}-{}'.format(DATA_DIR,
                                                 self.fake_customer_name,
                                                 self.fake_bucket_name,
                                                 expected_assembly_id,
                                                 csv_filename)
            expected_files.append(expected_csv)
        expected_manifest = '{}/{}/aws/{}/{}-Manifest.json'.format(DATA_DIR,
                                                                self.fake_customer_name,
                                                                self.fake_bucket_name,
                                                                self.fake_report_name)
        expected_files.append(expected_manifest)
        self.assertEqual(sorted(out), sorted(expected_files))


    @mock_s3
    def test_download_reports(self):
        today = datetime.today().replace(day=1)
        last_month = today + relativedelta(months=-1)
        bill_months = [last_month, today]

        expected_csv_list = []
        for bill_month in bill_months:
            fake_report_date = bill_month
            fake_report_end_date = fake_report_date + relativedelta(months=+1)
            report_range = '{}-{}'.format(fake_report_date.strftime('%Y%m%d'),
                                        fake_report_end_date.strftime('%Y%m%d'))

            # Moto setup
            conn = boto3.resource('s3', region_name=self.selected_region)
            conn.create_bucket(Bucket=self.fake_bucket_name)

            # push mocked csvs into Moto env
            fake_csv_files = {}
            for x in range(0, random.randint(2, 10)):
                csv_filename = '{}.csv'.format('-'.join(self.fake.words(random.randint(2, 5))))

                # mocked report file definition
                fake_report_file = '{}/{}/{}/{}/{}'.format(
                    self.fake_bucket_prefix,
                    self.fake_report_name,
                    report_range,
                    uuid.uuid4(),
                    csv_filename)
                fake_csv_files[csv_filename] = fake_report_file

                fake_csv_body = ','.join(self.fake.words(random.randint(5, 10)))
                conn.Object(self.fake_bucket_name,
                            fake_report_file).put(Body=fake_csv_body)
                key = conn.Object(self.fake_bucket_name, fake_report_file).get()
                self.assertEqual(fake_csv_body, str(key['Body'].read(), 'utf-8'))

            # mocked Manifest definition
            selected_csv = random.choice(list(fake_csv_files.keys()))
            fake_object = '{}/{}/{}/{}-Manifest.json'.format(
                self.fake_bucket_prefix,
                self.fake_report_name,
                report_range,
                self.fake_report_name)
            fake_object_body = {
                'assemblyId': '1234',
                'reportKeys': [fake_csv_files[selected_csv]],
                'billingPeriod': {'start': '20180901T000000.000Z'}
            }

            # push mocked manifest into Moto env
            conn.Object(self.fake_bucket_name,
                        fake_object).put(Body=json.dumps(fake_object_body))
            key = conn.Object(self.fake_bucket_name, fake_object).get()
            self.assertEqual(fake_object_body, json.load(key['Body']))
            report_key = fake_object_body.get('reportKeys').pop()
            expected_assembly_id = utils.get_assembly_id_from_cur_key(report_key)
            expected_csv = '{}/{}/aws/{}/{}-{}'.format(DATA_DIR,
                                                       self.fake_customer_name,
                                                       self.fake_bucket_name,
                                                       expected_assembly_id,
                                                       selected_csv)
            expected_csv_list.append(expected_csv)


        # actual test
        out = self.report_downloader.get_reports(len(bill_months))
        files_list = []
        for cur_dict in out:
            files_list.append(cur_dict['file'])
            self.assertIsNotNone(cur_dict['compression'])

        self.assertEqual(files_list, expected_csv_list)

        # Verify etag is stored
        for cur_dict in out:
            cur_file = cur_dict['file']
            file_name = cur_file.split('/')[-1]
            stats_recorder = ReportStatsDBAccessor(file_name, 1)
            self.assertIsNotNone(stats_recorder.get_etag())

            # Cleanup
            stats_recorder.remove()
            stats_recorder.commit()

            stats_recorder2 = ReportStatsDBAccessor(file_name, 1)
            self.assertIsNone(stats_recorder2.get_etag())
            stats_recorder.close_session()
            stats_recorder2.close_session()


    @mock_s3
    def test_download_file(self):
        fake_object = self.fake.word().lower()
        conn = boto3.resource('s3', region_name=self.selected_region)
        conn.create_bucket(Bucket=self.fake_bucket_name)
        conn.Object(self.fake_bucket_name, fake_object).put(Body='test')

        out, _ = self.aws_report_downloader.download_file(fake_object)
        self.assertEqual(out, DATA_DIR+'/'+self.fake_customer_name+'/aws/'+self.fake_bucket_name+'/'+fake_object)

    @mock_s3
    def test_download_file_missing_key(self):
        fake_object = self.fake.word().lower()
        conn = boto3.resource('s3', region_name=self.selected_region)
        conn.create_bucket(Bucket=self.fake_bucket_name)
        conn.Object(self.fake_bucket_name, fake_object).put(Body='test')

        missing_key = 'missing' + fake_object
        with self.assertRaises(AWSReportDownloaderNoFileError) as error:
            self.aws_report_downloader.download_file(missing_key)
        expected_err = 'Unable to find {} in S3 Bucket: {}'.format(missing_key, self.fake_bucket_name)
        self.assertEqual(expected_err, str(error.exception))

    @mock_s3
    def test_download_file_other_error(self):
        fake_object = self.fake.word().lower()
        # No S3 bucket created
        with self.assertRaises(AWSReportDownloaderError) as error:
            self.aws_report_downloader.download_file(fake_object)
        self.assertTrue('NoSuchBucket' in str(error.exception))

    @mock_s3
    def test_download_report(self):
        fake_report_date = self.fake.date_time().replace(day=1)
        fake_report_end_date = fake_report_date + relativedelta(months=+1)
        report_range = '{}-{}'.format(fake_report_date.strftime('%Y%m%d'),
                                      fake_report_end_date.strftime('%Y%m%d'))

        # mocked report file definition
        fake_report_file = '{}/{}/{}/{}/{}.csv'.format(
            self.fake_bucket_prefix,
            self.fake_report_name,
            report_range,
            uuid.uuid4(),
            'mocked-report-file')

        fake_report_file2 = '{}/{}/{}/{}/{}.csv'.format(
            self.fake_bucket_prefix,
            self.fake_report_name,
            report_range,
            uuid.uuid4(),
            'mocked-report-file2')

        # mocked Manifest definition
        fake_object = '{}/{}/{}/{}-Manifest.json'.format(
            self.fake_bucket_prefix,
            self.fake_report_name,
            report_range,
            self.fake_report_name)
        fake_object_body = {
            'assemblyId': '1234',
            'reportKeys':[fake_report_file, fake_report_file2],
            'billingPeriod': {'start': '20180901T000000.000Z'}
        }

        # Moto setup
        conn = boto3.resource('s3', region_name=self.selected_region)
        conn.create_bucket(Bucket=self.fake_bucket_name)

        # push mocked manifest into Moto env
        conn.Object(self.fake_bucket_name,
                    fake_object).put(Body=json.dumps(fake_object_body))
        key = conn.Object(self.fake_bucket_name, fake_object).get()
        self.assertEqual(fake_object_body, json.load(key['Body']))

        # push mocked csv into Moto env
        fake_csv_body = ','.join(self.fake.words(random.randint(5, 10)))
        conn.Object(self.fake_bucket_name,
                    fake_report_file).put(Body=fake_csv_body)
        conn.Object(self.fake_bucket_name,
                    fake_report_file2).put(Body=fake_csv_body)
        key = conn.Object(self.fake_bucket_name, fake_report_file).get()
        self.assertEqual(fake_csv_body, str(key['Body'].read(), 'utf-8'))

        # actual test. Run twice
        for _ in range(2):
            out = self.aws_report_downloader.download_report(fake_report_date)
            files_list = []
            for cur_dict in out:
                files_list.append(cur_dict['file'])
                self.assertIsNotNone(cur_dict['compression'])

            expected_paths = []
            for report_key in fake_object_body.get('reportKeys'):
                expected_assembly_id = utils.get_assembly_id_from_cur_key(report_key)

                expected_path_base = '{}/{}/{}/{}/{}-{}'
                file_name = os.path.basename(report_key)
                expected_path = expected_path_base.format(DATA_DIR,
                                                        self.fake_customer_name,
                                                        'aws',
                                                        self.fake_bucket_name,
                                                        expected_assembly_id,
                                                        file_name)
                expected_paths.append(expected_path)
            self.assertEqual(files_list, expected_paths)

    @mock_s3
    def test_download_report_missing_manifest(self):
        fake_report_date = self.fake.date_time().replace(day=1)

        # Moto setup
        conn = boto3.resource('s3', region_name=self.selected_region)
        conn.create_bucket(Bucket=self.fake_bucket_name)

        out = self.aws_report_downloader.download_report(fake_report_date)
        self.assertEqual(out, [])

    @mock_s3
    def test_download_report_missing_bucket(self):
        fake_report_date = self.fake.date_time().replace(day=1)

        with self.assertRaises(AWSReportDownloaderError) as error:
            self.aws_report_downloader.download_report(fake_report_date)

    @mock_s3
    @patch('masu.util.aws.common.get_assume_role_session',
           return_value=FakeSession)
    def test_missing_report_name(self, fake_session):
        """Test downloading a report with an invalid report name."""
        auth_credential = fake_arn(service='iam', generate_account_id=True)

        with self.assertRaises(MasuProviderError):
            AWSReportDownloader(self.fake_customer_name,
                                auth_credential,
                                's3_bucket',
                                'wrongreport')


    @mock_s3
    @patch('masu.util.aws.common.get_assume_role_session',
           return_value=FakeSession)
    def test_download_default_report(self, fake_session):
        fake_report_date = self.fake.date_time().replace(day=1)
        fake_report_end_date = fake_report_date + relativedelta(months=+1)
        report_range = '{}-{}'.format(fake_report_date.strftime('%Y%m%d'),
                                      fake_report_end_date.strftime('%Y%m%d'))

        # mocked report file definition
        fake_report_file = '{}/{}/{}/{}/{}.csv'.format(
            self.fake_bucket_prefix,
            self.fake_report_name,
            report_range,
            uuid.uuid4(),
            'mocked-report-file')

        # mocked Manifest definition
        fake_object = '{}/{}/{}/{}-Manifest.json'.format(
            self.fake_bucket_prefix,
            self.fake_report_name,
            report_range,
            self.fake_report_name)
        fake_object_body = {'assemblyId': '1234', 'reportKeys':[fake_report_file]}

        # Moto setup
        conn = boto3.resource('s3', region_name=self.selected_region)
        conn.create_bucket(Bucket=self.fake_bucket_name)

        # push mocked manifest into Moto env
        conn.Object(self.fake_bucket_name,
                    fake_object).put(Body=json.dumps(fake_object_body))
        key = conn.Object(self.fake_bucket_name, fake_object).get()
        self.assertEqual(fake_object_body, json.load(key['Body']))

        # push mocked csv into Moto env
        fake_csv_body = ','.join(self.fake.words(random.randint(5, 10)))
        conn.Object(self.fake_bucket_name,
                    fake_report_file).put(Body=fake_csv_body)
        key = conn.Object(self.fake_bucket_name, fake_report_file).get()
        self.assertEqual(fake_csv_body, str(key['Body'].read(), 'utf-8'))

        # actual test
        auth_credential = fake_arn(service='iam', generate_account_id=True)
        downloader = AWSReportDownloader(self.fake_customer_name,
                                         auth_credential,
                                         self.fake_bucket_name)
        self.assertEqual(downloader.report_name, self.fake_report_name)

    @mock_s3
    @patch('masu.util.aws.common.get_assume_role_session',
           return_value=FakeSessionNoReport)
    @patch('masu.util.aws.common.get_cur_report_definitions',
           return_value=[])
    def test_download_default_report_no_report_found(self, fake_session, fake_report_list):
        auth_credential = fake_arn(service='iam', generate_account_id=True)

        with self.assertRaises(MasuProviderError):
            AWSReportDownloader(self.fake_customer_name,
                                auth_credential,
                                self.fake_bucket_name)
