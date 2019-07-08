#
# Copyright 2018 Red Hat, Inc.
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

import datetime
from datetime import timedelta
from calendar import monthrange
from masu.database import OCP_REPORT_TABLE_MAP
from masu.database.report_db_accessor_base import ReportDBAccessorBase
from masu.database.reporting_common_db_accessor import ReportingCommonDBAccessor
from tests import MasuTestCase


class OCPDailySummaryTest(MasuTestCase):
    """Test Cases for the OCP Daily and Daily_Summary database tables."""

    # Select schema and open connection with PostgreSQL and SQLAlchemy
    # Establish connection using PostgreSQL server metadata (PostgreSQL, user, password, host, port, database name)
    # Initialize cursor and set search path to schema
    def setUp(self):
        """Establish the database connection."""
        self._datetime_format = '%Y-%m-%d %H:%M:%S'
        self._schema = 'acct10001'
        self.common_accessor = ReportingCommonDBAccessor()
        self.column_map = self.common_accessor.column_map
        self.accessor = ReportDBAccessorBase(
            self._schema, self.column_map
        )
        self.report_schema = self.accessor.report_schema
        print("Connection is successful!")

    # Close connection with PostgreSQL and SQLAlchemy
    def tearDown(self):
        """Close the DB session connection."""
        self.common_accessor.close_session()
        self.accessor.close_connections()
        self.accessor.close_session()
        print("Connection is closed")

    # Helper raw SQL function to select column data from table with optional query values of row and order by
    def table_select(self, table_name, columns=None, rows=None, order_by=None):
        command = ""
        if columns is not None:
            command = "SELECT {} FROM {};".format(str(columns), str(table_name))
        if rows is not None:
            command = command[:-1] + " WHERE {};".format(str(rows))
        if order_by is not None:
            command = command[:-1] + " ORDER BY {};".format(str(order_by))
        self.accessor._cursor.execute(command)
        data = self.accessor._cursor.fetchall()
        return data

    def get_time_interval(self):
        asc_data = self.table_select(OCP_REPORT_TABLE_MAP['storage_line_item_daily'], "usage_start", None,
                                     "usage_start ASC")
        desc_data = self.table_select(OCP_REPORT_TABLE_MAP['storage_line_item_daily'], "usage_start", None,
                                      "usage_start DESC")
        start_interval = asc_data[0][0].date()
        end_interval = desc_data[0][0].date()
        return start_interval, end_interval

    def date_range(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    # Datetime format util function
    def get_datetime(self, date_val):
        start = "\'" + str(date_val) + " 00:00:00+00\'"
        end = "\'" + str(date_val) + " 23:59:59+00\'"
        return start, end

    def get_ocp_line_item(self, table_name, columns):
        query = self.accessor._get_db_obj_query(
            table_name, columns)
        return query

    # OCP resource daily usage/cost data via DB accessor query
    def get_ocp_daily_db_accessor(self, table_name, columns, date_val):
        usage_start, usage_end = self.get_datetime(date_val)
        query = self.accessor._get_db_obj_query(
            table_name, columns)
        query_by_date = query.filter_by(usage_start=usage_start)
        return query_by_date

    # OCP resource daily summary usage/cost data via DB accessor query
    def get_ocp_daily_summary_db_accessor(self, table_name, columns, date_val):
        usage_start, usage_end = self.get_datetime(date_val)
        query = self.accessor._get_db_obj_query(
            table_name, columns)
        query_by_date = query.filter_by(usage_start=usage_start)
        return query_by_date

    # Assert daily and daily_summary values are correct based on DB accessor queries using SQLAlchemy
    def test_comparison_db_accessor(self):
        # database test between raw and daily reporting tables
        count = self.table_select(OCP_REPORT_TABLE_MAP['line_item'], "count(*)")[0][0]
        line_items = self.get_ocp_line_item(
            OCP_REPORT_TABLE_MAP['line_item'],
            ["cost_entry_bill_id", "cost_entry_product_id", "cost_entry_pricing_id", "cost_entry_reservation_id",
             "resource_id", "line_item_type", "usage_account_id", "usage_type", "availability_zone",
             "tax_type", "product_code", "usage_amount", "unblended_rate", "unblended_cost", "blended_rate",
             "blended_cost",
             "public_on_demand_cost", "public_on_demand_rate", "usage_start"])
        list_dict = [{"cost_entry_bill_id": line_items[0][0], "cost_entry_product_id": line_items[0][1],
                      "cost_entry_pricing_id": line_items[0][2],
                      "cost_entry_reservation_id": line_items[0][3],
                      "resource_id": line_items[0][4], "line_item_type": line_items[0][5],
                      "usage_account_id": line_items[0][6],
                      "usage_type": line_items[0][7], "availability_zone": line_items[0][8],
                      "tax_type": line_items[0][9], "product_code": line_items[0][10],
                      "usage_amount": line_items[0][11],
                      "unblended_rate": line_items[0][12], "unblended_cost": line_items[0][13],
                      "blended_rate": line_items[0][14],
                      "blended_cost": line_items[0][15], "public_on_demand_cost": line_items[0][16],
                      "public_on_demand_rate": line_items[0][17],
                      "usage_start": line_items[0][18]}]
        daily_counter = 0
        curr_date = line_items[0][18].date()
        print(curr_date)
        items_counter = 1
        while items_counter < count:
            if curr_date != line_items[items_counter][18].date():
                daily_values = self.get_ocp_daily_db_accessor(
                    OCP_REPORT_TABLE_MAP['line_item_daily'],
                    ["id", "product_code", "usage_amount", "unblended_rate", "unblended_cost", "blended_rate",
                     "blended_cost", "public_on_demand_cost", "public_on_demand_rate"], curr_date)
                while daily_counter < len(list_dict):
                    self.assertEqual(list_dict[daily_counter]["usage_amount"], daily_values[daily_counter][2])
                    self.assertEqual(list_dict[daily_counter]["unblended_rate"], daily_values[daily_counter][3])
                    self.assertEqual(list_dict[daily_counter]["unblended_cost"], daily_values[daily_counter][4])
                    self.assertEqual(list_dict[daily_counter]["blended_rate"], daily_values[daily_counter][5])
                    self.assertEqual(list_dict[daily_counter]["blended_cost"], daily_values[daily_counter][6])
                    self.assertEqual(list_dict[daily_counter]["public_on_demand_cost"], daily_values[daily_counter][7])
                    self.assertEqual(list_dict[daily_counter]["public_on_demand_rate"], daily_values[daily_counter][8])
                    daily_counter += 1
                print("Raw vs Daily tests have passed!")
                curr_date = line_items[items_counter][18].date()
                print(curr_date)
                list_dict = [{"cost_entry_bill_id": line_items[items_counter][0],
                              "cost_entry_product_id": line_items[items_counter][1],
                              "cost_entry_pricing_id": line_items[items_counter][2],
                              "cost_entry_reservation_id": line_items[items_counter][3],
                              "resource_id": line_items[items_counter][4],
                              "line_item_type": line_items[items_counter][5],
                              "usage_account_id": line_items[items_counter][6],
                              "usage_type": line_items[items_counter][7],
                              "availability_zone": line_items[items_counter][8],
                              "tax_type": line_items[items_counter][9], "product_code": line_items[items_counter][10],
                              "usage_amount": line_items[items_counter][11],
                              "unblended_rate": line_items[items_counter][12],
                              "unblended_cost": line_items[items_counter][13],
                              "blended_rate": line_items[items_counter][14],
                              "blended_cost": line_items[items_counter][15],
                              "public_on_demand_cost": line_items[items_counter][16],
                              "public_on_demand_rate": line_items[items_counter][17],
                              "usage_start": line_items[items_counter][18]}]
            dict_counter = 0
            flag = 0
            while dict_counter < len(list_dict):
                if (list_dict[dict_counter]["cost_entry_bill_id"] == line_items[items_counter][0] and
                        list_dict[dict_counter]["cost_entry_product_id"] == line_items[items_counter][1] and
                        list_dict[dict_counter]["cost_entry_pricing_id"] == line_items[items_counter][2] and
                        list_dict[dict_counter]["cost_entry_reservation_id"] == line_items[items_counter][3] and
                        list_dict[dict_counter]["resource_id"] == line_items[items_counter][4] and
                        list_dict[dict_counter]["line_item_type"] == line_items[items_counter][5] and
                        list_dict[dict_counter]["usage_account_id"] == line_items[items_counter][6] and
                        list_dict[dict_counter]["usage_type"] == line_items[items_counter][7] and
                        list_dict[dict_counter]["availability_zone"] == line_items[items_counter][8] and
                        list_dict[dict_counter]["tax_type"] == line_items[items_counter][9] and
                        list_dict[dict_counter]["product_code"] == line_items[items_counter][10] and
                        list_dict[dict_counter]["usage_start"].date() == line_items[items_counter][18].date()):
                    usage_amount = list_dict[dict_counter]["usage_amount"] + line_items[items_counter][11]
                    unblended_rate = max(list_dict[dict_counter]["unblended_rate"], line_items[items_counter][12])
                    unblended_cost = list_dict[dict_counter]["unblended_cost"] + line_items[items_counter][13]
                    blended_rate = max(list_dict[dict_counter]["blended_rate"], line_items[items_counter][14])
                    blended_cost = list_dict[dict_counter]["blended_cost"] + line_items[items_counter][15]
                    public_on_demand_cost = list_dict[dict_counter]["public_on_demand_cost"] + \
                                            line_items[items_counter][16]
                    public_on_demand_rate = max(list_dict[dict_counter]["public_on_demand_rate"],
                                                line_items[items_counter][17])
                    dic_entry_temp = {"usage_amount": usage_amount, "unblended_rate": unblended_rate,
                                      "unblended_cost": unblended_cost, "blended_rate": blended_rate,
                                      "blended_cost": blended_cost,
                                      "public_on_demand_cost": public_on_demand_cost,
                                      "public_on_demand_rate": public_on_demand_rate}
                    list_dict[dict_counter].update(dic_entry_temp)
                    flag = 1
                    break
                dict_counter += 1
            if flag == 0:
                list_dict.append({"cost_entry_bill_id": line_items[items_counter][0],
                                  "cost_entry_product_id": line_items[items_counter][1],
                                  "cost_entry_pricing_id": line_items[items_counter][2],
                                  "cost_entry_reservation_id": line_items[items_counter][3],
                                  "resource_id": line_items[items_counter][4],
                                  "line_item_type": line_items[items_counter][5],
                                  "usage_account_id": line_items[items_counter][6],
                                  "usage_type": line_items[items_counter][7],
                                  "availability_zone": line_items[items_counter][8],
                                  "tax_type": line_items[items_counter][9],
                                  "product_code": line_items[items_counter][10],
                                  "usage_amount": line_items[items_counter][11],
                                  "unblended_rate": line_items[items_counter][12],
                                  "unblended_cost": line_items[items_counter][13],
                                  "blended_rate": line_items[items_counter][14],
                                  "blended_cost": line_items[items_counter][15],
                                  "public_on_demand_cost": line_items[items_counter][16],
                                  "public_on_demand_rate": line_items[items_counter][17],
                                  "usage_start": line_items[items_counter][18]})
            items_counter += 1

        print("All Raw vs Daily tests have passed!")

        # database test between daily and daily_summary reporting tables
        start_interval, end_interval = (self.get_time_interval())
        for date_val in self.date_range(start_interval, end_interval):
            print("Date: " + str(date_val))
            parse_date = datetime.datetime.strptime(str(date_val), "%Y-%m-%d")
            daily_storage = self.get_ocp_daily_db_accessor(
                OCP_REPORT_TABLE_MAP['storage_line_item_daily'],
                ["persistentvolume_labels", "persistentvolumeclaim_labels", "persistentvolumeclaim_capacity_bytes",
                 "persistentvolumeclaim_capacity_byte_seconds", "volume_request_storage_byte_seconds",
                 "persistentvolumeclaim_usage_byte_seconds"], date_val)
            daily_summary_storage = self.get_ocp_daily_summary_db_accessor(
                OCP_REPORT_TABLE_MAP['storage_line_item_daily_summary'],
                ["volume_labels", "persistentvolumeclaim_capacity_gigabyte",
                 "persistentvolumeclaim_capacity_gigabyte_months", "volume_request_storage_gigabyte_months",
                 "persistentvolumeclaim_usage_gigabyte_months"], date_val)

            persistentvolumeclaim_capacity_gigabyte = float(daily_storage[0][2]) * (2 ** (-30))
            persistentvolumeclaim_capacity_gigabyte_months = float(daily_storage[0][3]) / 86400 * \
                                                             monthrange(parse_date.year, parse_date.month)[1] * (
                                                                         2 ** (-30))
            volume_request_storage_gigabyte_months = float(daily_storage[0][4]) / 86400 * \
                                                     monthrange(parse_date.year, parse_date.month)[1] * (2 ** (-30))
            persistentvolumeclaim_usage_gigabyte_months = float(daily_storage[0][5]) / 86400 * \
                                                          monthrange(parse_date.year, parse_date.month)[1] * (
                                                                      2 ** (-30))

            labels = dict(daily_storage[0][0])
            labels.update(daily_storage[0][1])
            self.assertEqual(labels, daily_summary_storage[0][0])
            self.assertEqual(persistentvolumeclaim_capacity_gigabyte, daily_summary_storage[0][1])
            self.assertEqual(persistentvolumeclaim_capacity_gigabyte_months, daily_summary_storage[0][2])
            self.assertEqual(volume_request_storage_gigabyte_months, daily_summary_storage[0][3])
            self.assertEqual(persistentvolumeclaim_usage_gigabyte_months, daily_summary_storage[0][4])

            daily_usage = self.get_ocp_daily_db_accessor(
                OCP_REPORT_TABLE_MAP['line_item_daily'],
                ["pod_usage_cpu_core_seconds", "pod_request_cpu_core_seconds",
                 "pod_limit_cpu_core_seconds", "pod_usage_memory_byte_seconds",
                 "pod_request_memory_byte_seconds", "pod_limit_memory_byte_seconds",
                 "node_capacity_cpu_core_seconds", "node_capacity_memory_bytes",
                 "node_capacity_memory_byte_seconds", "cluster_capacity_cpu_core_seconds",
                 "cluster_capacity_memory_byte_seconds", "total_capacity_cpu_core_seconds",
                 "total_capacity_memory_byte_seconds"], date_val)
            daily_summary_usage = self.get_ocp_daily_summary_db_accessor(
                OCP_REPORT_TABLE_MAP['line_item_daily_summary'],
                ["pod_usage_cpu_core_hours", "pod_request_cpu_core_hours",
                 "pod_limit_cpu_core_hours", "pod_usage_memory_gigabyte_hours",
                 "pod_request_memory_gigabyte_hours", "pod_limit_memory_gigabyte_hours",
                 "node_capacity_cpu_core_hours", "node_capacity_memory_gigabytes",
                 "node_capacity_memory_gigabyte_hours", "cluster_capacity_cpu_core_hours",
                 "cluster_capacity_memory_gigabyte_hours", "total_capacity_cpu_core_hours",
                 "total_capacity_memory_gigabyte_hours"], date_val)

            self.assertEqual(float(daily_usage[0][0]) / 3600, daily_summary_usage[0][0])
            self.assertEqual(float(daily_usage[0][1]) / 3600, daily_summary_usage[0][1])
            self.assertEqual(float(daily_usage[0][2]) / 3600, daily_summary_usage[0][2])
            self.assertEqual(float(daily_usage[0][3]) / 3600 * (2 ** (-30)), daily_summary_usage[0][3])
            self.assertEqual(float(daily_usage[0][4]) / 3600 * (2 ** (-30)), daily_summary_usage[0][4])
            self.assertEqual(float(daily_usage[0][5]) / 3600 * (2 ** (-30)), daily_summary_usage[0][5])
            self.assertEqual(float(daily_usage[0][6]) / 3600, daily_summary_usage[0][6])
            self.assertEqual(float(daily_usage[0][7]) * (2 ** (-30)), daily_summary_usage[0][7])
            self.assertEqual(float(daily_usage[0][8]) / 3600 * (2 ** (-30)), daily_summary_usage[0][8])
            self.assertEqual(float(daily_usage[0][9]) / 3600, daily_summary_usage[0][9])
            self.assertEqual(float(daily_usage[0][10]) / 3600 * (2 ** (-30)), daily_summary_usage[0][10])
            self.assertEqual(float(daily_usage[0][11]) / 3600, daily_summary_usage[0][11])
            self.assertEqual(float(daily_usage[0][12]) / 3600 * (2 ** (-30)), daily_summary_usage[0][12])

            print("DB Accessor tests have passed!")


# test script
psql = OCPDailySummaryTest()
psql.setUp()
psql.test_comparison_db_accessor()
psql.tearDown()
