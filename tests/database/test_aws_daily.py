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
import numpy as np
from masu.database import AWS_CUR_TABLE_MAP
from masu.database.report_db_accessor_base import ReportDBAccessorBase
from masu.database.reporting_common_db_accessor import ReportingCommonDBAccessor
from tests import MasuTestCase


class AWSDailyTest(MasuTestCase):
    """Test Cases for the AWS Daily and Daily_Summary database tables."""

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

    def get_today_date(self):
        return datetime.datetime.now().replace(microsecond=0, second=0, minute=0)

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

    def get_time_interval(self, table_name):
        asc_data = self.table_select(table_name, "usage_start", None, "usage_start ASC")
        desc_data = self.table_select(table_name, "usage_start", None, "usage_start DESC")
        start_interval = asc_data[0][0].date()
        end_interval = desc_data[0][0].date()
        return start_interval, end_interval

    def date_range(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    # Datetime format util function
    def get_datetime(self, date_val):
        start = "\'" + str(date_val) + " 00:00:00+00\'"
        return start

    def get_aws_line_item(self, table_name, columns):
        query = self.accessor._get_db_obj_query(
            table_name, columns)
        return query

    # AWS resource daily usage/cost data via raw SQL query (psycopg2)
    def get_aws_daily_raw(self, table_name, date_val):
        usage_start, usage_end = self.get_datetime(date_val)
        daily_data = np.array(self.table_select(table_name, "id, product_code, usage_amount, unblended_rate,"
                                                            "unblended_cost, blended_rate, blended_cost, public_on_demand_cost, public_on_demand_rate",
                                                "usage_start >= " + usage_start + " AND usage_end <= " + usage_end))
        values_list = []
        row = 0
        while row < daily_data.shape[0]:
            values = {
                "id": daily_data[row][0],
                "product_code": daily_data[row][1],
                "usage_amount": daily_data[row][2],
                "unblended_rate": daily_data[row][3],
                "unblended_cost": daily_data[row][4],
                "blended_rate": daily_data[row][5],
                "blended_cost": daily_data[row][6],
                "public_on_demand_cost": daily_data[row][7],
                "public_on_demand_rate": daily_data[row][8]
            }
            values_list.append(values)
            row += 1
        return values_list

    # AWS resource daily summary usage/cost data via raw SQL query (psycopg2)
    def get_aws_daily_summary_raw(self, table_name, date_val):
        usage_start, usage_end = self.get_datetime(date_val)
        daily_data = self.table_select(table_name, "id, product_code, resource_count, usage_amount,"
                                                   "unblended_rate, unblended_cost, blended_rate, blended_cost, public_on_demand_cost,"
                                                   "public_on_demand_rate",
                                       "usage_start >= " + usage_start + " AND usage_end <= " + usage_end)
        values = {}
        for row in daily_data:
            values = {
                "id": row[0],
                "product_code": row[1],
                "resource_count": row[2],
                "usage_amount": row[3],
                "unblended_rate": row[4],
                "unblended_cost": row[5],
                "blended_rate": row[6],
                "blended_cost": row[7],
                "public_on_demand_cost": row[8],
                "public_on_demand_rate": row[9]
            }
        return values

    # AWS resource daily usage/cost data via DB accessor query
    def get_aws_daily_db_accessor(self, table_name, columns, date_val):
        usage_start = self.get_datetime(date_val)
        query = self.accessor._get_db_obj_query(
            table_name, columns)
        query_by_date = query.filter_by(usage_start=usage_start)
        return query_by_date

    # AWS resource daily summary usage/cost data via DB accessor query
    def get_aws_daily_summary_db_accessor(self, table_name, columns, date_val):
        usage_start = self.get_datetime(date_val)
        query = self.accessor._get_db_obj_query(
            table_name, columns)
        query_by_date = query.filter_by(usage_start=usage_start)
        return query_by_date

    # Assert daily and daily_summary values are correct based on raw SQL queries from PostgreSQL
    def test_comparison_raw(self):
        start_interval, end_interval = (self.get_time_interval())
        for date_val in self.date_range(start_interval, end_interval):
            print("Date: " + str(date_val))
            daily_values = self.get_aws_daily_raw(AWS_CUR_TABLE_MAP['line_item_daily'], date_val)
            daily_summary_values = self.get_aws_daily_summary_raw(AWS_CUR_TABLE_MAP['line_item_daily_summary'],
                                                                  date_val)
            num_resources = daily_summary_values["resource_count"]
            usage_cost_sum, unblended_cost_sum, blended_cost_sum, public_on_demand_cost_sum = 0, 0, 0, 0
            unblended_rate_max, blended_rate_max, public_on_demand_rate_max = 0, 0, 0
            counter = 0
            while counter < num_resources:
                usage_cost_sum += daily_values[counter]["usage_amount"]
                unblended_cost_sum += daily_values[counter]["unblended_cost"]
                blended_cost_sum += daily_values[counter]["blended_cost"]
                public_on_demand_cost_sum += daily_values[counter]["public_on_demand_cost"]
                if daily_values[counter]["unblended_rate"] > unblended_rate_max:
                    unblended_rate_max = daily_values[counter]["unblended_rate"]
                if daily_values[counter]["blended_rate"] > blended_rate_max:
                    blended_rate_max = daily_values[counter]["blended_rate"]
                if daily_values[counter]["public_on_demand_rate"] > public_on_demand_rate_max:
                    public_on_demand_rate_max = daily_values[counter]["public_on_demand_rate"]
                counter += 1

            self.assertEqual(usage_cost_sum, daily_summary_values["usage_amount"])
            self.assertEqual(unblended_rate_max, daily_summary_values["unblended_rate"])
            self.assertEqual(unblended_cost_sum, daily_summary_values["unblended_cost"])
            self.assertEqual(blended_rate_max, daily_summary_values["blended_rate"])
            self.assertEqual(blended_cost_sum, daily_summary_values["blended_cost"])
            self.assertEqual(public_on_demand_cost_sum, daily_summary_values["public_on_demand_cost"])
            self.assertEqual(public_on_demand_rate_max, daily_summary_values["public_on_demand_rate"])
            print("Raw SQL tests have passed!")

    # Main test method
    # Assert raw, daily, and daily_summary values are correct based on DB accessor queries using SQLAlchemy
    def test_comparison_db_accessor(self):
        # database test between raw and daily reporting tables
        count = self.table_select(AWS_CUR_TABLE_MAP['line_item'], "count(*)")[0][0]
        line_items = self.get_aws_line_item(
            AWS_CUR_TABLE_MAP['line_item'],
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
                daily_values = self.get_aws_daily_db_accessor(
                    AWS_CUR_TABLE_MAP['line_item_daily'],
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
        start_interval, end_interval = (self.get_time_interval(AWS_CUR_TABLE_MAP['line_item_daily']))
        today = self.get_today_date().date()
        if end_interval == today:
            end_interval = today
        for date_val in self.date_range(start_interval, end_interval):
            print("Date: " + str(date_val))
            daily_values = self.get_aws_daily_db_accessor(
                AWS_CUR_TABLE_MAP['line_item_daily'],
                ["id", "product_code", "usage_amount", "unblended_rate", "unblended_cost", "blended_rate",
                 "blended_cost", "public_on_demand_cost", "public_on_demand_rate"], date_val)
            daily_summary_values = self.get_aws_daily_summary_db_accessor(
                AWS_CUR_TABLE_MAP['line_item_daily_summary'],
                ["id", "product_code", "resource_count", "usage_amount", "unblended_rate", "unblended_cost",
                 "blended_rate", "blended_cost", "public_on_demand_cost", "public_on_demand_rate"], date_val)
            num_resources = daily_summary_values[0][2]
            daily_count = daily_values.count()
            self.assertEqual(daily_count, num_resources)

            usage_cost_sum, unblended_cost_sum, blended_cost_sum, public_on_demand_cost_sum = 0, 0, 0, 0
            unblended_rate_max, blended_rate_max, public_on_demand_rate_max = 0, 0, 0
            counter = 0
            while counter < daily_count:
                usage_cost_sum += daily_values[counter][2]
                unblended_cost_sum += daily_values[counter][4]
                blended_cost_sum += daily_values[counter][6]
                public_on_demand_cost_sum += daily_values[counter][7]
                if daily_values[counter][3] > unblended_rate_max:
                    unblended_rate_max = daily_values[counter][3]
                if daily_values[counter][5] > blended_rate_max:
                    blended_rate_max = daily_values[counter][5]
                if daily_values[counter][8] > public_on_demand_rate_max:
                    public_on_demand_rate_max = daily_values[counter][8]
                counter += 1

            self.assertEqual(usage_cost_sum, daily_summary_values[0][3])
            self.assertEqual(unblended_rate_max, daily_summary_values[0][4])
            self.assertEqual(unblended_cost_sum, daily_summary_values[0][5])
            self.assertEqual(blended_rate_max, daily_summary_values[0][6])
            self.assertEqual(blended_cost_sum, daily_summary_values[0][7])
            self.assertEqual(public_on_demand_cost_sum, daily_summary_values[0][8])
            self.assertEqual(public_on_demand_rate_max, daily_summary_values[0][9])
            print("Daily vs Daily Summary tests have passed!")

        print("All Daily vs Daily Summary tests have passed!")
        print("All database tests have passed!")


# test script
psql = AWSDailyTest()
psql.setUp()
# psql.test_comparison_raw()
psql.test_comparison_db_accessor()
psql.tearDown()
