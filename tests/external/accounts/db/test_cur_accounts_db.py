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

"""Test the CURAccountsDB utility object."""

from masu.external import AMAZON_WEB_SERVICES
from masu.external.accounts.db.cur_accounts_db import CURAccountsDB
from tests import MasuTestCase


class CURAccountsDBTest(MasuTestCase):
    """Test Cases for the CURAccountsDB object."""

    def setUp(self):
        pass

    def test_get_accounts_from_source(self):
        """Test to get all accounts"""
        accounts = CURAccountsDB().get_accounts_from_source()
        if len(accounts) != 1:
            self.fail('unexpected number of accounts')

        account = accounts.pop()

        self.assertEqual(account.get('authentication'), 'arn:aws:iam::111111111111:role/CostManagement')
        self.assertEqual(account.get('billing_source'), 'test-bucket')
        self.assertEqual(account.get('customer_name'), 'Test Customer')
        self.assertEqual(account.get('provider_type'), AMAZON_WEB_SERVICES)
        self.assertEqual(account.get('provider_uuid'), '6e212746-484a-40cd-bba0-09a19d132d64')
