
# Copyright (c) 2012, Erik Smartt
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.

# Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import MySQLdb  # We're going raw...

from django.conf import settings
from django.db import models


class ShardModel(models.Model):
    date_created = models.DateTimeField(auto_now_add=True, blank=True)
    date_updated = models.DateTimeField(auto_now=True, blank=True)
    db_host = models.CharField(max_length=300, blank=True, null=True)
    db_table = models.CharField(max_length=300, blank=True, null=True)
    has_storage = models.BooleanField(default=False)

    ## Instance variables ##
    _columns = []  # You'll want to over-ride this!
    _db = None
    _indexes = []  # You'll probably want to over-ride this!
    _last_sql = None  # Stores the last SQL statement executed, which may be handy for debugging.
    _table = '0'  # A legacy property that you probably don't need. It's used for the default table_name if you don't write your own implementation.

    class Meta:
        abstract = True

    def _get_db_cursor(self):
        """
        Has a side-effect of settings self._db.  This is assumed to be a write-db.
        """
        db_name = settings.DATABASES['default']['NAME']
        db_user = settings.DATABASES['default']['USER']
        db_passwd = settings.DATABASES['default']['PASSWORD']

        if self.db_host:
            db_host = self.db_host
        else:
            db_host = settings.DATABASES['default']['HOST']

        try:
            db_port = int(settings.DATABASES['default']['PORT'])
        except ValueError:
            db_port = None

        if db_port:
            try:
                self._db = MySQLdb.connect(user=db_user, db=db_name, passwd=db_passwd, port=db_port, host=db_host)
            except MySQLdb.OperationalError:
                self._db = None

        else:
            try:
                self._db = MySQLdb.connect(user=db_user, db=db_name, passwd=db_passwd)
            except MySQLdb.OperationalError:
                self._db = None

        try:
            return self._db.cursor()
        except AttributeError:
            return None

    def table_name(self):
        if self.db_table:
            return self.db_table
        else:
            return "shard_{k}".format(k=self._table)

    def create_table_sql(self):
        try:
            indexes = []

            for i in self._indexes:
                indexes.append('CREATE INDEX `{index}_index` on `{table_name}` ({index});'.format(index=i, table_name=self.table_name()))

            sql = """BEGIN;
                CREATE TABLE `{table_name}` (
                    `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    {columns}
                );
                {indexes}
                COMMIT;""".format(table_name=self.table_name(), columns=', '.join(self._columns), indexes=' '.join(indexes))

        except UnicodeError:
            sql = None

        return sql

    def count(self, str=None):
        count = 0

        if self.has_storage:
            if str:
                sql = "SELECT COUNT(*) FROM `{table_name}` WHERE {s};".format(table_name=self.table_name(), s=str)
            else:
                sql = "SELECT COUNT(*) FROM `{table_name}`;".format(table_name=self.table_name())

            cursor = self._get_db_cursor()

            try:
                cursor.execute(sql)
                result = cursor.fetchone()
                cursor.close()

            except MySQLdb.ProgrammingError:
                count = -1

            except AttributeError:
                # cursor might be None (as in, we couldn't connect to the DB)
                count = -1

            else:
                self._last_sql = sql

                count = result[0]

        return count

    def create_storage(self):
        if self.has_storage is False:
            sql = self.create_table_sql()

            if sql:
                cursor = self._get_db_cursor()

                try:
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    #self._db.commit()
                    cursor.close()

                except MySQLdb.OperationalError:
                    # It's possible that the table already exists
                    pass

                except AttributeError:
                    # cursor might be None (as in, we couldn't connect to the DB)
                    pass

                else:
                    self._last_sql = sql

                    self.has_storage = True
                    self.save()

    def get_row_where(self, key=None, value=None, str=None):
        if self.has_storage:
            if str:
                sql = 'SELECT * FROM `{table_name}` WHERE {s};'.format(table_name=self.table_name(), s=str)
            else:
                sql = 'SELECT * FROM `{table_name}` WHERE `{key}` = "{value}";'.format(table_name=self.table_name(), key=key, value=value)

            cursor = self._get_db_cursor()

            try:
                cursor.execute(sql)
                result = cursor.fetchone()
                cursor.close()

            except MySQLdb.OperationalError:
                pass

            except AttributeError:
                pass

            else:
                self._last_sql = sql

                return result

        return False

    def get_sql(self, sql, limit=0, offset=0):
        if self.has_storage:
            cursor = self._get_db_cursor()

            try:
                cursor.execute(sql)

            except MySQLdb.OperationalError:
                pass

            except AttributeError:
                pass

            else:
                if limit > 0:
                    results = cursor.fetchmany(limit)

                else:
                    results = cursor.fetchall()

                cursor.close()

                self._last_sql = sql

                return [i for i in results]

        return []

    def insert(self, sql):
        # If we don't have a storage table, nows the time to create one.
        if self.has_storage is False:
            self.create_storage()

        cursor = self._get_db_cursor()

        try:
            cursor.execute(sql)
            results = cursor.fetchone()
            self._db.commit()
            cursor.close()

        except MySQLdb.ProgrammingError:
            pass

        except MySQLdb.OperationalError:
            pass

        except AttributeError:
            pass

        else:
            self._last_sql = sql

    def remove_storage(self):
        if self.has_storage is True:
            sql = "DROP TABLE `{table_name}`;".format(table_name=self.table_name())

            cursor = self._get_db_cursor()

            try:
                cursor.execute(sql)
                result = cursor.fetchone()
                self._db.commit()
                cursor.close()

            except MySQLdb.OperationalError:
                pass

            except AttributeError:
                pass

            else:
                self._last_sql = sql

                self.has_storage = False
                self.save()

    def remove_row_where(self, key=None, value=None, str=None):
        if self.has_storage:
            if str:
                sql = 'DELETE FROM `{table_name}` WHERE {s};'.format(table_name=self.table_name(), s=str)
            else:
                sql = 'DELETE FROM `{table_name}` WHERE `{key}` = "{value}";'.format(table_name=self.table_name(), key=key, value=value)

            cursor = self._get_db_cursor()

            try:
                cursor.execute(sql)

            except MySQLdb.OperationalError:
                pass

            except AttributeError:
                pass

            else:
                try:
                    results = cursor.fetchone()
                    self._db.commit()

                except Exception:
                    pass

                cursor.close()

                self._last_sql = sql
