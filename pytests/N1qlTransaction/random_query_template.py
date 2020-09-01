import random


class WhereClause(object):
    def get_where_clause(self, data="default", collection="",
                         num_insert=0, num_update=0, num_delete=0):
        if data =="employee":
            INSERT_CLAUSE = [
                "join_mo=10",
                "job_title='Support'",
                "Working_country='USA'",
                "citizen_of='INDIA'",
                "citizen_of='USA' AND job_title='Sales'",
                "salary > 100000",
                "job_title='Engineer'",
                "join_mo=9"
                ]
            WHERE_CLAUSE = [
                "join_mo < 3 OR join_mo > 11: period='winter'",
                "join_day > 25:joined='towards_end'",
                "join_day > 10 AND join_mo > 10:join_month='October'",
                "job_title='Engineer' and 'Testing' in skills:QE=True",
                "job_title='Support':SE=True",
                "join_yr > 2016 OR job_title='Engineer':mutated=1",
                "join_mo =1:join_month='January'",
                "job_title='Sales':VISA=['US', 'CANADA']",
                "['Development'] in skills:gaming=True"
                "citizen_of='INDIA':rating=exceeded_expectation",
                "Working_country='USA':temp_emp=True"
            ]
        else:
            WHERE_CLAUSE = [
                "join_mo < 3 OR join_mo > 11:period='winter'",
                "join_day > 25:joined='towards_end'",
                "join_day > 10 AND join_mo > 10:join_month='October'",
                "job_title='Engineer' and 'Testing' in skills:QE=True",
                "job_title='Support':SE=True",
                "join_yr > 2016 OR job_title='Engineer':mutated=1",
                "join_mo =10:join_month='October'",
                "job_title='Sales':VISA=['US', 'CANADA']",
                "['Development','C++'] in skills:gaming=True",
                "'Development' in skills:isdevloper=True",
                "join_mo=9:join_month='SEPTEMBER'",
                "job_title='Sales':Team='Sales'",
                "job_title='Engineer':Team='Development'"
            ]

        insert_clause = random.sample(INSERT_CLAUSE, num_insert)
        update_clause = random.sample(WHERE_CLAUSE, num_update)
        delete_clause = random.sample(WHERE_CLAUSE, num_delete)
        stmt = self.randomize_query(collection, insert_clause,
                                    update_clause, delete_clause)
        return stmt

    def randomize_query(self, collection,
                        insert_clause=[], update_clause=[], delete_clause=[]):
        stmts = []
        for stmt in insert_clause:
            stmts.append("%s:INSERT: %s" % (collection, stmt))
        for stmt in update_clause:
            stmts.append("%s:UPDATE: %s" % (collection, stmt))
        for stmt in delete_clause:
            stmts.append("%s:DELETE: %s" % (collection, stmt))
        return stmts
